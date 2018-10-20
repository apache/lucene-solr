/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.replicator;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.replicator.IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter;
import org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
import org.apache.lucene.replicator.ReplicationClient.SourceDirectoryFactory;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexAndTaxonomyReplicationClientTest extends ReplicatorTestCase {
  
  private static class IndexAndTaxonomyReadyCallback implements Callable<Boolean>, Closeable {
    
    private final Directory indexDir, taxoDir;
    private DirectoryReader indexReader;
    private DirectoryTaxonomyReader taxoReader;
    private FacetsConfig config;
    private long lastIndexGeneration = -1;
    
    public IndexAndTaxonomyReadyCallback(Directory indexDir, Directory taxoDir) throws IOException {
      this.indexDir = indexDir;
      this.taxoDir = taxoDir;
      config = new FacetsConfig();
      config.setHierarchical("A", true);
      if (DirectoryReader.indexExists(indexDir)) {
        indexReader = DirectoryReader.open(indexDir);
        lastIndexGeneration = indexReader.getIndexCommit().getGeneration();
        taxoReader = new DirectoryTaxonomyReader(taxoDir);
      }
    }
    
    @Override
    public Boolean call() throws Exception {
      if (indexReader == null) {
        indexReader = DirectoryReader.open(indexDir);
        lastIndexGeneration = indexReader.getIndexCommit().getGeneration();
        taxoReader = new DirectoryTaxonomyReader(taxoDir);
      } else {
        // verify search index
        DirectoryReader newReader = DirectoryReader.openIfChanged(indexReader);
        assertNotNull("should not have reached here if no changes were made to the index", newReader);
        long newGeneration = newReader.getIndexCommit().getGeneration();
        assertTrue("expected newer generation; current=" + lastIndexGeneration + " new=" + newGeneration, newGeneration > lastIndexGeneration);
        indexReader.close();
        indexReader = newReader;
        lastIndexGeneration = newGeneration;
        TestUtil.checkIndex(indexDir);
        
        // verify taxonomy index
        DirectoryTaxonomyReader newTaxoReader = TaxonomyReader.openIfChanged(taxoReader);
        if (newTaxoReader != null) {
          taxoReader.close();
          taxoReader = newTaxoReader;
        }
        TestUtil.checkIndex(taxoDir);
        
        // verify faceted search
        int id = Integer.parseInt(indexReader.getIndexCommit().getUserData().get(VERSION_ID), 16);
        IndexSearcher searcher = new IndexSearcher(indexReader);
        FacetsCollector fc = new FacetsCollector();
        searcher.search(new MatchAllDocsQuery(), fc);
        Facets facets = new FastTaxonomyFacetCounts(taxoReader, config, fc);
        assertEquals(1, facets.getSpecificValue("A", Integer.toString(id, 16)).intValue());
        
        DrillDownQuery drillDown = new DrillDownQuery(config);
        drillDown.add("A", Integer.toString(id, 16));
        TopDocs docs = searcher.search(drillDown, 10);
        assertEquals(1, docs.totalHits.value);
      }
      return null;
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(indexReader, taxoReader);
    }
  }
  
  private Directory publishIndexDir, publishTaxoDir;
  private MockDirectoryWrapper handlerIndexDir, handlerTaxoDir;
  private Replicator replicator;
  private SourceDirectoryFactory sourceDirFactory;
  private ReplicationClient client;
  private ReplicationHandler handler;
  private IndexWriter publishIndexWriter;
  private SnapshotDirectoryTaxonomyWriter publishTaxoWriter;
  private FacetsConfig config;
  private IndexAndTaxonomyReadyCallback callback;
  private Path clientWorkDir;
  
  private static final String VERSION_ID = "version";
  
  private void assertHandlerRevision(int expectedID, Directory dir) throws IOException {
    // loop as long as client is alive. test-framework will terminate us if
    // there's a serious bug, e.g. client doesn't really update. otherwise,
    // introducing timeouts is not good, can easily lead to false positives.
    while (client.isUpdateThreadAlive()) {
      // give client a chance to update
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
      
      try {
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
          int handlerID = Integer.parseInt(reader.getIndexCommit().getUserData().get(VERSION_ID), 16);
          if (expectedID == handlerID) {
            return;
          }
        } finally {
          reader.close();
        }
      } catch (Exception e) {
        // we can hit IndexNotFoundException or e.g. EOFException (on
        // segments_N) because it is being copied at the same time it is read by
        // DirectoryReader.open().
      }
    }
  }
  
  private Revision createRevision(final int id) throws IOException {
    publishIndexWriter.addDocument(newDocument(publishTaxoWriter, id));
    publishIndexWriter.setLiveCommitData(new HashMap<String, String>() {{
      put(VERSION_ID, Integer.toString(id, 16));
    }}.entrySet());
    publishIndexWriter.commit();
    publishTaxoWriter.commit();
    return new IndexAndTaxonomyRevision(publishIndexWriter, publishTaxoWriter);
  }
  
  private Document newDocument(TaxonomyWriter taxoWriter, int id) throws IOException {
    Document doc = new Document();
    doc.add(new FacetField("A", Integer.toString(id, 16)));
    return config.build(taxoWriter, doc);
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    publishIndexDir = newDirectory();
    publishTaxoDir = newDirectory();
    handlerIndexDir = newMockDirectory();
    handlerTaxoDir = newMockDirectory();
    clientWorkDir = createTempDir("replicationClientTest");
    sourceDirFactory = new PerSessionDirectoryFactory(clientWorkDir);
    replicator = new LocalReplicator();
    callback = new IndexAndTaxonomyReadyCallback(handlerIndexDir, handlerTaxoDir);
    handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, callback);
    client = new ReplicationClient(replicator, handler, sourceDirFactory);
    
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    publishIndexWriter = new IndexWriter(publishIndexDir, conf);
    publishTaxoWriter = new SnapshotDirectoryTaxonomyWriter(publishTaxoDir);
    config = new FacetsConfig();
    config.setHierarchical("A", true);
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    publishIndexWriter.close();
    IOUtils.close(client, callback, publishTaxoWriter, replicator, publishIndexDir, publishTaxoDir,
            handlerIndexDir, handlerTaxoDir);
    super.tearDown();
  }
  
  @Test
  public void testNoUpdateThread() throws Exception {
    assertNull("no version expected at start", handler.currentVersion());
    
    // Callback validates the replicated index
    replicator.publish(createRevision(1));
    client.updateNow();
    
    // make sure updating twice, when in fact there's nothing to update, works
    client.updateNow();
    
    replicator.publish(createRevision(2));
    client.updateNow();
    
    // Publish two revisions without update, handler should be upgraded to latest
    replicator.publish(createRevision(3));
    replicator.publish(createRevision(4));
    client.updateNow();
  }
  
  @Test
  public void testRestart() throws Exception {
    replicator.publish(createRevision(1));
    client.updateNow();
    
    replicator.publish(createRevision(2));
    client.updateNow();
    
    client.stopUpdateThread();
    client.close();
    client = new ReplicationClient(replicator, handler, sourceDirFactory);
    
    // Publish two revisions without update, handler should be upgraded to latest
    replicator.publish(createRevision(3));
    replicator.publish(createRevision(4));
    client.updateNow();
  }
  
  @Test
  public void testUpdateThread() throws Exception {
    client.startUpdateThread(10, "indexTaxo");
    
    replicator.publish(createRevision(1));
    assertHandlerRevision(1, handlerIndexDir);
    
    replicator.publish(createRevision(2));
    assertHandlerRevision(2, handlerIndexDir);
    
    // Publish two revisions without update, handler should be upgraded to latest
    replicator.publish(createRevision(3));
    replicator.publish(createRevision(4));
    assertHandlerRevision(4, handlerIndexDir);
  }
  
  @Test
  public void testRecreateTaxonomy() throws Exception {
    replicator.publish(createRevision(1));
    client.updateNow();
    
    // recreate index and taxonomy
    Directory newTaxo = newDirectory();
    new DirectoryTaxonomyWriter(newTaxo).close();
    publishTaxoWriter.replaceTaxonomy(newTaxo);
    publishIndexWriter.deleteAll();
    replicator.publish(createRevision(2));
    
    client.updateNow();
    newTaxo.close();
  }

  /*
   * This test verifies that the client and handler do not end up in a corrupt
   * index if exceptions are thrown at any point during replication. Either when
   * a client copies files from the server to the temporary space, or when the
   * handler copies them to the index directory.
   */
  @Test
  public void testConsistencyOnExceptions() throws Exception {
    // so the handler's index isn't empty
    replicator.publish(createRevision(1));
    client.updateNow();
    client.close();
    callback.close();

    // wrap sourceDirFactory to return a MockDirWrapper so we can simulate errors
    final SourceDirectoryFactory in = sourceDirFactory;
    final AtomicInteger failures = new AtomicInteger(atLeast(10));
    sourceDirFactory = new SourceDirectoryFactory() {
      
      private long clientMaxSize = 100, handlerIndexMaxSize = 100, handlerTaxoMaxSize = 100;
      private double clientExRate = 1.0, handlerIndexExRate = 1.0, handlerTaxoExRate = 1.0;
      
      @Override
      public void cleanupSession(String sessionID) throws IOException {
        in.cleanupSession(sessionID);
      }
      
      @SuppressWarnings("synthetic-access")
      @Override
      public Directory getDirectory(String sessionID, String source) throws IOException {
        Directory dir = in.getDirectory(sessionID, source);
        if (random().nextBoolean() && failures.get() > 0) { // client should fail, return wrapped dir
          MockDirectoryWrapper mdw = new MockDirectoryWrapper(random(), dir);
          mdw.setRandomIOExceptionRateOnOpen(clientExRate);
          mdw.setMaxSizeInBytes(clientMaxSize);
          mdw.setRandomIOExceptionRate(clientExRate);
          mdw.setCheckIndexOnClose(false);
          clientMaxSize *= 2;
          clientExRate /= 2;
          return mdw;
        }
        
        if (failures.get() > 0 && random().nextBoolean()) { // handler should fail
          if (random().nextBoolean()) { // index dir fail
            handlerIndexDir.setMaxSizeInBytes(handlerIndexMaxSize);
            handlerIndexDir.setRandomIOExceptionRate(handlerIndexExRate);
            handlerIndexDir.setRandomIOExceptionRateOnOpen(handlerIndexExRate);
            handlerIndexMaxSize *= 2;
            handlerIndexExRate /= 2;
          } else { // taxo dir fail
            handlerTaxoDir.setMaxSizeInBytes(handlerTaxoMaxSize);
            handlerTaxoDir.setRandomIOExceptionRate(handlerTaxoExRate);
            handlerTaxoDir.setRandomIOExceptionRateOnOpen(handlerTaxoExRate);
            handlerTaxoDir.setCheckIndexOnClose(false);
            handlerTaxoMaxSize *= 2;
            handlerTaxoExRate /= 2;
          }
        } else {
          // disable all errors
          handlerIndexDir.setMaxSizeInBytes(0);
          handlerIndexDir.setRandomIOExceptionRate(0.0);
          handlerIndexDir.setRandomIOExceptionRateOnOpen(0.0);
          handlerTaxoDir.setMaxSizeInBytes(0);
          handlerTaxoDir.setRandomIOExceptionRate(0.0);
          handlerTaxoDir.setRandomIOExceptionRateOnOpen(0.0);
        }

        return dir;
      }
    };
    
    handler = new IndexAndTaxonomyReplicationHandler(handlerIndexDir, handlerTaxoDir, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (random().nextDouble() < 0.2 && failures.get() > 0) {
          throw new RuntimeException("random exception from callback");
        }
        return null;
      }
    });

    final AtomicBoolean failed = new AtomicBoolean();

    // wrap handleUpdateException so we can act on the thrown exception
    client = new ReplicationClient(replicator, handler, sourceDirFactory) {
      @SuppressWarnings("synthetic-access")
      @Override
      protected void handleUpdateException(Throwable t) {
        if (t instanceof IOException) {
          try {
            if (VERBOSE) {
              System.out.println("hit exception during update: " + t);
              t.printStackTrace(System.out);
            }

            // test that the index can be read and also some basic statistics
            DirectoryReader reader = DirectoryReader.open(handlerIndexDir.getDelegate());
            try {
              int numDocs = reader.numDocs();
              int version = Integer.parseInt(reader.getIndexCommit().getUserData().get(VERSION_ID), 16);
              assertEquals(numDocs, version);
            } finally {
              reader.close();
            }
            // verify index is fully consistent
            TestUtil.checkIndex(handlerIndexDir.getDelegate());
            
            // verify taxonomy index is fully consistent (since we only add one
            // category to all documents, there's nothing much more to validate.
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1024);
            CheckIndex.Status indexStatus = null;

            try (CheckIndex checker = new CheckIndex(handlerTaxoDir.getDelegate())) {
              checker.setFailFast(true);
              checker.setInfoStream(new PrintStream(bos, false, IOUtils.UTF_8), false);
              try {
                indexStatus = checker.checkIndex(null);
              } catch (IOException | RuntimeException ioe) {
                // ok: we fallback below
              }
            }

          } catch (IOException e) {
            failed.set(true);
            throw new RuntimeException(e);
          } catch (RuntimeException e) {
            failed.set(true);
            throw e;
          } finally {
            // count-down number of failures
            failures.decrementAndGet();
            assert failures.get() >= 0 : "handler failed too many times: " + failures.get();
            if (VERBOSE) {
              if (failures.get() == 0) {
                System.out.println("no more failures expected");
              } else {
                System.out.println("num failures left: " + failures.get());
              }
            }
          }
        } else {
          failed.set(true);
          if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
          }
          throw new RuntimeException(t);
        }
      }
    };
    
    client.startUpdateThread(10, "indexAndTaxo");
    
    final Directory baseHandlerIndexDir = handlerIndexDir.getDelegate();
    int numRevisions = atLeast(20) + 2;
    for (int i = 2; i < numRevisions && failed.get() == false; i++) {
      replicator.publish(createRevision(i));
      assertHandlerRevision(i, baseHandlerIndexDir);
    }

    // disable errors -- maybe randomness didn't exhaust all allowed failures,
    // and we don't want e.g. CheckIndex to hit false errors. 
    handlerIndexDir.setMaxSizeInBytes(0);
    handlerIndexDir.setRandomIOExceptionRate(0.0);
    handlerIndexDir.setRandomIOExceptionRateOnOpen(0.0);
    handlerTaxoDir.setMaxSizeInBytes(0);
    handlerTaxoDir.setRandomIOExceptionRate(0.0);
    handlerTaxoDir.setRandomIOExceptionRateOnOpen(0.0);
  }
  
}
