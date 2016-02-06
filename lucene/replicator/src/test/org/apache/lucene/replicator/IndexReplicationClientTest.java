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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.replicator.ReplicationClient.ReplicationHandler;
import org.apache.lucene.replicator.ReplicationClient.SourceDirectoryFactory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.ThreadInterruptedException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexReplicationClientTest extends ReplicatorTestCase {
  
  private static class IndexReadyCallback implements Callable<Boolean>, Closeable {
    
    private final Directory indexDir;
    private DirectoryReader reader; 
    private long lastGeneration = -1;
    
    public IndexReadyCallback(Directory indexDir) throws IOException {
      this.indexDir = indexDir;
      if (DirectoryReader.indexExists(indexDir)) {
        reader = DirectoryReader.open(indexDir);
        lastGeneration = reader.getIndexCommit().getGeneration();
      }
    }
    
    @Override
    public Boolean call() throws Exception {
      if (reader == null) {
        reader = DirectoryReader.open(indexDir);
        lastGeneration = reader.getIndexCommit().getGeneration();
      } else {
        DirectoryReader newReader = DirectoryReader.openIfChanged(reader);
        assertNotNull("should not have reached here if no changes were made to the index", newReader);
        long newGeneration = newReader.getIndexCommit().getGeneration();
        assertTrue("expected newer generation; current=" + lastGeneration + " new=" + newGeneration, newGeneration > lastGeneration);
        reader.close();
        reader = newReader;
        lastGeneration = newGeneration;
        TestUtil.checkIndex(indexDir);
      }
      return null;
    }
    
    @Override
    public void close() throws IOException {
      IOUtils.close(reader);
    }
  }
  
  private MockDirectoryWrapper publishDir, handlerDir;
  private Replicator replicator;
  private SourceDirectoryFactory sourceDirFactory;
  private ReplicationClient client;
  private ReplicationHandler handler;
  private IndexWriter publishWriter;
  private IndexReadyCallback callback;
  
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
          } else if (VERBOSE) {
            System.out.println("expectedID=" + expectedID + " actual=" + handlerID + " generation=" + reader.getIndexCommit().getGeneration());
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
    publishWriter.addDocument(new Document());
    publishWriter.setCommitData(new HashMap<String, String>() {{
      put(VERSION_ID, Integer.toString(id, 16));
    }});
    publishWriter.commit();
    return new IndexRevision(publishWriter);
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    publishDir = newMockDirectory();
    handlerDir = newMockDirectory();
    sourceDirFactory = new PerSessionDirectoryFactory(createTempDir("replicationClientTest"));
    replicator = new LocalReplicator();
    callback = new IndexReadyCallback(handlerDir);
    handler = new IndexReplicationHandler(handlerDir, callback);
    client = new ReplicationClient(replicator, handler, sourceDirFactory);
    
    IndexWriterConfig conf = newIndexWriterConfig(null);
    conf.setIndexDeletionPolicy(new SnapshotDeletionPolicy(conf.getIndexDeletionPolicy()));
    publishWriter = new IndexWriter(publishDir, conf);
  }
  
  @After
  @Override
  public void tearDown() throws Exception {
    publishWriter.close();
    IOUtils.close(client, callback, replicator, publishDir, handlerDir);
    super.tearDown();
  }
  
  @Test
  public void testNoUpdateThread() throws Exception {
    assertNull("no version expected at start", handler.currentVersion());
    
    // Callback validates the replicated index
    replicator.publish(createRevision(1));
    client.updateNow();
    
    replicator.publish(createRevision(2));
    client.updateNow();
    
    // Publish two revisions without update, handler should be upgraded to latest
    replicator.publish(createRevision(3));
    replicator.publish(createRevision(4));
    client.updateNow();
  }
  
  @Test
  public void testUpdateThread() throws Exception {
    client.startUpdateThread(10, "index");
    
    replicator.publish(createRevision(1));
    assertHandlerRevision(1, handlerDir);
    
    replicator.publish(createRevision(2));
    assertHandlerRevision(2, handlerDir);
    
    // Publish two revisions without update, handler should be upgraded to latest
    replicator.publish(createRevision(3));
    replicator.publish(createRevision(4));
    assertHandlerRevision(4, handlerDir);
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
    
    // Replicator violates write-once policy. It may be that the
    // handler copies files to the index dir, then fails to copy a
    // file and reverts the copy operation. On the next attempt, it
    // will copy the same file again. There is nothing wrong with this
    // in a real system, but it does violate write-once, and MDW
    // doesn't like it. Disabling it means that we won't catch cases
    // where the handler overwrites an existing index file, but
    // there's nothing currently we can do about it, unless we don't
    // use MDW.
    handlerDir.setPreventDoubleWrite(false);

    // wrap sourceDirFactory to return a MockDirWrapper so we can simulate errors
    final SourceDirectoryFactory in = sourceDirFactory;
    final AtomicInteger failures = new AtomicInteger(atLeast(10));
    sourceDirFactory = new SourceDirectoryFactory() {
      
      private long clientMaxSize = 100, handlerMaxSize = 100;
      private double clientExRate = 1.0, handlerExRate = 1.0;
      
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
          handlerDir.setMaxSizeInBytes(handlerMaxSize);
          handlerDir.setRandomIOExceptionRateOnOpen(handlerExRate);
          handlerDir.setRandomIOExceptionRate(handlerExRate);
          handlerMaxSize *= 2;
          handlerExRate /= 2;
        } else {
          // disable errors
          handlerDir.setMaxSizeInBytes(0);
          handlerDir.setRandomIOExceptionRate(0.0);
          handlerDir.setRandomIOExceptionRateOnOpen(0.0);
        }
        return dir;
      }
    };
    
    handler = new IndexReplicationHandler(handlerDir, new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        if (random().nextDouble() < 0.2 && failures.get() > 0) {
          throw new RuntimeException("random exception from callback");
        }
        return null;
      }
    });
    
    // wrap handleUpdateException so we can act on the thrown exception
    client = new ReplicationClient(replicator, handler, sourceDirFactory) {
      @SuppressWarnings("synthetic-access")
      @Override
      protected void handleUpdateException(Throwable t) {
        if (t instanceof IOException) {
          if (VERBOSE) {
            System.out.println("hit exception during update: " + t);
            t.printStackTrace(System.out);
          }
          try {
            // test that the index can be read and also some basic statistics
            DirectoryReader reader = DirectoryReader.open(handlerDir.getDelegate());
            try {
              int numDocs = reader.numDocs();
              int version = Integer.parseInt(reader.getIndexCommit().getUserData().get(VERSION_ID), 16);
              assertEquals(numDocs, version);
            } finally {
              reader.close();
            }
            // verify index consistency
            TestUtil.checkIndex(handlerDir.getDelegate());
          } catch (IOException e) {
            // exceptions here are bad, don't ignore them
            throw new RuntimeException(e);
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
          if (t instanceof RuntimeException) throw (RuntimeException) t;
          throw new RuntimeException(t);
        }
      }
    };
    
    client.startUpdateThread(10, "index");

    final Directory baseHandlerDir = handlerDir.getDelegate();
    int numRevisions = atLeast(20);
    for (int i = 2; i < numRevisions; i++) {
      replicator.publish(createRevision(i));
      assertHandlerRevision(i, baseHandlerDir);
    }
    
    // disable errors -- maybe randomness didn't exhaust all allowed failures,
    // and we don't want e.g. CheckIndex to hit false errors. 
    handlerDir.setMaxSizeInBytes(0);
    handlerDir.setRandomIOExceptionRate(0.0);
    handlerDir.setRandomIOExceptionRateOnOpen(0.0);
  }
  
}
