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
package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.UTF8TaxonomyWriterCache;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;


public class TestDirectoryTaxonomyWriter extends FacetTestCase {

  // A No-Op TaxonomyWriterCache which always discards all given categories, and
  // always returns true in put(), to indicate some cache entries were cleared.
  private static TaxonomyWriterCache NO_OP_CACHE = new TaxonomyWriterCache() {
    
    @Override
    public void close() {}
    @Override
    public int get(FacetLabel categoryPath) { return -1; }
    @Override
    public boolean put(FacetLabel categoryPath, int ordinal) { return true; }
    @Override
    public boolean isFull() { return true; }
    @Override
    public void clear() {}
    @Override
    public int size() { return 0; }
    
  };
  
  @Test
  public void testCommit() throws Exception {
    // Verifies that nothing is committed to the underlying Directory, if
    // commit() wasn't called.
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    assertFalse(DirectoryReader.indexExists(dir));
    ltw.commit(); // first commit, so that an index will be created
    ltw.addCategory(new FacetLabel("a"));
    
    IndexReader r = DirectoryReader.open(dir);
    assertEquals("No categories should have been committed to the underlying directory", 1, r.numDocs());
    r.close();
    ltw.close();
    dir.close();
  }
  
  @Test
  public void testCommitUserData() throws Exception {
    // Verifies taxonomy commit data
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    assertTrue(taxoWriter.getCache() == NO_OP_CACHE);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.addCategory(new FacetLabel("b"));
    Map<String, String> userCommitData = new HashMap<>();
    userCommitData.put("testing", "1 2 3");
    taxoWriter.setLiveCommitData(userCommitData.entrySet());
    taxoWriter.close();
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals("2 categories plus root should have been committed to the underlying directory", 3, r.numDocs());
    Map <String, String> readUserCommitData = r.getIndexCommit().getUserData();
    assertTrue("wrong value extracted from commit data", 
        "1 2 3".equals(readUserCommitData.get("testing")));
    assertNotNull(DirectoryTaxonomyWriter.INDEX_EPOCH + " not found in commitData", readUserCommitData.get(DirectoryTaxonomyWriter.INDEX_EPOCH));
    r.close();
    
    // open DirTaxoWriter again and commit, INDEX_EPOCH should still exist
    // in the commit data, otherwise DirTaxoReader.refresh() might not detect
    // that the taxonomy index has been recreated.
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    taxoWriter.addCategory(new FacetLabel("c")); // add a category so that commit will happen
    taxoWriter.setLiveCommitData(new HashMap<String, String>(){{
      put("just", "data");
    }}.entrySet());
    taxoWriter.commit();
    
    // verify taxoWriter.getCommitData()
    Map<String,String> data = new HashMap<>();
    Iterable<Map.Entry<String,String>> iter = taxoWriter.getLiveCommitData();
    if (iter != null) {
      for(Map.Entry<String,String> ent : iter) {
        data.put(ent.getKey(), ent.getValue());
      }
    }
    
    assertNotNull(DirectoryTaxonomyWriter.INDEX_EPOCH
        + " not found in taoxWriter.commitData", data.get(DirectoryTaxonomyWriter.INDEX_EPOCH));
    taxoWriter.close();
    
    r = DirectoryReader.open(dir);
    readUserCommitData = r.getIndexCommit().getUserData();
    assertNotNull(DirectoryTaxonomyWriter.INDEX_EPOCH + " not found in commitData", readUserCommitData.get(DirectoryTaxonomyWriter.INDEX_EPOCH));
    r.close();
    
    dir.close();
  }
  
  @Test
  public void testRollback() throws Exception {
    // Verifies that if rollback is called, DTW is closed.
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter dtw = new DirectoryTaxonomyWriter(dir);
    assertTrue(dtw.getCache() instanceof UTF8TaxonomyWriterCache);
    dtw.addCategory(new FacetLabel("a"));
    dtw.rollback();
    // should not have succeeded to add a category following rollback.
    expectThrows(AlreadyClosedException.class, () -> {
      dtw.addCategory(new FacetLabel("a"));
    });

    dir.close();
  }
  
  @Test
  public void testRecreateRollback() throws Exception {
    // Tests rollback with OpenMode.CREATE
    Directory dir = newDirectory();
    new DirectoryTaxonomyWriter(dir).close();
    assertEquals(1, getEpoch(dir));
    new DirectoryTaxonomyWriter(dir, OpenMode.CREATE).rollback();
    assertEquals(1, getEpoch(dir));
    
    dir.close();
  }
  
  @Test
  public void testEnsureOpen() throws Exception {
    // verifies that an exception is thrown if DTW was closed
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter dtw = new DirectoryTaxonomyWriter(dir);
    dtw.close();
    // should not succeed to add a category following close.
    expectThrows(AlreadyClosedException.class, () -> {
      dtw.addCategory(new FacetLabel("a"));
    });

    dir.close();
  }

  private void touchTaxo(DirectoryTaxonomyWriter taxoWriter, FacetLabel cp) throws IOException {
    taxoWriter.addCategory(cp);
    taxoWriter.setLiveCommitData(new HashMap<String, String>(){{
      put("just", "data");
    }}.entrySet());
    taxoWriter.commit();
  }
  
  @Test
  public void testRecreateAndRefresh() throws Exception {
    // DirTaxoWriter lost the INDEX_EPOCH property if it was opened in
    // CREATE_OR_APPEND (or commit(userData) called twice), which could lead to
    // DirTaxoReader succeeding to refresh().
    try (Directory dir = newDirectory()) {

      DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
      touchTaxo(taxoWriter, new FacetLabel("a"));

      TaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);

      touchTaxo(taxoWriter, new FacetLabel("b"));

      TaxonomyReader newtr = TaxonomyReader.openIfChanged(taxoReader);
      taxoReader.close();
      taxoReader = newtr;
      assertEquals(1, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));

      // now recreate the taxonomy, and check that the epoch is preserved after opening DirTW again.
      taxoWriter.close();

      taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE, NO_OP_CACHE);
      touchTaxo(taxoWriter, new FacetLabel("c"));
      taxoWriter.close();

      taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
      touchTaxo(taxoWriter, new FacetLabel("d"));
      taxoWriter.close();

      newtr = TaxonomyReader.openIfChanged(taxoReader);
      taxoReader.close();
      taxoReader = newtr;
      assertEquals(2, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));
      taxoReader.close();
    }
  }

  @Test
  public void testBackwardsCompatibility() throws Exception {
    // tests that if the taxonomy index doesn't have the INDEX_EPOCH
    // property (supports pre-3.6 indexes), all still works.
    Directory dir = newDirectory();
    
    // create an empty index first, so that DirTaxoWriter initializes indexEpoch to 1.
    new IndexWriter(dir, new IndexWriterConfig(null)).close();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    taxoWriter.close();
    
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    assertEquals(1, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));
    assertNull(TaxonomyReader.openIfChanged(taxoReader));
    taxoReader.close();
    
    dir.close();
  }

  public void testConcurrency() throws Exception {
    final int ncats = TEST_NIGHTLY ? atLeast(100000)  : atLeast(1000); // at night, add many categories
    final int range = ncats * 3; // affects the categories selection
    final AtomicInteger numCats = new AtomicInteger(ncats);
    final Directory dir = newDirectory();
    final ConcurrentHashMap<String,String> values = new ConcurrentHashMap<>();
    final double d = random().nextDouble();
    final TaxonomyWriterCache cache;
    if (d < 0.7) {
      // this is the fastest, yet most memory consuming
      cache = new UTF8TaxonomyWriterCache();
    } else if (TEST_NIGHTLY && d > 0.98) {
      // this is the slowest, but tests the writer concurrency when no caching is done.
      // only pick it during NIGHTLY tests, and even then, with very low chances.
      cache = NO_OP_CACHE;
    } else {
      // this is slower than UTF8, but less memory consuming, and exercises finding categories on disk too.
      cache = new LruTaxonomyWriterCache(ncats / 10);
    }
    if (VERBOSE) {
      System.out.println("TEST: use cache=" + cache);
    }
    final DirectoryTaxonomyWriter tw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE, cache);
    Thread[] addThreads = new Thread[atLeast(4)];
    for (int z = 0; z < addThreads.length; z++) {
      addThreads[z] = new Thread() {
        @Override
        public void run() {
          Random random = random();
          while (numCats.decrementAndGet() > 0) {
            try {
              int value = random.nextInt(range);
              FacetLabel cp = new FacetLabel(Integer.toString(value / 1000), Integer.toString(value / 10000),
                  Integer.toString(value / 100000), Integer.toString(value));
              int ord = tw.addCategory(cp);
              assertTrue("invalid parent for ordinal " + ord + ", category " + cp, tw.getParent(ord) != -1);
              String l1 = FacetsConfig.pathToString(cp.components, 1);
              String l2 = FacetsConfig.pathToString(cp.components, 2);
              String l3 = FacetsConfig.pathToString(cp.components, 3);
              String l4 = FacetsConfig.pathToString(cp.components, 4);
              values.put(l1, l1);
              values.put(l2, l2);
              values.put(l3, l3);
              values.put(l4, l4);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
      };
    }
    
    for (Thread t : addThreads) t.start();
    for (Thread t : addThreads) t.join();
    tw.close();
    
    DirectoryTaxonomyReader dtr = new DirectoryTaxonomyReader(dir);
    // +1 for root category
    if (values.size() + 1 != dtr.getSize()) {
      for(String value : values.keySet()) {
        FacetLabel label = new FacetLabel(FacetsConfig.stringToPath(value));
        if (dtr.getOrdinal(label) == -1) {
          System.out.println("FAIL: path=" + label + " not recognized");
        }
      }
      fail("mismatch number of categories");
    }

    int[] parents = dtr.getParallelTaxonomyArrays().parents();
    for (String cat : values.keySet()) {
      FacetLabel cp = new FacetLabel(FacetsConfig.stringToPath(cat));
      assertTrue("category not found " + cp, dtr.getOrdinal(cp) > 0);
      int level = cp.length;
      int parentOrd = 0; // for root, parent is always virtual ROOT (ord=0)
      FacetLabel path = new FacetLabel();
      for (int i = 0; i < level; i++) {
        path = cp.subpath(i + 1);
        int ord = dtr.getOrdinal(path);
        assertEquals("invalid parent for cp=" + path, parentOrd, parents[ord]);
        parentOrd = ord; // next level should have this parent
      }
    }

    IOUtils.close(dtr, dir);
  }

  private long getEpoch(Directory taxoDir) throws IOException {
    SegmentInfos infos = SegmentInfos.readLatestCommit(taxoDir);
    return Long.parseLong(infos.getUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH));
  }
  
  @Test
  public void testReplaceTaxonomy() throws Exception {
    Directory input = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(input);
    int ordA = taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.close();
    
    Directory dir = newDirectory();
    taxoWriter = new DirectoryTaxonomyWriter(dir);
    int ordB = taxoWriter.addCategory(new FacetLabel("b"));
    taxoWriter.addCategory(new FacetLabel("c"));
    taxoWriter.commit();
    
    long origEpoch = getEpoch(dir);
    
    // replace the taxonomy with the input one
    taxoWriter.replaceTaxonomy(input);
    
    // LUCENE-4633: make sure that category "a" is not added again in any case
    taxoWriter.addTaxonomy(input, new MemoryOrdinalMap());
    assertEquals("no categories should have been added", 2, taxoWriter.getSize()); // root + 'a'
    assertEquals("category 'a' received new ordinal?", ordA, taxoWriter.addCategory(new FacetLabel("a")));

    // add the same category again -- it should not receive the same ordinal !
    int newOrdB = taxoWriter.addCategory(new FacetLabel("b"));
    assertNotSame("new ordinal cannot be the original ordinal", ordB, newOrdB);
    assertEquals("ordinal should have been 2 since only one category was added by replaceTaxonomy", 2, newOrdB);

    taxoWriter.close();
    
    long newEpoch = getEpoch(dir);
    assertTrue("index epoch should have been updated after replaceTaxonomy", origEpoch < newEpoch);
    
    dir.close();
    input.close();
  }

  @Test
  public void testReaderFreshness() throws Exception {
    // ensures that the internal index reader is always kept fresh. Previously,
    // this simple scenario failed, if the cache just evicted the category that
    // is being added.
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE, NO_OP_CACHE);
    int o1 = taxoWriter.addCategory(new FacetLabel("a"));
    int o2 = taxoWriter.addCategory(new FacetLabel("a"));
    assertTrue("ordinal for same category that is added twice should be the same !", o1 == o2);
    taxoWriter.close();
    dir.close();
  }

  @Test
  public void testCommitNoEmptyCommits() throws Exception {
    // LUCENE-4972: DTW used to create empty commits even if no changes were made
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.commit();
    
    long gen1 = SegmentInfos.getLastCommitGeneration(dir);
    taxoWriter.commit();
    long gen2 = SegmentInfos.getLastCommitGeneration(dir);
    assertEquals("empty commit should not have changed the index", gen1, gen2);
    
    taxoWriter.close();
    dir.close();
  }
  
  @Test
  public void testCloseNoEmptyCommits() throws Exception {
    // LUCENE-4972: DTW used to create empty commits even if no changes were made
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.commit();
    
    long gen1 = SegmentInfos.getLastCommitGeneration(dir);
    taxoWriter.close();
    long gen2 = SegmentInfos.getLastCommitGeneration(dir);
    assertEquals("empty commit should not have changed the index", gen1, gen2);
    
    taxoWriter.close();
    dir.close();
  }
  
  @Test
  public void testPrepareCommitNoEmptyCommits() throws Exception {
    // LUCENE-4972: DTW used to create empty commits even if no changes were made
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir);
    taxoWriter.addCategory(new FacetLabel("a"));
    taxoWriter.prepareCommit();
    taxoWriter.commit();
    
    long gen1 = SegmentInfos.getLastCommitGeneration(dir);
    taxoWriter.prepareCommit();
    taxoWriter.commit();
    long gen2 = SegmentInfos.getLastCommitGeneration(dir);
    assertEquals("empty commit should not have changed the index", gen1, gen2);
    
    taxoWriter.close();
    dir.close();
  }

  // TODO: this test can hit pathological cases: it adds only a few docs, what is going on?
  @Test @Nightly
  public void testHugeLabel() throws Exception {
    Directory indexDir = newDirectory(), taxoDir = newDirectory();
    IndexWriter indexWriter = new IndexWriter(indexDir, newIndexWriterConfig(new MockAnalyzer(random())));
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir, OpenMode.CREATE, new UTF8TaxonomyWriterCache());
    FacetsConfig config = new FacetsConfig();
    
    // Add one huge label:
    String bigs = null;
    int ordinal = -1;

    int len = FacetLabel.MAX_CATEGORY_PATH_LENGTH - 4; // for the dimension and separator
    bigs = TestUtil.randomSimpleString(random(), len, len);
    FacetField ff = new FacetField("dim", bigs);
    FacetLabel cp = new FacetLabel("dim", bigs);
    ordinal = taxoWriter.addCategory(cp);
    Document doc = new Document();
    doc.add(ff);
    indexWriter.addDocument(config.build(taxoWriter, doc));

    // Add tiny ones to cause a re-hash
    for (int i = 0; i < 3; i++) {
      String s = TestUtil.randomSimpleString(random(), 1, 10);
      taxoWriter.addCategory(new FacetLabel("dim", s));
      doc = new Document();
      doc.add(new FacetField("dim", s));
      indexWriter.addDocument(config.build(taxoWriter, doc));
    }

    // when too large components were allowed to be added, this resulted in a new added category
    assertEquals(ordinal, taxoWriter.addCategory(cp));

    indexWriter.close();
    IOUtils.close(taxoWriter);
    
    DirectoryReader indexReader = DirectoryReader.open(indexDir);
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);
    IndexSearcher searcher = new IndexSearcher(indexReader);
    DrillDownQuery ddq = new DrillDownQuery(new FacetsConfig());
    ddq.add("dim", bigs);
    assertEquals(1, searcher.search(ddq, 10).totalHits.value);
    
    IOUtils.close(indexReader, taxoReader, indexDir, taxoDir);
  }
  
  @Test
  public void testReplaceTaxoWithLargeTaxonomy() throws Exception {
    Directory srcTaxoDir = newDirectory(), targetTaxoDir = newDirectory();
    
    // build source, large, taxonomy
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(srcTaxoDir);
    int ord = taxoWriter.addCategory(new FacetLabel("A", "1", "1", "1", "1", "1", "1"));
    taxoWriter.close();
    
    taxoWriter = new DirectoryTaxonomyWriter(targetTaxoDir);
    int ordinal = taxoWriter.addCategory(new FacetLabel("B", "1"));
    assertEquals(1, taxoWriter.getParent(ordinal)); // call getParent to initialize taxoArrays
    taxoWriter.commit();
    
    taxoWriter.replaceTaxonomy(srcTaxoDir);
    assertEquals(ord - 1, taxoWriter.getParent(ord));
    taxoWriter.close();
    
    srcTaxoDir.close();
    targetTaxoDir.close();
  }
  
}
