package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.facet.FacetTestCase;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter.MemoryOrdinalMap;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.cl2o.Cl2oTaxonomyWriterCache;
import org.apache.lucene.facet.taxonomy.writercache.lru.LruTaxonomyWriterCache;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.junit.Test;

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

public class TestDirectoryTaxonomyWriter extends FacetTestCase {

  // A No-Op TaxonomyWriterCache which always discards all given categories, and
  // always returns true in put(), to indicate some cache entries were cleared.
  private static TaxonomyWriterCache NO_OP_CACHE = new TaxonomyWriterCache() {
    
    @Override
    public void close() {}
    @Override
    public int get(CategoryPath categoryPath) { return -1; }
    @Override
    public boolean put(CategoryPath categoryPath, int ordinal) { return true; }
    @Override
    public boolean isFull() { return true; }
    @Override
    public void clear() {}
    
  };
  
  @Test
  public void testCommit() throws Exception {
    // Verifies that nothing is committed to the underlying Directory, if
    // commit() wasn't called.
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    assertFalse(DirectoryReader.indexExists(dir));
    ltw.commit(); // first commit, so that an index will be created
    ltw.addCategory(new CategoryPath("a"));
    
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
    taxoWriter.addCategory(new CategoryPath("a"));
    taxoWriter.addCategory(new CategoryPath("b"));
    Map<String, String> userCommitData = new HashMap<String, String>();
    userCommitData.put("testing", "1 2 3");
    taxoWriter.setCommitData(userCommitData);
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
    taxoWriter.addCategory(new CategoryPath("c")); // add a category so that commit will happen
    taxoWriter.setCommitData(new HashMap<String, String>(){{
      put("just", "data");
    }});
    taxoWriter.commit();
    
    // verify taxoWriter.getCommitData()
    assertNotNull(DirectoryTaxonomyWriter.INDEX_EPOCH
        + " not found in taoxWriter.commitData", taxoWriter.getCommitData().get(DirectoryTaxonomyWriter.INDEX_EPOCH));
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
    dtw.addCategory(new CategoryPath("a"));
    dtw.rollback();
    try {
      dtw.addCategory(new CategoryPath("a"));
      fail("should not have succeeded to add a category following rollback.");
    } catch (AlreadyClosedException e) {
      // expected
    }
    
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
    try {
      dtw.addCategory(new CategoryPath("a"));
      fail("should not have succeeded to add a category following close.");
    } catch (AlreadyClosedException e) {
      // expected
    }
    dir.close();
  }

  private void touchTaxo(DirectoryTaxonomyWriter taxoWriter, CategoryPath cp) throws IOException {
    taxoWriter.addCategory(cp);
    taxoWriter.setCommitData(new HashMap<String, String>(){{
      put("just", "data");
    }});
    taxoWriter.commit();
  }
  
  @Test
  public void testRecreateAndRefresh() throws Exception {
    // DirTaxoWriter lost the INDEX_EPOCH property if it was opened in
    // CREATE_OR_APPEND (or commit(userData) called twice), which could lead to
    // DirTaxoReader succeeding to refresh().
    Directory dir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("a"));
    
    TaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);

    touchTaxo(taxoWriter, new CategoryPath("b"));
    
    TaxonomyReader newtr = TaxonomyReader.openIfChanged(taxoReader);
    taxoReader.close();
    taxoReader = newtr;
    assertEquals(1, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));

    // now recreate the taxonomy, and check that the epoch is preserved after opening DirTW again.
    taxoWriter.close();
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("c"));
    taxoWriter.close();
    
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("d"));
    taxoWriter.close();

    newtr = TaxonomyReader.openIfChanged(taxoReader);
    taxoReader.close();
    taxoReader = newtr;
    assertEquals(2, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));

    taxoReader.close();
    dir.close();
  }

  @Test
  public void testBackwardsCompatibility() throws Exception {
    // tests that if the taxonomy index doesn't have the INDEX_EPOCH
    // property (supports pre-3.6 indexes), all still works.
    Directory dir = newDirectory();
    
    // create an empty index first, so that DirTaxoWriter initializes indexEpoch to 1.
    new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    taxoWriter.close();
    
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    assertEquals(1, Integer.parseInt(taxoReader.getCommitUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH)));
    assertNull(TaxonomyReader.openIfChanged(taxoReader));
    taxoReader.close();
    
    dir.close();
  }

  public void testConcurrency() throws Exception {
    final int ncats = atLeast(100000); // add many categories
    final int range = ncats * 3; // affects the categories selection
    final AtomicInteger numCats = new AtomicInteger(ncats);
    final Directory dir = newDirectory();
    final ConcurrentHashMap<String,String> values = new ConcurrentHashMap<String,String>();
    final double d = random().nextDouble();
    final TaxonomyWriterCache cache;
    if (d < 0.7) {
      // this is the fastest, yet most memory consuming
      cache = new Cl2oTaxonomyWriterCache(1024, 0.15f, 3);
    } else if (TEST_NIGHTLY && d > 0.98) {
      // this is the slowest, but tests the writer concurrency when no caching is done.
      // only pick it during NIGHTLY tests, and even then, with very low chances.
      cache = NO_OP_CACHE;
    } else {
      // this is slower than CL2O, but less memory consuming, and exercises finding categories on disk too.
      cache = new LruTaxonomyWriterCache(ncats / 10);
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
              CategoryPath cp = new CategoryPath(Integer.toString(value / 1000), Integer.toString(value / 10000),
                  Integer.toString(value / 100000), Integer.toString(value));
              int ord = tw.addCategory(cp);
              assertTrue("invalid parent for ordinal " + ord + ", category " + cp, tw.getParent(ord) != -1);
              String l1 = cp.subpath(1).toString('/');
              String l2 = cp.subpath(2).toString('/');
              String l3 = cp.subpath(3).toString('/');
              String l4 = cp.subpath(4).toString('/');
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
    assertEquals("mismatch number of categories", values.size() + 1, dtr.getSize()); // +1 for root category
    int[] parents = dtr.getParallelTaxonomyArrays().parents();
    for (String cat : values.keySet()) {
      CategoryPath cp = new CategoryPath(cat, '/');
      assertTrue("category not found " + cp, dtr.getOrdinal(cp) > 0);
      int level = cp.length;
      int parentOrd = 0; // for root, parent is always virtual ROOT (ord=0)
      CategoryPath path = CategoryPath.EMPTY;
      for (int i = 0; i < level; i++) {
        path = cp.subpath(i + 1);
        int ord = dtr.getOrdinal(path);
        assertEquals("invalid parent for cp=" + path, parentOrd, parents[ord]);
        parentOrd = ord; // next level should have this parent
      }
    }
    dtr.close();
    
    dir.close();
  }

  private long getEpoch(Directory taxoDir) throws IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(taxoDir);
    return Long.parseLong(infos.getUserData().get(DirectoryTaxonomyWriter.INDEX_EPOCH));
  }
  
  @Test
  public void testReplaceTaxonomy() throws Exception {
    Directory input = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(input);
    int ordA = taxoWriter.addCategory(new CategoryPath("a"));
    taxoWriter.close();
    
    Directory dir = newDirectory();
    taxoWriter = new DirectoryTaxonomyWriter(dir);
    int ordB = taxoWriter.addCategory(new CategoryPath("b"));
    taxoWriter.addCategory(new CategoryPath("c"));
    taxoWriter.commit();
    
    long origEpoch = getEpoch(dir);
    
    // replace the taxonomy with the input one
    taxoWriter.replaceTaxonomy(input);
    
    // LUCENE-4633: make sure that category "a" is not added again in any case
    taxoWriter.addTaxonomy(input, new MemoryOrdinalMap());
    assertEquals("no categories should have been added", 2, taxoWriter.getSize()); // root + 'a'
    assertEquals("category 'a' received new ordinal?", ordA, taxoWriter.addCategory(new CategoryPath("a")));

    // add the same category again -- it should not receive the same ordinal !
    int newOrdB = taxoWriter.addCategory(new CategoryPath("b"));
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
    int o1 = taxoWriter.addCategory(new CategoryPath("a"));
    int o2 = taxoWriter.addCategory(new CategoryPath("a"));
    assertTrue("ordinal for same category that is added twice should be the same !", o1 == o2);
    taxoWriter.close();
    dir.close();
  }
  
}
