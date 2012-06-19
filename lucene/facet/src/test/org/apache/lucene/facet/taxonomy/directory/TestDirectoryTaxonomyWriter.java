package org.apache.lucene.facet.taxonomy.directory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.InconsistentTaxonomyException;
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
import org.apache.lucene.util.LuceneTestCase;
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

public class TestDirectoryTaxonomyWriter extends LuceneTestCase {

  // A No-Op TaxonomyWriterCache which always discards all given categories, and
  // always returns true in put(), to indicate some cache entries were cleared.
  private static TaxonomyWriterCache NO_OP_CACHE = new TaxonomyWriterCache() {
    
    @Override
    public void close() {}
    @Override
    public int get(CategoryPath categoryPath) { return -1; }
    @Override
    public int get(CategoryPath categoryPath, int length) { return -1; }
    @Override
    public boolean put(CategoryPath categoryPath, int ordinal) { return true; }
    @Override
    public boolean put(CategoryPath categoryPath, int prefixLen, int ordinal) { return true; }
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
    Map <String, String> userCommitData = new HashMap<String, String>();
    userCommitData.put("testing", "1 2 3");
    taxoWriter.commit(userCommitData);
    taxoWriter.close();
    DirectoryReader r = DirectoryReader.open(dir);
    assertEquals("2 categories plus root should have been committed to the underlying directory", 3, r.numDocs());
    Map <String, String> readUserCommitData = r.getIndexCommit().getUserData();
    assertTrue("wrong value extracted from commit data", 
        "1 2 3".equals(readUserCommitData.get("testing")));
    assertNotNull("index.create.time not found in commitData", readUserCommitData.get(DirectoryTaxonomyWriter.INDEX_CREATE_TIME));
    r.close();
    
    // open DirTaxoWriter again and commit, INDEX_CREATE_TIME should still exist
    // in the commit data, otherwise DirTaxoReader.refresh() might not detect
    // that the taxonomy index has been recreated.
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    taxoWriter.addCategory(new CategoryPath("c")); // add a category so that commit will happen
    taxoWriter.commit(new HashMap<String, String>(){{
      put("just", "data");
    }});
    taxoWriter.close();
    
    r = DirectoryReader.open(dir);
    readUserCommitData = r.getIndexCommit().getUserData();
    assertNotNull("index.create.time not found in commitData", readUserCommitData.get(DirectoryTaxonomyWriter.INDEX_CREATE_TIME));
    r.close();
    
    dir.close();
  }
  
  @Test
  public void testRollback() throws Exception {
    // Verifies that if callback is called, DTW is closed.
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
    taxoWriter.commit(new HashMap<String, String>(){{
      put("just", "data");
    }});
  }
  
  @Test
  public void testRecreateAndRefresh() throws Exception {
    // DirTaxoWriter lost the INDEX_CREATE_TIME property if it was opened in
    // CREATE_OR_APPEND (or commit(userData) called twice), which could lead to
    // DirTaxoReader succeeding to refresh().
    Directory dir = newDirectory();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("a"));
    
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);

    touchTaxo(taxoWriter, new CategoryPath("b"));
    
    // this should not fail
    taxoReader.refresh();

    // now recreate the taxonomy, and check that the timestamp is preserved after opening DirTW again.
    taxoWriter.close();
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("c"));
    taxoWriter.close();
    
    taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    touchTaxo(taxoWriter, new CategoryPath("d"));
    taxoWriter.close();

    // this should fail
    try {
      taxoReader.refresh();
      fail("IconsistentTaxonomyException should have been thrown");
    } catch (InconsistentTaxonomyException e) {
      // ok, expected
    }
    
    taxoReader.close();
    dir.close();
  }

  @Test
  public void testUndefinedCreateTime() throws Exception {
    // tests that if the taxonomy index doesn't have the INDEX_CREATE_TIME
    // property (supports pre-3.6 indexes), all still works.
    Directory dir = newDirectory();
    
    // create an empty index first, so that DirTaxoWriter initializes createTime to null.
    new IndexWriter(dir, new IndexWriterConfig(TEST_VERSION_CURRENT, null)).close();
    
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, NO_OP_CACHE);
    // we cannot commit null keys/values, this ensures that if DirTW.createTime is null, we can still commit.
    taxoWriter.close();
    
    DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(dir);
    taxoReader.refresh();
    taxoReader.close();
    
    dir.close();
  }

  public void testConcurrency() throws Exception {
    final int ncats = atLeast(100000); // add many categories
    final int range = ncats * 3; // affects the categories selection
    final AtomicInteger numCats = new AtomicInteger(ncats);
    final Directory dir = newDirectory();
    final ConcurrentHashMap<Integer,Integer> values = new ConcurrentHashMap<Integer,Integer>();
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
              tw.addCategory(new CategoryPath("a", Integer.toString(value)));
              values.put(value, value);
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
    assertEquals("mismatch number of categories", values.size() + 2, dtr.getSize()); // +2 for root category + "a"
    for (Integer value : values.keySet()) {
      assertTrue("category not found a/" + value, dtr.getOrdinal(new CategoryPath("a", value.toString())) > 0);
    }
    dtr.close();
    
    dir.close();
  }

  private String getCreateTime(Directory taxoDir) throws IOException {
    SegmentInfos infos = new SegmentInfos();
    infos.read(taxoDir);
    return infos.getUserData().get(DirectoryTaxonomyWriter.INDEX_CREATE_TIME);
  }
  
  @Test
  public void testReplaceTaxonomy() throws Exception {
    Directory input = newDirectory();
    DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(input);
    taxoWriter.addCategory(new CategoryPath("a"));
    taxoWriter.close();
    
    Directory dir = newDirectory();
    taxoWriter = new DirectoryTaxonomyWriter(dir);
    int ordinal = taxoWriter.addCategory(new CategoryPath("b"));
    taxoWriter.addCategory(new CategoryPath("c"));
    taxoWriter.commit();
    
    String origCreateTime = getCreateTime(dir);
    
    // replace the taxonomy with the input one
    taxoWriter.replaceTaxonomy(input);
    
    // add the same category again -- it should not receive the same ordinal !
    int newOrdinal = taxoWriter.addCategory(new CategoryPath("b"));
    assertNotSame("new ordinal cannot be the original ordinal", ordinal, newOrdinal);
    assertEquals("ordinal should have been 2 since only one category was added by replaceTaxonomy", 2, newOrdinal);
    
    taxoWriter.close();
    
    String newCreateTime = getCreateTime(dir);
    assertNotSame("create time should have been changed after replaceTaxonomy", origCreateTime, newCreateTime);
    
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
