package org.apache.lucene.facet.taxonomy.directory;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.junit.Test;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;

/**
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
  private static class NoOpCache implements TaxonomyWriterCache {

    NoOpCache() { }
    
    public void close() {}
    public int get(CategoryPath categoryPath) { return -1; }
    public int get(CategoryPath categoryPath, int length) { return get(categoryPath); }
    public boolean put(CategoryPath categoryPath, int ordinal) { return true; }
    public boolean put(CategoryPath categoryPath, int prefixLen, int ordinal) { return true; }
    public boolean hasRoom(int numberOfEntries) { return false; }
    
  }
  
  @Test
  public void testCommit() throws Exception {
    // Verifies that nothing is committed to the underlying Directory, if
    // commit() wasn't called.
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, new NoOpCache());
    assertFalse(IndexReader.indexExists(dir));
    ltw.commit(); // first commit, so that an index will be created
    ltw.addCategory(new CategoryPath("a"));
    
    IndexReader r = IndexReader.open(dir);
    assertEquals("No categories should have been committed to the underlying directory", 1, r.numDocs());
    r.close();
    ltw.close();
    dir.close();
  }
  
  @Test
  public void testCommitUserData() throws Exception {
    // Verifies that committed data is retrievable
    Directory dir = newDirectory();
    DirectoryTaxonomyWriter ltw = new DirectoryTaxonomyWriter(dir, OpenMode.CREATE_OR_APPEND, new NoOpCache());
    assertFalse(IndexReader.indexExists(dir));
    ltw.commit(); // first commit, so that an index will be created
    ltw.addCategory(new CategoryPath("a"));
    ltw.addCategory(new CategoryPath("b"));
    Map <String, String> userCommitData = new HashMap<String, String>();
    userCommitData.put("testing", "1 2 3");
    ltw.commit(userCommitData);
    ltw.close();
    IndexReader r = IndexReader.open(dir);
    assertEquals("2 categories plus root should have been committed to the underlying directory", 3, r.numDocs());
    Map <String, String> readUserCommitData = r.getIndexCommit().getUserData();
    assertTrue("wrong value extracted from commit data", 
        "1 2 3".equals(readUserCommitData.get("testing")));
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
  
}
