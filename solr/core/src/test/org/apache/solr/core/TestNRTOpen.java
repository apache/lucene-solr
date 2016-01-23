package org.apache.solr.core;

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

import java.io.File;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestNRTOpen extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    // use a filesystem, because we need to create an index, then "start up solr"
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    // and dont delete it initially
    System.setProperty("solr.test.leavedatadir", "true");
    // set these so that merges won't break the test
    System.setProperty("solr.tests.maxBufferedDocs", "100000");
    System.setProperty("solr.tests.mergePolicy", "org.apache.lucene.index.LogDocMergePolicy");
    initCore("solrconfig-basic.xml", "schema-minimal.xml");
    // add a doc
    assertU(adoc("foo", "bar"));
    assertU(commit());
    File myDir = initCoreDataDir;
    deleteCore();
    // boot up again over the same index
    initCoreDataDir = myDir;
    initCore("solrconfig-basic.xml", "schema-minimal.xml");
    // startup
    assertNRT(1);
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    // ensure we clean up after ourselves, this will fire before superclass...
    System.clearProperty("solr.test.leavedatadir");
    System.clearProperty("solr.directoryFactory");
    System.clearProperty("solr.tests.maxBufferedDocs");
    System.clearProperty("solr.tests.mergePolicy");
  }
  
  public void setUp() throws Exception {
    super.setUp();
    // delete all, then add initial doc
    assertU(delQ("*:*"));
    assertU(adoc("foo", "bar"));
    assertU(commit());
  }
  
  public void testReaderIsNRT() {
    // core reload
    String core = h.getCore().getName();
    h.getCoreContainer().reload(core);
    assertNRT(1);
    
    // add a doc and soft commit
    assertU(adoc("baz", "doc"));
    assertU(commit("softCommit", "true"));
    assertNRT(2);
    
    // add a doc and hard commit
    assertU(adoc("bazz", "doc"));
    assertU(commit());
    assertNRT(3);
    
    // add a doc and core reload
    assertU(adoc("bazzz", "doc2"));
    h.getCoreContainer().reload(core);
    assertNRT(4);
  }
  
  public void testSharedCores() {
    // clear out any junk
    assertU(optimize());
    
    Set<Object> s1 = getCoreCacheKeys();
    assertEquals(1, s1.size());
    
    // add a doc, will go in a new segment
    assertU(adoc("baz", "doc"));
    assertU(commit("softCommit", "true"));
    
    Set<Object> s2 = getCoreCacheKeys();
    assertEquals(2, s2.size());
    assertTrue(s2.containsAll(s1));
    
    // add two docs, will go in a new segment
    assertU(adoc("foo", "doc"));
    assertU(adoc("foo2", "doc"));
    assertU(commit());
    
    Set<Object> s3 = getCoreCacheKeys();
    assertEquals(3, s3.size());
    assertTrue(s3.containsAll(s2));
    
    // delete a doc
    assertU(delQ("foo2:doc"));
    assertU(commit());
    
    // same cores
    assertEquals(s3, getCoreCacheKeys());
  }
  
  static void assertNRT(int maxDoc) {
    RefCounted<SolrIndexSearcher> searcher = h.getCore().getSearcher();
    try {
      DirectoryReader ir = searcher.get().getRawReader();
      assertEquals(maxDoc, ir.maxDoc());
      assertTrue("expected NRT reader, got: " + ir, ir.toString().contains(":nrt"));
    } finally {
      searcher.decref();
    }
  }
  
  private Set<Object> getCoreCacheKeys() {
    RefCounted<SolrIndexSearcher> searcher = h.getCore().getSearcher();
    Set<Object> set = Collections.newSetFromMap(new IdentityHashMap<Object,Boolean>());
    try {
      DirectoryReader ir = searcher.get().getRawReader();
      for (LeafReaderContext context : ir.leaves()) {
        set.add(context.reader().getCoreCacheKey());
      }
    } finally {
      searcher.decref();
    }
    return set;
  }
}
