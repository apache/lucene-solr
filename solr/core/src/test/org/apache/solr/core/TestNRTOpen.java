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
    initCore("solrconfig-basic.xml", "schema-minimal.xml");
    // add a doc
    assertU(adoc("foo", "bar"));
    assertU(commit());
    File myDir = dataDir;
    deleteCore();
    // boot up again over the same index
    dataDir = myDir;
    initCore("solrconfig-basic.xml", "schema-minimal.xml");
  }
  
  @AfterClass
  public static void afterClass() throws Exception {
    // ensure we clean up after ourselves, this will fire before superclass...
    System.clearProperty("solr.test.leavedatadir");
    System.clearProperty("solr.directoryFactory");
  }
  
  public void testReaderIsNRT() {
    assertNRT();
    String core = h.getCore().getName();
    h.getCoreContainer().reload(core);
    assertNRT();
  }
  
  private void assertNRT() {
    RefCounted<SolrIndexSearcher> searcher = h.getCore().getSearcher();
    try {
      DirectoryReader ir = searcher.get().getIndexReader();
      assertEquals(1, ir.maxDoc());
      assertTrue("expected NRT reader, got: " + ir, ir.toString().contains(":nrt"));
    } finally {
      searcher.decref();
    }
  }
}
