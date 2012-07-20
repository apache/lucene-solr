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

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.DirectUpdateHandler2;
import org.apache.solr.util.RefCounted;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPropInjectDefaults extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-propinject-indexdefault.xml", "schema.xml");
  }

  @Test
  public void testMergePolicyDefaults() throws Exception {
    RefCounted<IndexWriter> iw = ((DirectUpdateHandler2) h.getCore()
        .getUpdateHandler()).getSolrCoreState().getIndexWriter(h.getCore());
    LogByteSizeMergePolicy mp;
    try {
      mp = (LogByteSizeMergePolicy) iw.get().getConfig().getMergePolicy();
    } finally {
      iw.decref();
    }
    assertEquals(32.0, mp.getMaxMergeMB(), 0);
  }
  
  @Test
  public void testPropsDefaults() throws Exception {
    RefCounted<IndexWriter> iw = ((DirectUpdateHandler2) h.getCore()
        .getUpdateHandler()).getSolrCoreState().getIndexWriter(h.getCore());
    ConcurrentMergeScheduler cms;
    try {
      cms = (ConcurrentMergeScheduler) iw.get().getConfig().getMergeScheduler();
    } finally {
      iw.decref();
    }
    assertEquals(4, cms.getMaxThreadCount());
  }

}
