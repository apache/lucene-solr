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
package org.apache.solr.core;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LiveIndexWriterConfig;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.RandomMergePolicy;
import org.apache.solr.update.LoggingInfoStream;
import org.junit.BeforeClass;

// See: https://issues.apache.org/jira/browse/SOLR-12028 Tests cannot remove files on Windows machines occasionally
public class TestSolrIndexConfig extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-indexconfig-mergepolicyfactory.xml","schema.xml");
  }

  public void testLiveWriter() throws Exception {
    SolrCore core = h.getCore();
    RefCounted<IndexWriter> iw = core.getUpdateHandler().getSolrCoreState().getIndexWriter(core);
    try {
      checkIndexWriterConfig(iw.get().getConfig());
    } finally {
      if (null != iw) iw.decref();
    }
  }

  
  public void testIndexConfigParsing() throws Exception {
    IndexWriterConfig iwc = solrConfig.indexConfig.toIndexWriterConfig(h.getCore());
    try {
      checkIndexWriterConfig(iwc);
    } finally {
      iwc.getInfoStream().close();
    }
  }

  private void checkIndexWriterConfig(LiveIndexWriterConfig iwc) {

    assertTrue(iwc.getInfoStream() instanceof LoggingInfoStream);
    assertTrue(iwc.getMergePolicy().getClass().toString(),
               iwc.getMergePolicy() instanceof RandomMergePolicy);

  }

}
