package org.apache.solr.core;

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

import java.io.IOException;

import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.update.DirectUpdateHandler2;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPropInjectDefaults extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-propinject-indexdefault.xml", "schema.xml");
  }
  
  class ExposeWriterHandler extends DirectUpdateHandler2 {
    public ExposeWriterHandler() throws IOException {
      super(h.getCore());
    }

    public IndexWriter getWriter() throws IOException {
      forceOpenWriter();
      return writer;
    }
  }

  @Test
  public void testMergePolicyDefaults() throws Exception {
    ExposeWriterHandler uh = new ExposeWriterHandler();
    IndexWriter writer = uh.getWriter();
    LogByteSizeMergePolicy mp = (LogByteSizeMergePolicy)writer.getConfig().getMergePolicy();
    assertEquals(32.0, mp.getMaxMergeMB(), 0);
    uh.close();
  }
  
  @Test
  public void testPropsDefaults() throws Exception {
    ExposeWriterHandler uh = new ExposeWriterHandler();
    IndexWriter writer = uh.getWriter();
    ConcurrentMergeScheduler cms = (ConcurrentMergeScheduler)writer.getConfig().getMergeScheduler();
    assertEquals(4, cms.getMaxThreadCount());
    uh.close();
  }

}
