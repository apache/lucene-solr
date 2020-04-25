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
package org.apache.solr.update;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test that runtime exceptions thrown during analysis
 * result in Solr errors that contain the document ID.
 */
public class AnalysisErrorHandlingTest extends SolrTestCaseJ4 {

  public String getCoreName() { return "basic"; }

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig-basic.xml","solr/analysisconfs/analysis-err-schema.xml");
  }

  @Test
  public void testMultipleUpdatesPerAdd() {
    clearIndex();
    SolrException se = expectThrows(SolrException.class,
        () -> h.update("<add><doc><field name=\"id\">1</field><field name=\"text\">Alas Poor Yorik</field></doc></add>")
    );
    assertTrue(se.getMessage().contains("Exception writing document id 1 to the index"));
  }
}
