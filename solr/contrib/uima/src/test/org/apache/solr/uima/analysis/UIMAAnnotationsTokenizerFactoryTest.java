package org.apache.solr.uima.analysis;

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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 */
public class UIMAAnnotationsTokenizerFactoryTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("uima/uima-tokenizers-solrconfig.xml", "uima/uima-tokenizers-schema.xml");
  }

  @Test
  public void testInitialization() throws Exception {
    assertNotNull(h.getCore().getLatestSchema().getField("sentences"));
    assertNotNull(h.getCore().getLatestSchema().getFieldType("sentences"));
  }

  @Test
  public void testIndexAndQuery() throws Exception {
    assertU("<add><doc><field name=\"id\">123</field><field name=\"text\">One and 1 is two. Instead One or 1 is 0.</field></doc></add>");
    assertU(commit());
    SolrQueryRequest req = req("qt", "/terms", "terms.fl", "sentences");
    assertQ(req, "//lst[@name='sentences']/int[@name='One and 1 is two.']");
    assertQ(req, "//lst[@name='sentences']/int[@name=' Instead One or 1 is 0.']");
    req.close();
  }
}
