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
import org.apache.solr.schema.IndexSchema;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration test which uses {@link org.apache.lucene.analysis.uima.UIMAAnnotationsTokenizerFactory} in Solr schema
 */
public class UIMATokenizersSolrIntegrationTest extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("uima/uima-tokenizers-solrconfig.xml", "uima/uima-tokenizers-schema.xml");
  }

  @Test
  public void testInitialization() throws Exception {
    IndexSchema schema = h.getCore().getLatestSchema();
    assertNotNull(schema.getField("sentences"));
    assertNotNull(schema.getFieldType("sentences"));
    assertNotNull(schema.getField("nouns"));
    assertNotNull(schema.getFieldType("nouns"));
  }

  @Test
  public void testUIMATokenizerIndexAndQuery() throws Exception {
    assertU("<add><doc><field name=\"id\">123</field><field name=\"text\">One and 1 is two. Instead One or 1 is 0.</field></doc></add>");
    assertU(commit());
    SolrQueryRequest req = req("qt", "/terms", "terms.fl", "sentences");
    assertQ(req, "//lst[@name='sentences']/int[@name='One and 1 is two.']");
    assertQ(req, "//lst[@name='sentences']/int[@name=' Instead One or 1 is 0.']");
    req.close();
  }

  @Test
  public void testUIMATypeAwareTokenizerIndexAndQuery() throws Exception {
    assertU("<add><doc><field name=\"id\">123</field><field name=\"text\">The counter counts the beans: 1 and 2 and three.</field></doc></add>");
    assertU(commit());
    SolrQueryRequest req = req("qt", "/terms", "terms.fl", "nouns");
    assertQ(req, "//lst[@name='nouns']/int[@name='beans']");
    assertQ(req, "//lst[@name='nouns']/int[@name='counter']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='The']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='counts']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='the']");
    assertQ(req, "//lst[@name='nouns']/int[@name!=':']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='1']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='and']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='2']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='three']");
    assertQ(req, "//lst[@name='nouns']/int[@name!='.']");
    req.close();
  }
}
