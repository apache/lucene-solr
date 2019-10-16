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
package org.apache.solr.ltr.feature;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestEdisMaxSolrFeature extends TestRerankBase {

  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "w1 w2", "description", "w1 w2",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w1 w2 w3 w4 w5 w8", "popularity", "7"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2", "popularity", "8"));
    assertU(commit());
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testEdisMaxSolrFeature() throws Exception {
    loadFeature(
        "SomeEdisMax",
        SolrFeature.class.getName(),
        "{\"q\":\"{!edismax qf='title description' pf='description' mm=100% boost='pow(popularity, 0.1)' v='w1' tie=0.1}\"}");

    loadModel("EdisMax-model", LinearModel.class.getName(),
        new String[] {"SomeEdisMax"}, "{\"weights\":{\"SomeEdisMax\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    query.add("rq", "{!ltr model=EdisMax-model reRankDocs=4}");
    query.set("debugQuery", "on");
    restTestHarness.query("/query" + query.toQueryString());
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
  }
}
