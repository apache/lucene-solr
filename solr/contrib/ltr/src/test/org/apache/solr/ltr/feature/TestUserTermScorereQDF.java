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

public class TestUserTermScorereQDF extends TestRerankBase {

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
  public void testUserTermScorerQWithDF() throws Exception {
    // before();
    loadFeature("matchedTitleDF", SolrFeature.class.getName(),
        "{\"q\":\"w5\",\"df\":\"title\"}");
    loadModel("Term-matchedTitleDF", LinearModel.class.getName(),
        new String[] {"matchedTitleDF"},
        "{\"weights\":{\"matchedTitleDF\":1.0}}");
    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "2");
    query.add("rq", "{!ltr model=Term-matchedTitleDF reRankDocs=4}");
    query.set("debugQuery", "on");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
  }
}
