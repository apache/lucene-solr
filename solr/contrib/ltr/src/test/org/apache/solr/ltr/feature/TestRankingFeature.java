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

import java.util.LinkedHashMap;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestRankingFeature extends TestRerankBase {

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
  public void testRankingSolrFeature() throws Exception {
    // before();
    loadFeature("powpularityS", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");
    loadFeature("unpopularityS", SolrFeature.class.getName(),
        "{\"q\":\"{!func}div(1,popularity)\"}");

    loadModel("powpularityS-model", LinearModel.class.getName(),
        new String[] {"powpularityS"}, "{\"weights\":{\"powpularityS\":1.0}}");
    loadModel("unpopularityS-model", LinearModel.class.getName(),
        new String[] {"unpopularityS"}, "{\"weights\":{\"unpopularityS\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");
    // Normal term match

    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=4}");
    query.set("debugQuery", "on");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==64.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==49.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==36.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==1.0");

    query.remove("rq");
    query.add("rq", "{!ltr model=unpopularityS-model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='8'");

    //bad solr ranking feature
    loadFeature("powdesS", SolrFeature.class.getName(),
        "{\"q\":\"{!func}pow(description,2)\"}");
    loadModel("powdesS-model", LinearModel.class.getName(),
        new String[] {"powdesS"}, "{\"weights\":{\"powdesS\":1.0}}");

    query.remove("rq");
    query.add("rq", "{!ltr model=powdesS-model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(),
        "/error/msg/=='"+FeatureException.class.getName()+": " +
        "java.lang.UnsupportedOperationException: " +
        "Unable to extract feature for powdesS'");

  }

  @Test
  public void testParamsToMap() throws Exception {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<String,Object>();
    params.put("q", "{!func}pow(popularity,2)");
    doTestParamsToMap(SolrFeature.class.getName(), params);
  }

}
