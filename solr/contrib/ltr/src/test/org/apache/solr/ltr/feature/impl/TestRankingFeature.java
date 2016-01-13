package org.apache.solr.ltr.feature.impl;

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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.junit.Test;

public class TestRankingFeature extends TestQueryFeature {
  @Test
  public void testRankingSolrFeature() throws Exception {
    // before();
    loadFeature("powpularityS", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");
    loadFeature("unpopularityS", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"{!func}div(1,popularity)\"}");

    loadModel("powpularityS-model", RankSVMModel.class.getCanonicalName(),
        new String[] {"powpularityS"}, "{\"weights\":{\"powpularityS\":1.0}}");
    loadModel("unpopularityS-model", RankSVMModel.class.getCanonicalName(),
        new String[] {"unpopularityS"}, "{\"weights\":{\"unpopularityS\":1.0}}");

    SolrQuery query = new SolrQuery();
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
    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
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

    query.set("debugQuery", "on");
    res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='8'");
    // aftertest();

  }
}
