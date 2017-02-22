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

package org.apache.solr.ltr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLTRWithSort extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);
    assertU(adoc("id", "1", "title", "a1", "description", "E", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "a1 b1", "description",
        "B", "popularity", "2"));
    assertU(adoc("id", "3", "title", "a1 b1 c1", "description", "B", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "a1 b1 c1 d1", "description", "B", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "a1 b1 c1 d1 e1", "description", "E", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "a1 b1 c1 d1 e1 f1", "description", "B",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "a1 b1 c1 d1 e1 f1 g1", "description",
        "C", "popularity", "7"));
    assertU(adoc("id", "8", "title", "a1 b1 c1 d1 e1 f1 g1 h1", "description",
        "D", "popularity", "8"));
    assertU(commit());
  }

  @Test
  public void testRankingSolrSort() throws Exception {
    // before();
    loadFeature("powpularityS", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"{!func}pow(popularity,2)\"}");

    loadModel("powpularityS-model", LinearModel.class.getCanonicalName(),
        new String[] {"powpularityS"}, "{\"weights\":{\"powpularityS\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:a1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='4'");

    //Add sort
    query.add("sort", "description desc");
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='5'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    query.add("rq", "{!ltr model=powpularityS-model reRankDocs=4}");
    query.set("debugQuery", "on");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==8");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==64.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==49.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='5'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==25.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==1.0");

    // aftertest();

  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

}
