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

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestFilterSolrFeature extends TestRerankBase {
  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2 2asd asdd didid", "description",
        "w2 2asd asdd didid", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w1", "description", "w1", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w1", "description", "w1", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(adoc("id", "6", "title", "w6 w2", "description", "w1 w2",
        "popularity", "6"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5", "description",
        "w6 w2 w3 w4 w5 w8", "popularity", "88888"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2 w8", "description",
        "w1 w1 w1 w2 w2", "popularity", "88888"));
    assertU(commit());
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void testUserTermScoreWithFQ() throws Exception {
    loadFeature("SomeTermFQ", SolrFeature.class.getCanonicalName(),
        "{\"fq\":[\"{!terms f=popularity}88888\"]}");
    loadFeature("SomeEfiFQ", SolrFeature.class.getCanonicalName(),
        "{\"fq\":[\"{!terms f=title}${user_query}\"]}");
    loadModel("Term-modelFQ", RankSVMModel.class.getCanonicalName(),
        new String[] {"SomeTermFQ", "SomeEfiFQ"},
        "{\"weights\":{\"SomeTermFQ\":1.6, \"SomeEfiFQ\":2.0}}");
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score");
    query.add("rows", "3");
    query.add("fq", "{!terms f=title}w1");
    query.add("rq",
        "{!ltr model=Term-modelFQ reRankDocs=5 efi.user_query='w5'}");

    String res = restTestHarness.query("/query" + query.toQueryString());
    System.out.println(res);
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==5");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==3.6");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.6");
  }

  @Test
  public void testBadFeature() throws Exception {
    // Missing q/fq
    String feature = getFeatureInJson("badFeature", "test",
        SolrFeature.class.getCanonicalName(), "{\"df\":\"foo\"]}");
    assertJPut(FEATURE_ENDPOINT, feature, "/responseHeader/status==500");
  }

}
