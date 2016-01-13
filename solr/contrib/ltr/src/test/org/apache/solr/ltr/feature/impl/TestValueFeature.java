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
import org.apache.solr.ltr.ranking.LTRComponent;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestValueFeature extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");

    assertU(adoc("id", "1", "title", "w1"));
    assertU(adoc("id", "2", "title", "w2"));
    assertU(adoc("id", "3", "title", "w3"));
    assertU(adoc("id", "4", "title", "w4"));
    assertU(adoc("id", "5", "title", "w5"));
    assertU(adoc("id", "6", "title", "w1 w2"));
    assertU(adoc("id", "7", "title", "w1 w2 w3 w4 w5"));
    assertU(adoc("id", "8", "title", "w1 w1 w1 w2 w2"));
    assertU(commit());
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test(expected = Exception.class)
  public void testValueFeature1() throws Exception {
    loadFeature("c1", ValueFeature.class.getCanonicalName(), "{}");
  }

  @Test(expected = Exception.class)
  public void testValueFeature2() throws Exception {
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "{\"value\":\"\"}");
  }

  @Test(expected = Exception.class)
  public void testValueFeature3() throws Exception {
    loadFeature("c2", ValueFeature.class.getCanonicalName(),
        "{\"value\":\" \"}");
  }

  @Test
  public void testValueFeature4() throws Exception {
    loadFeature("c3", ValueFeature.class.getCanonicalName(), "c3",
        "{\"value\":2}");
    loadModel("m3", RankSVMModel.class.getCanonicalName(), new String[] {"c3"},
        "c3", "{\"weights\":{\"c3\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m3 reRankDocs=4}");

    // String res = restTestHarness.query("/query" + query.toQueryString());
    // System.out.println("\n\n333333\n\n" + res);

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==2.0");
  }

  @Test
  public void testValueFeature5() throws Exception {
    loadFeature("c4", ValueFeature.class.getCanonicalName(), "c4",
        "{\"value\":\"2\"}");
    loadModel("m4", RankSVMModel.class.getCanonicalName(), new String[] {"c4"},
        "c4", "{\"weights\":{\"c4\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m4 reRankDocs=4}");

    // String res = restTestHarness.query("/query" + query.toQueryString());
    // System.out.println("\n\n44444\n\n" + res);

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==2.0");
  }

  @Test
  public void testValueFeature6() throws Exception {
    loadFeature("c5", ValueFeature.class.getCanonicalName(), "c5",
        "{\"value\":\"${val5}\"}");
    loadModel("m5", RankSVMModel.class.getCanonicalName(), new String[] {"c5"},
        "c5", "{\"weights\":{\"c5\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score,fvonly:[fvonly]");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("rq", "{!ltr model=m5 reRankDocs=4}");

    // String res = restTestHarness.query("/query" + query.toQueryString());
    // System.out.println(res);

    // No efi.val passed in
    assertJQ("/query" + query.toQueryString(), "/responseHeader/status==400");
  }

  @Test
  public void testValueFeature7() throws Exception {
    loadFeature("c6", ValueFeature.class.getCanonicalName(), "c6",
        "{\"value\":\"${val6}\"}");
    loadModel("m6", RankSVMModel.class.getCanonicalName(), new String[] {"c6"},
        "c6", "{\"weights\":{\"c6\":1.0}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=m6 reRankDocs=4 efi.val6='2'}");

    // String res = restTestHarness.query("/query" + query.toQueryString());
    // System.out.println(res);

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==2.0");
  }
}
