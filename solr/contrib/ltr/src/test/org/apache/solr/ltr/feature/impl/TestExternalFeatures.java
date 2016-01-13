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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestExternalFeatures extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity",
        "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity",
        "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    assertU(commit());

    loadFeatures("external_features.json");
    loadModels("external_model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void externalTest1() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score");
    query.add("rows", "3");
    query.add(LTRComponent.LTRParams.FV, "true");

    // Regular scores
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==1.0");

    query.add("fl", "[fv]");
    // Model is not specified so we should get a model does not exist exception
    assertJQ("/query" + query.toQueryString(), "/error/msg=='model is null'");

    // No match scores since user_query not passed in to external feature info
    // and feature depended on it.
    query.add("rq", "{!ltr reRankDocs=3 model=externalmodel}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==0.0");

    // Matched user query since it was passed in
    query.remove("rq");
    query
        .add("rq", "{!ltr reRankDocs=3 model=externalmodel efi.user_query=w3}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/score==0.999");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==0.0");

    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
  }

  @Test
  public void externalStopwordTest() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,fv:[fv]");
    query.add("rows", "1");
    query.add(LTRComponent.LTRParams.FV, "true");
    // Stopword only query passed in
    query.add("rq",
        "{!ltr reRankDocs=3 model=externalmodel efi.user_query='a'}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv==''");

    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
  }

}
