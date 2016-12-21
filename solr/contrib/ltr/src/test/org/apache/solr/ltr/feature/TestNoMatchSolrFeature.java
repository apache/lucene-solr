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

import java.util.ArrayList;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.model.MultipleAdditiveTreesModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;

public class TestNoMatchSolrFeature extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
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

    loadFeature("nomatchfeature", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"foobarbat12345\",\"df\":\"title\"}");
    loadFeature("yesmatchfeature", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"w1\",\"df\":\"title\"}");
    loadFeature("nomatchfeature2", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"foobarbat12345\",\"df\":\"title\"}");
    loadModel(
        "nomatchmodel",
        LinearModel.class.getCanonicalName(),
        new String[] {"nomatchfeature", "yesmatchfeature", "nomatchfeature2"},
        "{\"weights\":{\"nomatchfeature\":1.0,\"yesmatchfeature\":1.1,\"nomatchfeature2\":1.1}}");

    loadFeature("nomatchfeature3", SolrFeature.class.getCanonicalName(),
        "{\"q\":\"foobarbat12345\",\"df\":\"title\"}");
    loadModel("nomatchmodel2", LinearModel.class.getCanonicalName(),
        new String[] {"nomatchfeature3"},
        "{\"weights\":{\"nomatchfeature3\":1.0}}");

    loadFeature("nomatchfeature4", SolrFeature.class.getCanonicalName(),
        "noMatchFeaturesStore", "{\"q\":\"foobarbat12345\",\"df\":\"title\"}");
    loadModel("nomatchmodel3", LinearModel.class.getCanonicalName(),
        new String[] {"nomatchfeature4"}, "noMatchFeaturesStore",
        "{\"weights\":{\"nomatchfeature4\":1.0}}");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void test2NoMatch1YesMatchFeatureReturnsFvWith1FeatureAndDocScoreScaledByModel() throws Exception {
    // Tests model with all no matching features but 1
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,fv:[fv]");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=nomatchmodel reRankDocs=4}");

    final SolrQuery yesMatchFeatureQuery = new SolrQuery();
    yesMatchFeatureQuery.setQuery("title:w1");
    yesMatchFeatureQuery.add("fl", "score");
    yesMatchFeatureQuery.add("rows", "4");
    String res = restTestHarness.query("/query"
        + yesMatchFeatureQuery.toQueryString());

    final Map<String,Object> jsonParse = (Map<String,Object>) ObjectBuilder
        .fromJSON(res);
    final Double doc0Score = (Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(0)).get("score");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score=="
        + (doc0Score * 1.1));
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/fv=='"+FeatureLoggerTestUtils.toFeatureVector("yesmatchfeature", doc0Score.toString())+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/fv==''");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/fv==''");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='4'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/fv==''");
  }

  @Test
  public void test1NoMatchFeatureReturnsFvWith1MatchingFeatureFromStoreAndDocWith0Score() throws Exception {
    // Tests model with all no matching features, but 1 feature store feature matching for extraction
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,fv:[fv]");
    query.add("rows", "4");
    query.add("rq", "{!ltr model=nomatchmodel2 reRankDocs=4}");

    final SolrQuery yesMatchFeatureQuery = new SolrQuery();
    yesMatchFeatureQuery.setQuery("title:w1");
    yesMatchFeatureQuery.add("fl", "score");
    yesMatchFeatureQuery.add("rows", "4");
    String res = restTestHarness.query("/query"
        + yesMatchFeatureQuery.toQueryString());

    final Map<String,Object> jsonParse = (Map<String,Object>) ObjectBuilder
        .fromJSON(res);
    final Double doc0Score = (Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(0)).get("score");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.0");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/fv=='"+FeatureLoggerTestUtils.toFeatureVector("yesmatchfeature", doc0Score.toString())+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/fv==''");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/fv==''");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/fv==''");
  }

  @Test
  public void tesOnlyNoMatchFeaturesInStoreAndModelReturnsZeroScore() throws Exception {
    // Tests model with all no matching features
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,fv:[fv]");
    query.add("rows", "4");
    query.add("fv", "true");
    query.add("rq", "{!ltr model=nomatchmodel3 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv==''");
  }

  @Test
  public void tesOnlyNoMatchFeaturesInStoreAndModelReturnsNonzeroScore() throws Exception {
    // Tests model with all no matching features but expects a non 0 score
    //  MultipleAdditiveTrees will return scores even for docs without any feature matches
    loadModel(
        "nomatchmodel4",
        MultipleAdditiveTreesModel.class.getCanonicalName(),
        new String[] {"nomatchfeature4"},
        "noMatchFeaturesStore",
        "{\"trees\":[{\"weight\":\"1f\", \"root\":{\"feature\": \"matchedTitle\",\"threshold\": \"0.5f\",\"left\":{\"value\" : \"-10\"},\"right\":{\"value\" : \"9\"}}}]}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*, score,fv:[fv]");
    query.add("rows", "4");
    query.add("rq", "{!ltr model=nomatchmodel4 reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/score==0.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/fv==''");
  }

}
