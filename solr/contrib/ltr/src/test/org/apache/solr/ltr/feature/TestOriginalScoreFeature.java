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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.ObjectBuilder;

public class TestOriginalScoreFeature extends TestRerankBase {

  @BeforeClass
  public static void before() throws Exception {
    setuptest(false);

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

  @Test
  public void testOriginalScore() throws Exception {
    loadFeature("score", OriginalScoreFeature.class.getCanonicalName(), "{}");

    loadModel("originalScore", LinearModel.class.getCanonicalName(),
        new String[] {"score"}, "{\"weights\":{\"score\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    final Map<String,Object> jsonParse = (Map<String,Object>) ObjectBuilder
        .fromJSON(res);
    final String doc0Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(0)).get("score")).toString();
    final String doc1Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(1)).get("score")).toString();
    final String doc2Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(2)).get("score")).toString();
    final String doc3Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(3)).get("score")).toString();

    query.add("fl", "[fv]");
    query.add("rq", "{!ltr model=originalScore reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score=="
        + doc0Score);
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score=="
        + doc1Score);
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score=="
        + doc2Score);
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/score=="
        + doc3Score);
  }

  @Test
  public void testOriginalScoreWithNonScoringFeatures() throws Exception {
    loadFeature("origScore", OriginalScoreFeature.class.getCanonicalName(),
        "store2", "{}");
    loadFeature("c2", ValueFeature.class.getCanonicalName(), "store2",
        "{\"value\":2.0}");

    loadModel("origScore", LinearModel.class.getCanonicalName(),
        new String[] {"origScore"}, "store2",
        "{\"weights\":{\"origScore\":1.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score, fv:[fv]");
    query.add("rows", "4");
    query.add("wt", "json");
    query.add("rq", "{!ltr model=origScore reRankDocs=4}");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    final Map<String,Object> jsonParse = (Map<String,Object>) ObjectBuilder
        .fromJSON(res);
    final String doc0Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(0)).get("score")).toString();
    final String doc1Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(1)).get("score")).toString();
    final String doc2Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(2)).get("score")).toString();
    final String doc3Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(3)).get("score")).toString();

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/fv=='" + FeatureLoggerTestUtils.toFeatureVector("origScore", doc0Score, "c2", "2.0")+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");

    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[1]/fv=='" + FeatureLoggerTestUtils.toFeatureVector("origScore", doc1Score, "c2", "2.0")+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[2]/fv=='" + FeatureLoggerTestUtils.toFeatureVector("origScore", doc2Score, "c2", "2.0")+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[3]/fv=='" + FeatureLoggerTestUtils.toFeatureVector("origScore", doc3Score, "c2", "2.0")+"'");
  }

}
