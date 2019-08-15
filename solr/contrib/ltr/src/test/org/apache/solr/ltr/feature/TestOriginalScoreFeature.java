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
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestOriginalScoreFeature extends TestRerankBase {

  @Before
  public void before() throws Exception {
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

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testOriginalScore() throws Exception {
    loadFeature("score", OriginalScoreFeature.class.getName(), "{}");
    loadModel("originalScore", LinearModel.class.getName(),
        new String[] {"score"}, "{\"weights\":{\"score\":1.0}}");

    implTestOriginalScoreResponseDocsCheck("originalScore", "score", null, null);
  }

  @Test
  public void testOriginalScoreWithNonScoringFeatures() throws Exception {
    loadFeature("origScore", OriginalScoreFeature.class.getName(),
        "store2", "{}");
    loadFeature("c2", ValueFeature.class.getName(), "store2",
        "{\"value\":2.0}");

    loadModel("origScore", LinearModel.class.getName(),
        new String[] {"origScore"}, "store2",
        "{\"weights\":{\"origScore\":1.0}}");

    implTestOriginalScoreResponseDocsCheck("origScore", "origScore", "c2", "2.0");
  }

  @SuppressWarnings("unchecked")
  public static void implTestOriginalScoreResponseDocsCheck(String modelName,
      String origScoreFeatureName,
      String nonScoringFeatureName, String nonScoringFeatureValue) throws Exception {

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");
    query.add("wt", "json");

    final int doc0Id = 1;
    final int doc1Id = 8;
    final int doc2Id = 6;
    final int doc3Id = 7;

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='"+doc0Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='"+doc1Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='"+doc2Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='"+doc3Id+"'");

    final String res = restTestHarness.query("/query" + query.toQueryString());
    final Map<String,Object> jsonParse = (Map<String,Object>) Utils
        .fromJSONString (res);
    final String doc0Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(0)).get("score")).toString();
    final String doc1Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(1)).get("score")).toString();
    final String doc2Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(2)).get("score")).toString();
    final String doc3Score = ((Double) ((Map<String,Object>) ((ArrayList<Object>) ((Map<String,Object>) jsonParse
        .get("response")).get("docs")).get(3)).get("score")).toString();

    final boolean debugQuery = random().nextBoolean();
    if (debugQuery) {
      query.add(CommonParams.DEBUG_QUERY, "true");
    }

    query.remove("fl");
    query.add("fl", "*, score, fv:[fv]");
    query.add("rq", "{!ltr model="+modelName+" reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='"+doc0Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='"+doc1Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='"+doc2Id+"'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='"+doc3Id+"'");

    implTestOriginalScoreResponseDocsCheck(modelName, query, 0, doc0Id, origScoreFeatureName, doc0Score,
        nonScoringFeatureName, nonScoringFeatureValue, debugQuery);
    implTestOriginalScoreResponseDocsCheck(modelName, query, 1, doc1Id, origScoreFeatureName, doc1Score,
        nonScoringFeatureName, nonScoringFeatureValue, debugQuery);
    implTestOriginalScoreResponseDocsCheck(modelName, query, 2, doc2Id, origScoreFeatureName, doc2Score,
        nonScoringFeatureName, nonScoringFeatureValue, debugQuery);
    implTestOriginalScoreResponseDocsCheck(modelName, query, 3, doc3Id, origScoreFeatureName, doc3Score,
        nonScoringFeatureName, nonScoringFeatureValue, debugQuery);
  }

  private static void implTestOriginalScoreResponseDocsCheck(String modelName,
      SolrQuery query, int docIdx, int docId,
      String origScoreFeatureName, String origScoreFeatureValue,
      String nonScoringFeatureName, String nonScoringFeatureValue,
      boolean debugQuery) throws Exception {

    final String fv;
    if (nonScoringFeatureName == null) {
      fv = FeatureLoggerTestUtils.toFeatureVector(origScoreFeatureName, origScoreFeatureValue);
    } else {
      fv = FeatureLoggerTestUtils.toFeatureVector(origScoreFeatureName, origScoreFeatureValue, nonScoringFeatureName, nonScoringFeatureValue);
    }

    assertJQ("/query" + query.toQueryString(), "/response/docs/["+docIdx+"]/fv=='"+fv+"'");
    if (debugQuery) {
      assertJQ("/query" + query.toQueryString(),
          "/debug/explain/"+docId+"=='\n"+origScoreFeatureValue+" = LinearModel(name="+modelName+",featureWeights=["+origScoreFeatureName+"=1.0]) model applied to features, sum of:\n  "+origScoreFeatureValue+" = prod of:\n    1.0 = weight on feature\n    "+origScoreFeatureValue+" = OriginalScoreFeature [query:"+query.getQuery()+"]\n'");
    }
  }

  @Test
  public void testParamsToMap() throws Exception {
    final LinkedHashMap<String,Object> params = new LinkedHashMap<String,Object>();
    doTestParamsToMap(OriginalScoreFeature.class.getName(), params);
  }

}
