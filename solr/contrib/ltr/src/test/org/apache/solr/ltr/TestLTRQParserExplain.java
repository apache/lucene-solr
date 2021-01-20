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

import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.feature.SolrFeature;
import org.apache.solr.ltr.interleaving.algorithms.TeamDraftInterleaving;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLTRQParserExplain extends TestRerankBase {

  @Before
  public void setup() throws Exception {
    setuptest(true);
    loadFeatures("features-store-test-model.json");
  }

  @After
  public void after() throws Exception {
    aftertest();
  }


  @Test
  public void testRerankedExplain() throws Exception {
    loadModel("linear2", LinearModel.class.getName(), new String[] {
        "constant1", "constant2", "pop"},
        "{\"weights\":{\"pop\":1.0,\"constant1\":1.5,\"constant2\":3.5}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "2");
    query.add("rq", "{!ltr reRankDocs=2 model=linear2}");
    query.add("fl", "*,score");

    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='\n13.5 = LinearModel(name=linear2,featureWeights=[constant1=1.5,constant2=3.5,pop=1.0]) model applied to features, sum of:\n  1.5 = prod of:\n    1.5 = weight on feature\n    1.0 = ValueFeature [name=constant1, params={value=1}]\n  7.0 = prod of:\n    3.5 = weight on feature\n    2.0 = ValueFeature [name=constant2, params={value=2}]\n  5.0 = prod of:\n    1.0 = weight on feature\n    5.0 = FieldValueFeature [name=pop, params={field=popularity}]\n'");
  }

  @Test
  public void testRerankedExplainSameBetweenDifferentDocsWithSameFeatures() throws Exception {
    loadFeatures("features-linear.json");
    loadModels("linear-model.json");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "4");
    query.add("rq", "{!ltr reRankDocs=4 model=6029760550880411648}");
    query.add("fl", "*,score");
    query.add("wt", "json");
    final String expectedExplainNormalizer = "normalized using MinMaxNormalizer(min=0.0,max=10.0)";
    final String expectedExplain = "\n3.5116758 = LinearModel(name=6029760550880411648,featureWeights=["
        + "title=0.0,"
        + "description=0.1,"
        + "keywords=0.2,"
        + "popularity=0.3,"
        + "text=0.4,"
        + "queryIntentPerson=0.1231231,"
        + "queryIntentCompany=0.12121211"
        + "]) model applied to features, sum of:\n  0.0 = prod of:\n    0.0 = weight on feature\n    1.0 = ValueFeature [name=title, params={value=1}]\n  0.2 = prod of:\n    0.1 = weight on feature\n    2.0 = ValueFeature [name=description, params={value=2}]\n  0.4 = prod of:\n    0.2 = weight on feature\n    2.0 = ValueFeature [name=keywords, params={value=2}]\n  0.09 = prod of:\n    0.3 = weight on feature\n    0.3 = "+expectedExplainNormalizer+"\n      3.0 = ValueFeature [name=popularity, params={value=3}]\n  1.6 = prod of:\n    0.4 = weight on feature\n    4.0 = ValueFeature [name=text, params={value=4}]\n  0.6156155 = prod of:\n    0.1231231 = weight on feature\n    5.0 = ValueFeature [name=queryIntentPerson, params={value=5}]\n  0.60606056 = prod of:\n    0.12121211 = weight on feature\n    5.0 = ValueFeature [name=queryIntentCompany, params={value=5}]\n";

    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/7=='"+expectedExplain+"'}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='"+expectedExplain+"'}");
  }

  @Test
  public void LinearScoreExplainMissingEfiFeatureShouldReturnDefaultScore() throws Exception {
    loadFeatures("features-linear-efi.json");
    loadModels("linear-model-efi.json");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "4");
    query.add("rq", "{!ltr reRankDocs=4 model=linear-efi}");
    query.add("fl", "*,score");
    query.add("wt", "xml");

    final String linearModelEfiString = "LinearModel(name=linear-efi,featureWeights=["
      + "sampleConstant=1.0,"
      + "search_number_of_nights=2.0])";

    query.remove("wt");
    query.add("wt", "json");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/7=='\n5.0 = "+linearModelEfiString+" model applied to features, sum of:\n  5.0 = prod of:\n    1.0 = weight on feature\n    5.0 = ValueFeature [name=sampleConstant, params={value=5}]\n" +
            "  0.0 = prod of:\n" +
            "    2.0 = weight on feature\n" +
            "    0.0 = The feature has no value\n'}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='\n5.0 = "+linearModelEfiString+" model applied to features, sum of:\n  5.0 = prod of:\n    1.0 = weight on feature\n    5.0 = ValueFeature [name=sampleConstant, params={value=5}]\n" +
            "  0.0 = prod of:\n" +
            "    2.0 = weight on feature\n" +
            "    0.0 = The feature has no value\n'}");
  }

  @Test
  public void multipleAdditiveTreesScoreExplainMissingEfiFeatureShouldReturnDefaultScore() throws Exception {
    loadFeatures("external_features_for_sparse_processing.json");
    loadModels("multipleadditivetreesmodel_external_binary_features.json");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "4");
    query.add("rq", "{!ltr reRankDocs=4 model=external_model_binary_feature efi.user_device_tablet=1}");
    query.add("fl", "*,score");

    final String tree1 = "(weight=1.0,root=(feature=user_device_smartphone,threshold=0.5,left=0.0,right=50.0))";
    final String tree2 = "(weight=1.0,root=(feature=user_device_tablet,threshold=0.5,left=0.0,right=65.0))";
    final String trees = "["+tree1+","+tree2+"]";

    query.add("wt", "json");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/7=='\n" +
            "65.0 = MultipleAdditiveTreesModel(name=external_model_binary_feature,trees="+trees+") model applied to features, sum of:\n" +
            "  0.0 = tree 0 | \\'user_device_smartphone\\':0.0 <= 0.500001, Go Left | val: 0.0\n" +
            "  65.0 = tree 1 | \\'user_device_tablet\\':1.0 > 0.500001, Go Right | val: 65.0\n'}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='\n" +
            "65.0 = MultipleAdditiveTreesModel(name=external_model_binary_feature,trees="+trees+") model applied to features, sum of:\n" +
            "  0.0 = tree 0 | \\'user_device_smartphone\\':0.0 <= 0.500001, Go Left | val: 0.0\n" +
            "  65.0 = tree 1 | \\'user_device_tablet\\':1.0 > 0.500001, Go Right | val: 65.0\n'}");
  }

  @Test
  public void interleavingModels_shouldReturnExplainForTheModelPicked() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10));//Random Boolean Choices Generation from Seed: [1,0]

    loadFeature("featureA1", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}1\"]}");
    loadFeature("featureA2", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=description}bloomberg\"]}");
    loadFeature("featureAB", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}2\"]}");
    loadFeature("featureB1", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}5\"]}");
    loadFeature("featureB2", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=title}different\"]}");

    loadModel("modelA", LinearModel.class.getName(),
        new String[]{"featureA1", "featureA2", "featureAB"},
        "{\"weights\":{\"featureA1\":3.0, \"featureA2\":9.0, \"featureAB\":27.0}}");

    loadModel("modelB", LinearModel.class.getName(),
        new String[]{"featureB1", "featureB2", "featureAB"},
        "{\"weights\":{\"featureB1\":2.0, \"featureB2\":4.0, \"featureAB\":8.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "10");
    query.add("rq", "{!ltr reRankDocs=10 model=modelA model=modelB}");
    query.add("fl", "*,score");

    /*
    Doc6 = "featureA1=1.0 featureA2=1.0 featureB2=1.0", ScoreA(12), ScoreB(4)
    Doc7 = "featureA2=1.0 featureAB=1.0", ScoreA(36), ScoreB(8)
    Doc8 = "featureA2=1.0", ScoreA(9), ScoreB(0)
    Doc9 = "featureA2=1.0 featureB1=1.0", ScoreA(9), ScoreB(2)
    
    ModelARerankedList = [7,6,8,9]
    ModelBRerankedList = [7,6,9,8]

    Random Boolean Choices Generation from Seed: [1,0]
    
    */

    int[] expectedInterleaved = new int[]{7, 6, 8, 9};
    String[] expectedExplains = new String[]{
        "\n8.0 = LinearModel(name=modelB," +
            "featureWeights=[featureB1=2.0,featureB2=4.0,featureAB=8.0]) " +
            "model applied to features, sum of:\n  " +
            "0.0 = prod of:\n    2.0 = weight on feature\n    0.0 = SolrFeature [name=featureB1, params={fq=[{!terms f=popularity}5]}]\n  " +
            "0.0 = prod of:\n    4.0 = weight on feature\n    0.0 = SolrFeature [name=featureB2, params={fq=[{!terms f=title}different]}]\n  " +
            "8.0 = prod of:\n    8.0 = weight on feature\n    1.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n",
        "\n12.0 = LinearModel(name=modelA," +
            "featureWeights=[featureA1=3.0,featureA2=9.0,featureAB=27.0]) " +
            "model applied to features, sum of:\n  " +
            "3.0 = prod of:\n    3.0 = weight on feature\n    1.0 = SolrFeature [name=featureA1, params={fq=[{!terms f=popularity}1]}]\n  " +
            "9.0 = prod of:\n    9.0 = weight on feature\n    1.0 = SolrFeature [name=featureA2, params={fq=[{!terms f=description}bloomberg]}]\n  " +
            "0.0 = prod of:\n    27.0 = weight on feature\n    0.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n",
        "\n9.0 = LinearModel(name=modelA," +
            "featureWeights=[featureA1=3.0,featureA2=9.0,featureAB=27.0]) " +
            "model applied to features, sum of:\n  " +
            "0.0 = prod of:\n    3.0 = weight on feature\n    0.0 = SolrFeature [name=featureA1, params={fq=[{!terms f=popularity}1]}]\n  " +
            "9.0 = prod of:\n    9.0 = weight on feature\n    1.0 = SolrFeature [name=featureA2, params={fq=[{!terms f=description}bloomberg]}]\n  " +
            "0.0 = prod of:\n    27.0 = weight on feature\n    0.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n",
        "\n2.0 = LinearModel(name=modelB," +
            "featureWeights=[featureB1=2.0,featureB2=4.0,featureAB=8.0]) " +
            "model applied to features, sum of:\n  " +
            "2.0 = prod of:\n    2.0 = weight on feature\n    1.0 = SolrFeature [name=featureB1, params={fq=[{!terms f=popularity}5]}]\n  " +
            "0.0 = prod of:\n    4.0 = weight on feature\n    0.0 = SolrFeature [name=featureB2, params={fq=[{!terms f=title}different]}]\n  " +
            "0.0 = prod of:\n    8.0 = weight on feature\n    0.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n"};
    
   

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==4";
    for (int i = 1; i <= 4; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 4] = "/debug/explain/" + expectedInterleaved[(i - 1)] + "=='" + expectedExplains[(i - 1)]+"'}";
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

  @Test
  public void interleavingModelsWithOriginalRanking_shouldReturnExplainForTheModelPicked() throws Exception {
    TeamDraftInterleaving.setRANDOM(new Random(10));//Random Boolean Choices Generation from Seed: [1,0]

    loadFeature("featureA1", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}1\"]}");
    loadFeature("featureA2", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=description}bloomberg\"]}");
    loadFeature("featureAB", SolrFeature.class.getName(),
        "{\"fq\":[\"{!terms f=popularity}2\"]}");

    loadModel("modelA", LinearModel.class.getName(),
        new String[]{"featureA1", "featureA2", "featureAB"},
        "{\"weights\":{\"featureA1\":3.0, \"featureA2\":9.0, \"featureAB\":27.0}}");

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "10");
    query.add("rq", "{!ltr reRankDocs=10 model=modelA model=_OriginalRanking_}");
    query.add("fl", "*,score");

    /*
    Doc6 = "featureA1=1.0 featureA2=1.0 featureB2=1.0", ScoreA(12)
    Doc7 = "featureA2=1.0 featureAB=1.0", ScoreA(36)
    Doc8 = "featureA2=1.0", ScoreA(9)
    Doc9 = "featureA2=1.0 featureB1=1.0", ScoreA(9)
    
    ModelARerankedList = [7,6,8,9]
    OriginalRanking = [9,8,7,6]

    Random Boolean Choices Generation from Seed: [1,0]
    
    */

    int[] expectedInterleaved = new int[]{9, 7, 6, 8};
    String[] expectedExplains = new String[]{
        "\n0.07662583 = weight(title:bloomberg in 3) [SchemaSimilarity], result of:\n  " +
            "0.07662583 = score(freq=4.0), computed as boost * idf * tf from:\n    " +
            "0.105360515 = idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:\n      4 = n, number of documents containing term\n      4 = N, total number of documents with field\n    " +
            "0.72727275 = tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:\n      4.0 = freq, occurrences of term within document\n      " +
            "1.2 = k1, term saturation parameter\n      " +
            "0.75 = b, length normalization parameter\n      " +
            "4.0 = dl, length of field\n      " +
            "3.0 = avgdl, average length of field\n",
        "\n36.0 = LinearModel(name=modelA," +
            "featureWeights=[featureA1=3.0,featureA2=9.0,featureAB=27.0]) " +
            "model applied to features, sum of:\n  " +
            "0.0 = prod of:\n    3.0 = weight on feature\n    0.0 = SolrFeature [name=featureA1, params={fq=[{!terms f=popularity}1]}]\n  " +
            "9.0 = prod of:\n    9.0 = weight on feature\n    1.0 = SolrFeature [name=featureA2, params={fq=[{!terms f=description}bloomberg]}]\n  " +
            "27.0 = prod of:\n    27.0 = weight on feature\n    1.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n",
        "\n12.0 = LinearModel(name=modelA," +
            "featureWeights=[featureA1=3.0,featureA2=9.0,featureAB=27.0]) " +
            "model applied to features, sum of:\n  " +
            "3.0 = prod of:\n    3.0 = weight on feature\n    1.0 = SolrFeature [name=featureA1, params={fq=[{!terms f=popularity}1]}]\n  " +
            "9.0 = prod of:\n    9.0 = weight on feature\n    1.0 = SolrFeature [name=featureA2, params={fq=[{!terms f=description}bloomberg]}]\n  " +
            "0.0 = prod of:\n    27.0 = weight on feature\n    0.0 = SolrFeature [name=featureAB, params={fq=[{!terms f=popularity}2]}]\n",
        "\n0.07525751 = weight(title:bloomberg in 2) [SchemaSimilarity], result of:\n  " +
            "0.07525751 = score(freq=3.0), computed as boost * idf * tf from:\n    " +
            "0.105360515 = idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:\n      4 = n, number of documents containing term\n      4 = N, total number of documents with field\n    " +
            "0.71428573 = tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:\n      3.0 = freq, occurrences of term within document\n      " +
            "1.2 = k1, term saturation parameter\n      " +
            "0.75 = b, length normalization parameter\n      " +
            "3.0 = dl, length of field\n      " +
            "3.0 = avgdl, average length of field\n"};

    String[] tests = new String[16];
    tests[0] = "/response/numFound/==4";
    for (int i = 1; i <= 4; i++) {
      tests[i] = "/response/docs/[" + (i - 1) + "]/id==\"" + expectedInterleaved[(i - 1)] + "\"";
      tests[i + 4] = "/debug/explain/" + expectedInterleaved[(i - 1)] + "=='" + expectedExplains[(i - 1)]+"'}";
    }
    assertJQ("/query" + query.toQueryString(), tests);
  }

}
