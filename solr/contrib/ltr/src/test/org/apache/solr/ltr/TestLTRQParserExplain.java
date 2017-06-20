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
import org.apache.solr.ltr.model.LinearModel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLTRQParserExplain extends TestRerankBase {

  @BeforeClass
  public static void setup() throws Exception {
    setuptest(true);
    loadFeatures("features-store-test-model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }


  @Test
  public void testRerankedExplain() throws Exception {
    loadModel("linear2", LinearModel.class.getCanonicalName(), new String[] {
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

}
