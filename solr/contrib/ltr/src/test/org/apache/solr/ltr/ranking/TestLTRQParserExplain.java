package org.apache.solr.ltr.ranking;

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

import java.lang.invoke.MethodHandles;

import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL
public class TestLTRQParserExplain extends TestRerankBase {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    loadFeatures("features-store-test-model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void checkReranked() throws Exception {

    loadModel("svm", RankSVMModel.class.getCanonicalName(), new String[] {
        "constant1", "constant2"},
        "{\"weights\":{\"constant1\":1.5,\"constant2\":3.5}}");
    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "2");
    query.add("rq", "{!ltr reRankDocs=2 model=svm}");
    query.add("fl", "*,score");
    // query.add("wt","xml");
    // System.out.println(restTestHarness.query("/query" +
    // query.toQueryString()));
    // query.add("wt","json");
    // assertJQ(
    // "/query" + query.toQueryString(),
    // "/debug/explain/7=='\n8.5 = svm [ org.apache.solr.ltr.ranking.RankSVMModel ] model applied to features, sum of:\n  1.5 = prod of:\n    1.5 = weight on feature [would be cool to have the name :)]\n    1.0 = ValueFeature [name=constant1 value=1.0]\n  7.0 = prod of:\n    3.5 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=constant2 value=2.0]\n'");
    query.add("wt", "xml");
    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
  }

  @Test
  public void checkReranked2() throws Exception {
    loadModel("svm2", RankSVMModel.class.getCanonicalName(), new String[] {
        "constant1", "constant2", "pop"},
        "{\"weights\":{\"pop\":1.0,\"constant1\":1.5,\"constant2\":3.5}}");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "2");
    query.add("rq", "{!ltr reRankDocs=2 model=svm2}");
    query.add("fl", "*,score");

    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='\n13.5 = svm2 [ org.apache.solr.ltr.ranking.RankSVMModel ] model applied to features, sum of:\n  1.5 = prod of:\n    1.5 = weight on feature [would be cool to have the name :)]\n    1.0 = ValueFeature [name=constant1 value=1.0]\n  7.0 = prod of:\n    3.5 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=constant2 value=2.0]\n  5.0 = prod of:\n    1.0 = weight on feature [would be cool to have the name :)]\n    5.0 = FieldValueFeature [name=pop fields=[popularity]]\n'");
    query.add("wt", "xml");
    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
  }

  @Test
  public void checkReranked3() throws Exception {
    loadFeatures("features-ranksvm.json");
    loadModels("ranksvm-model.json");

    SolrQuery query = new SolrQuery();
    query.setQuery("title:bloomberg");
    query.setParam("debugQuery", "on");
    query.add("rows", "4");
    query.add("rq", "{!ltr reRankDocs=4 model=6029760550880411648}");
    query.add("fl", "*,score");
    query.add("wt", "xml");

    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
    query.remove("wt");
    query.add("wt", "json");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/7=='\n3.5116758 = 6029760550880411648 [ org.apache.solr.ltr.ranking.RankSVMModel ] model applied to features, sum of:\n  0.0 = prod of:\n    0.0 = weight on feature [would be cool to have the name :)]\n    1.0 = ValueFeature [name=title value=1.0]\n  0.2 = prod of:\n    0.1 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=description value=2.0]\n  0.4 = prod of:\n    0.2 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=keywords value=2.0]\n  0.09 = prod of:\n    0.3 = weight on feature [would be cool to have the name :)]\n    0.3 = normalized using org.apache.solr.ltr.feature.norm.impl.MinMaxNormalizer [params {min=0.0, max=10.0}]\n      3.0 = ValueFeature [name=popularity value=3.0]\n  1.6 = prod of:\n    0.4 = weight on feature [would be cool to have the name :)]\n    4.0 = ValueFeature [name=text value=4.0]\n  0.6156155 = prod of:\n    0.1231231 = weight on feature [would be cool to have the name :)]\n    5.0 = ValueFeature [name=queryIntentPerson value=5.0]\n  0.60606056 = prod of:\n    0.12121211 = weight on feature [would be cool to have the name :)]\n    5.0 = ValueFeature [name=queryIntentCompany value=5.0]\n'}");
    assertJQ(
        "/query" + query.toQueryString(),
        "/debug/explain/9=='\n3.5116758 = 6029760550880411648 [ org.apache.solr.ltr.ranking.RankSVMModel ] model applied to features, sum of:\n  0.0 = prod of:\n    0.0 = weight on feature [would be cool to have the name :)]\n    1.0 = ValueFeature [name=title value=1.0]\n  0.2 = prod of:\n    0.1 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=description value=2.0]\n  0.4 = prod of:\n    0.2 = weight on feature [would be cool to have the name :)]\n    2.0 = ValueFeature [name=keywords value=2.0]\n  0.09 = prod of:\n    0.3 = weight on feature [would be cool to have the name :)]\n    0.3 = normalized using org.apache.solr.ltr.feature.norm.impl.MinMaxNormalizer [params {min=0.0, max=10.0}]\n      3.0 = ValueFeature [name=popularity value=3.0]\n  1.6 = prod of:\n    0.4 = weight on feature [would be cool to have the name :)]\n    4.0 = ValueFeature [name=text value=4.0]\n  0.6156155 = prod of:\n    0.1231231 = weight on feature [would be cool to have the name :)]\n    5.0 = ValueFeature [name=queryIntentPerson value=5.0]\n  0.60606056 = prod of:\n    0.12121211 = weight on feature [would be cool to have the name :)]\n    5.0 = ValueFeature [name=queryIntentCompany value=5.0]\n'}");
  }

  // @Test
  // public void checkfq() throws Exception {
  //
  // System.out.println("after: \n" + restTestHarness.query("/config/managed"));
  //
  // FunctionQueryFeature fq = new FunctionQueryFeature("log(popularity)");
  // assertJPut(featureEndpoint, gson.toJson(fq), "/responseHeader/status==0");
  // fq = new FunctionQueryFeature("tf_title_bloomberg",
  // "tf(title,'bloomberg')");
  // assertJPut(featureEndpoint, gson.toJson(fq), "/responseHeader/status==0");
  // // fq.(new NamedParams().add("fq", "log(popularity)"));
  //
  // ModelMetadata model = new ModelMetadata("sum3",
  // SumModel.class.getCanonicalName(), getFeatures(new String[] {
  // "log(popularity)", "tf_title_bloomberg", "t1", "t2" }));
  //
  // assertJPut(modelEndpoint, gson.toJson(model), "/responseHeader/status==0");
  //
  // SolrQuery query = new SolrQuery();
  // query.setQuery("title:bloomberg");
  // query.setParam("debugQuery", "on");
  // query.add("rows", "4");
  // query.add("rq", "{!ltr reRankDocs=4 model=sum3}");
  // query.add("fl", "*,score");
  // System.out.println(restTestHarness.query("/query" +
  // query.toQueryString()));
  // }
}
