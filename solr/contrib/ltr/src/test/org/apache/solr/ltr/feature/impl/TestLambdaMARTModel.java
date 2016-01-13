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

//import static org.junit.internal.matchers.StringContains.containsString;

import java.lang.invoke.MethodHandles;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.ranking.LTRComponent;
import org.apache.solr.ltr.util.ModelException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestLambdaMARTModel extends TestRerankBase {

  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");

    h.update(adoc("id", "1", "title", "w1", "description", "w1", "popularity",
        "1"));
    h.update(adoc("id", "2", "title", "w2", "description", "w2", "popularity",
        "2"));
    h.update(adoc("id", "3", "title", "w3", "description", "w3", "popularity",
        "3"));
    h.update(adoc("id", "4", "title", "w4", "description", "w4", "popularity",
        "4"));
    h.update(adoc("id", "5", "title", "w5", "description", "w5", "popularity",
        "5"));
    h.update(commit());

    loadFeatures("lambdamart_features.json"); // currently needed to force
                                              // scoring on all docs
    loadModels("lambdamart_model.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Ignore
  @Test
  public void lambdaMartTest1() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add(LTRComponent.LTRParams.FV, "true");
    query.add("fl", "*,score");

    // Regular scores
    // System.out.println(restTestHarness.query(request)
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==1.0");

    // No match scores since user_query not passed in to external feature info
    // and feature depended on it.
    query.add("fl", "[fv]");
    query.add("rq", "{!ltr reRankDocs=3 model=lambdamartmodel}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[0]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='2'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='3'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[2]/score==-120.0");

    // Matched user query since it was passed in
    query.remove("rq");
    query.add("rq",
        "{!ltr reRankDocs=3 model=lambdamartmodel efi.user_query=w3}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==30.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='1'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='2'");
    assertJQ("/query" + query.toQueryString(),
        "/response/docs/[2]/score==-120.0");

    System.out.println(restTestHarness.query("/query" + query.toQueryString()));

  }

  @Ignore
  @Test
  public void lambdaMartTestExplain() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,[fv]");
    query.add("rows", "3");
    query.add(LTRComponent.LTRParams.FV, "true");

    query.add("rq",
        "{!ltr reRankDocs=3 model=lambdamartmodel efi.user_query=w3}");

    // test out the explain feature, make sure it returns something
    query.setParam("debugQuery", "on");
    String qryResult = JQ("/query" + query.toQueryString());

    System.out.println(qryResult);

    qryResult = qryResult.replaceAll("\n", " ");
    // FIXME containsString doesn't exist.
    // assertThat(qryResult, containsString("\"debug\":{"));
    // qryResult = qryResult.substring(qryResult.indexOf("debug"));
    //
    // assertThat(qryResult, containsString("\"explain\":{"));
    // qryResult = qryResult.substring(qryResult.indexOf("explain"));
    //
    // assertThat(qryResult, containsString("lambdamartmodel"));
    // assertThat(qryResult,
    // containsString("org.apache.solr.ltr.ranking.LambdaMARTModel"));
    //
    // assertThat(qryResult, containsString("-100.0 = tree 0"));
    // assertThat(qryResult, containsString("50.0 = tree 0"));
    // assertThat(qryResult, containsString("-20.0 = tree 1"));
    // assertThat(qryResult, containsString("'matchedTitle':1.0 > 0.5"));
    // assertThat(qryResult, containsString("'matchedTitle':0.0 <= 0.5"));
    //
    // assertThat(qryResult, containsString(" Go Right "));
    // assertThat(qryResult, containsString(" Go Left "));
    // assertThat(qryResult,
    // containsString("'this_feature_doesnt_exist' does not exist in FV"));

    System.out.println(restTestHarness.query("/query" + query.toQueryString()));
  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoParams() throws Exception {
    createModelFromFiles("lambdamart_model_no_params.json",
        "lambdamart_features.json");

  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoTrees() throws Exception {
    createModelFromFiles("lambdamart_model_no_trees.json",
        "lambdamart_features.json");
  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoWeight() throws Exception {
    createModelFromFiles("lambdamart_model_no_weight.json",
        "lambdamart_features.json");
  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoTree() throws Exception {
    createModelFromFiles("lambdamart_model_no_tree.json",
        "lambdamart_features.json");
  }

  @Test(expected = SolrException.class)
  public void lambdaMartTestNoFeatures() throws Exception {
    createModelFromFiles("lambdamart_model_no_features.json",
        "lambdamart_features.json");
  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoRight() throws Exception {
    createModelFromFiles("lambdamart_model_no_right.json",
        "lambdamart_features.json");

  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoLeft() throws Exception {
    createModelFromFiles("lambdamart_model_no_left.json",
        "lambdamart_features.json");
  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoThreshold() throws Exception {
    createModelFromFiles("lambdamart_model_no_threshold.json",
        "lambdamart_features.json");

  }

  @Test(expected = ModelException.class)
  public void lambdaMartTestNoFeature() throws Exception {
    createModelFromFiles("lambdamart_model_no_feature.json",
        "lambdamart_features.json");
  }
}
