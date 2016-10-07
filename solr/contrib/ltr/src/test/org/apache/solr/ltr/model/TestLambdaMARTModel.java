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
package org.apache.solr.ltr.model;

//import static org.junit.internal.matchers.StringContains.containsString;

import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.model.ModelException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

@SuppressCodecs({"Lucene3x", "Lucene41", "Lucene40", "Appending"})
public class TestLambdaMARTModel extends TestRerankBase {


  @BeforeClass
  public static void before() throws Exception {
    setuptest("solrconfig-ltr.xml", "schema-ltr.xml");

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity","1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity","2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity","3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity","4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity","5"));
    assertU(commit());

    loadFeatures("lambdamart_features.json"); // currently needed to force
    // scoring on all docs
    loadModels("lambdamartmodel.json");
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  
  @Test
  public void testLambdaMartScoringWithAndWithoutEfiFeatureMatches() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("rows", "3");
    query.add("fl", "*,score");

    // Regular scores
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==1.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==1.0");

    // No match scores since user_query not passed in to external feature info
    // and feature depended on it.
    query.add("rq", "{!ltr reRankDocs=3 model=lambdamartmodel efi.user_query=dsjkafljjk}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==-120.0");

    // Matched user query since it was passed in
    query.remove("rq");
    query.add("rq", "{!ltr reRankDocs=3 model=lambdamartmodel efi.user_query=w3}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==-20.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==-120.0");
  }

  @Ignore
  @Test
  public void lambdaMartTestExplain() throws Exception {
    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,[fv]");
    query.add("rows", "3");

    query.add("rq",
        "{!ltr reRankDocs=3 model=lambdamartmodel efi.user_query=w3}");

    // test out the explain feature, make sure it returns something
    query.setParam("debugQuery", "on");
    String qryResult = JQ("/query" + query.toQueryString());

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
    // containsString(LambdaMARTModel.class.getCanonicalName()));
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
  }

  @Test
  public void lambdaMartTestNoParams() throws Exception {
    final ModelException expectedException = 
        new ModelException("no trees declared for model lambdamartmodel_no_params");
    try {
        createModelFromFiles("lambdamartmodel_no_params.json",
              "lambdamart_features.json");
        fail("lambdaMartTestNoParams failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }

  }

  @Test
  public void lambdaMartTestEmptyParams() throws Exception {
    final ModelException expectedException = 
        new ModelException("no trees declared for model lambdamartmodel_no_trees");
    try {
        createModelFromFiles("lambdamartmodel_no_trees.json",
            "lambdamart_features.json");
        fail("lambdaMartTestEmptyParams failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestNoWeight() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree doesn't contain a weight");
    try {
        createModelFromFiles("lambdamartmodel_no_weight.json",
            "lambdamart_features.json");
        fail("lambdaMartTestNoWeight failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestTreesParamDoesNotContatinTree() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree doesn't contain a tree");
    try {
        createModelFromFiles("lambdamartmodel_no_tree.json",
            "lambdamart_features.json");
        fail("lambdaMartTestTreesParamDoesNotContatinTree failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestNoFeaturesSpecified() throws Exception {
    final ModelException expectedException = 
        new ModelException("no features declared for model lambdamartmodel_no_features");
    try {
        createModelFromFiles("lambdamartmodel_no_features.json",
            "lambdamart_features.json");
        fail("lambdaMartTestNoFeaturesSpecified failed to throw exception: "+expectedException);
    } catch (ModelException actualException) {
      assertEquals(expectedException.toString(), actualException.toString());
    }
  }

  @Test
  public void lambdaMartTestNoRight() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree node is missing right");
    try {
        createModelFromFiles("lambdamartmodel_no_right.json",
            "lambdamart_features.json");
        fail("lambdaMartTestNoRight failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestNoLeft() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree node is missing left");
    try {
        createModelFromFiles("lambdamartmodel_no_left.json",
            "lambdamart_features.json");
        fail("lambdaMartTestNoLeft failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestNoThreshold() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree node is missing threshold");
    try {
        createModelFromFiles("lambdamartmodel_no_threshold.json",
            "lambdamart_features.json");
        fail("lambdaMartTestNoThreshold failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

  @Test
  public void lambdaMartTestMissingTreeFeature() throws Exception {
    final ModelException expectedException = 
        new ModelException("LambdaMARTModel tree node is leaf with left=-100.0 and right=75.0");
    try {
        createModelFromFiles("lambdamartmodel_no_feature.json",
              "lambdamart_features.json");
        fail("lambdaMartTestMissingTreeFeature failed to throw exception: "+expectedException);
    } catch (ModelException actualException) {
      assertEquals(expectedException.toString(), actualException.toString());
    }
  }
}
