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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.StringContains.containsString;

public class TestMultipleAdditiveTreesModel extends TestRerankBase {

  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity","1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity","2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity","3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity","4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity","5"));
    assertU(commit());
  }

  @After
  public void after() throws Exception {
    aftertest();
  }


  @Test
  public void testMultipleAdditiveTrees() throws Exception {
    loadFeatures("multipleadditivetreesmodel_features.json");
    loadModels("multipleadditivetreesmodel.json");

    doTestMultipleAdditiveTreesScoringWithAndWithoutEfiFeatureMatches();
    doTestMultipleAdditiveTreesExplain();
  }

  private void doTestMultipleAdditiveTreesScoringWithAndWithoutEfiFeatureMatches() throws Exception {

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
    query.add("rq", "{!ltr reRankDocs=3 model=multipleadditivetreesmodel efi.user_query=dsjkafljjk}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==-120.0");

    // Matched user query since it was passed in
    query.remove("rq");
    query.add("rq", "{!ltr reRankDocs=3 model=multipleadditivetreesmodel efi.user_query=w3}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='3'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/score==30.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/score==-120.0");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/score==-120.0");
  }

  private void doTestMultipleAdditiveTreesExplain() throws Exception {

    final SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    query.add("fl", "*,score,[fv]");
    query.add("rows", "3");

    query.add("rq",
        "{!ltr reRankDocs=3 model=multipleadditivetreesmodel efi.user_query=w3}");

    // test out the explain feature, make sure it returns something
    query.setParam("debugQuery", "on");

    String qryResult = JQ("/query" + query.toQueryString());
    qryResult = qryResult.replaceAll("\n", " ");

    assertThat(qryResult, containsString("\"debug\":{"));
    qryResult = qryResult.substring(qryResult.indexOf("debug"));

    assertThat(qryResult, containsString("\"explain\":{"));
    qryResult = qryResult.substring(qryResult.indexOf("explain"));

    assertThat(qryResult, containsString("multipleadditivetreesmodel"));
    assertThat(qryResult, containsString(MultipleAdditiveTreesModel.class.getSimpleName()));

    assertThat(qryResult, containsString("-100.0 = tree 0"));
    assertThat(qryResult, containsString("50.0 = tree 0"));
    assertThat(qryResult, containsString("-20.0 = tree 1"));
    assertThat(qryResult, containsString("'matchedTitle':1.0 > 0.5"));
    assertThat(qryResult, containsString("'matchedTitle':0.0 <= 0.5"));

    assertThat(qryResult, containsString(" Go Right "));
    assertThat(qryResult, containsString(" Go Left "));
  }

  @Test
  public void multipleAdditiveTreesTestNoParams() throws Exception {
    final ModelException expectedException =
        new ModelException("no trees declared for model multipleadditivetreesmodel_no_params");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_params.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestEmptyParams() throws Exception {
    final ModelException expectedException =
        new ModelException("no trees declared for model multipleadditivetreesmodel_no_trees");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_trees.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestNoWeight() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree doesn't contain a weight");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_weight.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestTreesParamDoesNotContatinTree() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree doesn't contain a tree");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_tree.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestNoFeaturesSpecified() throws Exception {
    final ModelException expectedException =
        new ModelException("no features declared for model multipleadditivetreesmodel_no_features");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_features.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestNoRight() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree node is missing right");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_right.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestNoLeft() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree node is missing left");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_left.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestNoThreshold() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree node is missing threshold");
    Exception ex = expectThrows(Exception.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_threshold.json",
          "multipleadditivetreesmodel_features.json");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

  @Test
  public void multipleAdditiveTreesTestMissingTreeFeature() throws Exception {
    final ModelException expectedException =
        new ModelException("MultipleAdditiveTreesModel tree node is leaf with left=-100.0 and right=75.0");

    ModelException ex = expectThrows(ModelException.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_no_feature.json",
          "multipleadditivetreesmodel_features.json");
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void multipleAdditiveTreesTestMissingFeatureStore(){
    final ModelException expectedException =
        new ModelException("Missing or empty feature store: not_existent_store");

    ModelException ex = expectThrows(ModelException.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_notExistentStore.json",
          "multipleadditivetreesmodel_features.json");
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void multipleAdditiveTreesTestUnknownFeature(){
    final ModelException expectedException =
        new ModelException("Feature: notExist1 not found in store: _DEFAULT_");

    ModelException ex = expectThrows(ModelException.class, () -> {
      createModelFromFiles("multipleadditivetreesmodel_unknownFeature.json",
          "multipleadditivetreesmodel_features.json");
    });
    assertEquals(expectedException.toString(), ex.toString());
  }
 
}
