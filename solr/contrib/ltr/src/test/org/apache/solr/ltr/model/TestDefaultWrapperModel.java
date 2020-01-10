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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultWrapperModel extends TestRerankBase {

  final private static String featureStoreName = "test";
  private static File baseModelFile = null;

  static List<Feature> features = null;

  @Before
  public void setupBeforeClass() throws Exception {
    setuptest(false);
    assertU(adoc("id", "1", "title", "w1", "description", "w1", "popularity", "1"));
    assertU(adoc("id", "2", "title", "w2", "description", "w2", "popularity", "2"));
    assertU(adoc("id", "3", "title", "w3", "description", "w3", "popularity", "3"));
    assertU(adoc("id", "4", "title", "w4", "description", "w4", "popularity", "4"));
    assertU(adoc("id", "5", "title", "w5", "description", "w5", "popularity", "5"));
    assertU(commit());

    loadFeature("popularity", FieldValueFeature.class.getName(), "test", "{\"field\":\"popularity\"}");
    loadFeature("const", ValueFeature.class.getName(), "test", "{\"value\":5}");
    features = new ArrayList<>();
    features.add(getManagedFeatureStore().getFeatureStore("test").get("popularity"));
    features.add(getManagedFeatureStore().getFeatureStore("test").get("const"));

    final String baseModelJson = getModelInJson("linear", LinearModel.class.getName(),
        new String[] {"popularity", "const"},
        featureStoreName,
        "{\"weights\":{\"popularity\":-1.0, \"const\":1.0}}");
    // prepare the base model as a file resource
    baseModelFile = new File(tmpConfDir, "baseModel.json");
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(baseModelFile), StandardCharsets.UTF_8))) {
      writer.write(baseModelJson);
    }
    baseModelFile.deleteOnExit();
  }
  
  @After
  public void cleanup() throws Exception {
    features = null;
    baseModelFile = null;
    aftertest();
  }

  private static String getDefaultWrapperModelInJson(String wrapperModelName, String[] features, String params) {
    return getModelInJson(wrapperModelName, DefaultWrapperModel.class.getName(),
        features, featureStoreName, params);
  }

  @Test
  public void testLoadModelFromResource() throws Exception {
    String wrapperModelJson = getDefaultWrapperModelInJson("fileWrapper",
        new String[0],
        "{\"resource\":\"" + baseModelFile.getName() + "\"}");
    assertJPut(ManagedModelStore.REST_END_POINT, wrapperModelJson, "/responseHeader/status==0");

    final SolrQuery query = new SolrQuery();
    query.setQuery("{!func}pow(popularity,2)");
    query.add("rows", "3");
    query.add("fl", "*,score");
    query.add("rq", "{!ltr reRankDocs=3 model=fileWrapper}");
    assertJQ("/query" + query.toQueryString(),
             "/response/docs/[0]/id==\"3\"", "/response/docs/[0]/score==2.0",
             "/response/docs/[1]/id==\"4\"", "/response/docs/[1]/score==1.0",
             "/response/docs/[2]/id==\"5\"", "/response/docs/[2]/score==0.0");
  }

  @Test
  public void testLoadNestedWrapperModel() throws Exception {
    String otherWrapperModelJson = getDefaultWrapperModelInJson("otherNestedWrapper",
        new String[0],
        "{\"resource\":\"" + baseModelFile.getName() + "\"}");
    File otherWrapperModelFile = new File(tmpConfDir, "nestedWrapperModel.json");
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(otherWrapperModelFile), StandardCharsets.UTF_8))) {
      writer.write(otherWrapperModelJson);
    }

    String wrapperModelJson = getDefaultWrapperModelInJson("nestedWrapper",
        new String[0],
        "{\"resource\":\"" + otherWrapperModelFile.getName() + "\"}");
    assertJPut(ManagedModelStore.REST_END_POINT, wrapperModelJson, "/responseHeader/status==0");
    final SolrQuery query = new SolrQuery();
    query.setQuery("{!func}pow(popularity,2)");
    query.add("rows", "3");
    query.add("fl", "*,score");
    query.add("rq", "{!ltr reRankDocs=3 model=nestedWrapper}");
    assertJQ("/query" + query.toQueryString(),
             "/response/docs/[0]/id==\"3\"", "/response/docs/[0]/score==2.0",
             "/response/docs/[1]/id==\"4\"", "/response/docs/[1]/score==1.0",
             "/response/docs/[2]/id==\"5\"", "/response/docs/[2]/score==0.0");
  }

  @Test
  public void testLoadModelFromUnknownResource() throws Exception {
    String wrapperModelJson = getDefaultWrapperModelInJson("unknownWrapper",
        new String[0],
        "{\"resource\":\"unknownModel.json\"}");
    assertJPut(ManagedModelStore.REST_END_POINT, wrapperModelJson,
               "/responseHeader/status==400",
               "/error/msg==\"org.apache.solr.ltr.model.ModelException: "
               + "Failed to fetch the wrapper model from given resource (unknownModel.json)\"");
  }

  @Test
  public void testLoadModelWithEmptyParams() throws Exception {
    String wrapperModelJson = getDefaultWrapperModelInJson("invalidWrapper",
        new String[0],
        "{}");
    assertJPut(ManagedModelStore.REST_END_POINT, wrapperModelJson,
               "/responseHeader/status==400",
               "/error/msg==\"org.apache.solr.ltr.model.ModelException: "
               + "no resource configured for model invalidWrapper\"");
  }

}
