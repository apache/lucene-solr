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
package org.apache.solr.ltr.store.rest;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FieldValueFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.DefaultWrapperModel;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.store.FeatureStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestModelManagerPersistence extends TestRerankBase {

  @Before
  public void init() throws Exception {
    setupPersistenttest(true);
  }
  @After
  public void cleanup() throws Exception {
    aftertest();
  }

  // executed first
  @SuppressWarnings("unchecked")
  @Test
  public void testFeaturePersistence() throws Exception {

    loadFeature("feature", ValueFeature.class.getName(), "test",
        "{\"value\":2}");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    loadFeature("feature1", ValueFeature.class.getName(), "test1",
        "{\"value\":2}");
    loadFeature("feature2", ValueFeature.class.getName(), "test",
        "{\"value\":2}");
    loadFeature("feature3", ValueFeature.class.getName(), "test2",
        "{\"value\":2}");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[1]/name=='feature2'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/[0]/name=='feature1'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/features/[0]/name=='feature3'");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[1]/name=='feature2'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/[0]/name=='feature1'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/features/[0]/name=='feature3'");
    loadModel("test-model", LinearModel.class.getName(),
        new String[] {"feature"}, "test", "{\"weights\":{\"feature\":1.0}}");
    loadModel("test-model2", LinearModel.class.getName(),
        new String[] {"feature1"}, "test1", "{\"weights\":{\"feature1\":1.0}}");
    final String fstorecontent = FileUtils
        .readFileToString(fstorefile, "UTF-8");
    final String mstorecontent = FileUtils
        .readFileToString(mstorefile, "UTF-8");

    //check feature/model stores on deletion
    final ArrayList<Object> fStore = (ArrayList<Object>) ((Map<String,Object>)
        Utils.fromJSONString(fstorecontent)).get("managedList");
    for (int idx = 0;idx < fStore.size(); ++ idx) {
      String store = (String) ((Map<String,Object>)fStore.get(idx)).get("store");
      assertTrue(store.equals("test") || store.equals("test2") || store.equals("test1"));
    }

    final ArrayList<Object> mStore = (ArrayList<Object>) ((Map<String,Object>)
        Utils.fromJSONString(mstorecontent)).get("managedList");
    for (int idx = 0;idx < mStore.size(); ++ idx) {
      String store = (String) ((Map<String,Object>)mStore.get(idx)).get("store");
      assertTrue(store.equals("test") || store.equals("test1"));
    }

    assertJDelete(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/responseHeader/status==0");
    assertJDelete(ManagedModelStore.REST_END_POINT + "/test-model2",
        "/responseHeader/status==0");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/features/==[]");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='test-model'");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/features/==[]");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='test-model'");

    assertJDelete(ManagedModelStore.REST_END_POINT + "/test-model",
        "/responseHeader/status==0");
    assertJDelete(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/responseHeader/status==0");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/==[]");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/==[]");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
  }

  @Test
  public void testFilePersistence() throws Exception {
    // check whether models and features are empty
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/==[]");

    // load models and features from files
    loadFeatures("features-linear.json");
    loadModels("linear-model.json");

    // check loaded models and features
    final String modelName = "6029760550880411648";
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='"+modelName+"'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[0]/name=='title'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[1]/name=='description'");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='"+modelName+"'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[0]/name=='title'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[1]/name=='description'");

    // check persistence after restart
    jetty.stop();
    jetty.start();
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='"+modelName+"'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[0]/name=='title'");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/[1]/name=='description'");

    // delete loaded models and features
    restTestHarness.delete(ManagedModelStore.REST_END_POINT + "/"+modelName);
    restTestHarness.delete(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME);
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/==[]");

    // check persistence after reload
    restTestHarness.reload();
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/==[]");

    // check persistence after restart
    jetty.stop();
    jetty.start();
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FeatureStore.DEFAULT_FEATURE_STORE_NAME,
        "/features/==[]");
  }

  private static void doWrapperModelPersistenceChecks(String modelName,
      String featureStoreName, String baseModelFileName) throws Exception {
    // note that the wrapper and the wrapped model always have the same name
    assertJQ(ManagedModelStore.REST_END_POINT,
        // the wrapped model shouldn't be registered
        "!/models/[1]/name=='"+modelName+"'",
        // but the wrapper model should be registered
        "/models/[0]/name=='"+modelName+"'",
        "/models/[0]/class=='" + DefaultWrapperModel.class.getName() + "'",
        "/models/[0]/store=='" + featureStoreName + "'",
        // the wrapper model shouldn't contain the definitions of the wrapped model
        "/models/[0]/features/==[]",
        // but only its own parameters
        "/models/[0]/params=={resource:'"+baseModelFileName+"'}");
  }

  @Test
  public void testWrapperModelPersistence() throws Exception {
    final String modelName = "linear";
    final String FS_NAME = "testWrapper";

    // check whether models and features are empty
    assertJQ(ManagedModelStore.REST_END_POINT,
             "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FS_NAME,
             "/features/==[]");

    // setup features
    loadFeature("popularity", FieldValueFeature.class.getName(), FS_NAME, "{\"field\":\"popularity\"}");
    loadFeature("const", ValueFeature.class.getName(), FS_NAME, "{\"value\":5}");

    // setup base model
    String baseModelJson = getModelInJson(modelName, LinearModel.class.getName(),
                                          new String[] {"popularity", "const"}, FS_NAME,
                                          "{\"weights\":{\"popularity\":-1.0, \"const\":1.0}}");
    File baseModelFile = new File(tmpConfDir, "baseModelForPersistence.json");
    try (BufferedWriter writer = new BufferedWriter(
        new OutputStreamWriter(new FileOutputStream(baseModelFile), StandardCharsets.UTF_8))) {
      writer.write(baseModelJson);
    }
    baseModelFile.deleteOnExit();

    // setup wrapper model
    String wrapperModelJson = getModelInJson(modelName, DefaultWrapperModel.class.getName(),
                                             new String[0], FS_NAME,
                                             "{\"resource\":\"" + baseModelFile.getName() + "\"}");
    assertJPut(ManagedModelStore.REST_END_POINT, wrapperModelJson, "/responseHeader/status==0");
    doWrapperModelPersistenceChecks(modelName, FS_NAME, baseModelFile.getName());

    // check persistence after reload
    restTestHarness.reload();
    doWrapperModelPersistenceChecks(modelName, FS_NAME, baseModelFile.getName());

    // check persistence after restart
    jetty.stop();
    jetty.start();
    doWrapperModelPersistenceChecks(modelName, FS_NAME, baseModelFile.getName());

    // delete test settings
    restTestHarness.delete(ManagedModelStore.REST_END_POINT + "/" + modelName);
    restTestHarness.delete(ManagedFeatureStore.REST_END_POINT + "/" + FS_NAME);
    assertJQ(ManagedModelStore.REST_END_POINT,
             "/models/==[]");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/" + FS_NAME,
             "/features/==[]");

    // NOTE: we don't test the persistence of the deletion here because it's tested in testFilePersistence
  }

  public static class DummyCustomFeature extends ValueFeature {
    public DummyCustomFeature(String name, Map<String,Object> params) {
      super(name, params);
    }
  }

  public static class DummyCustomModel extends LinearModel {
    public DummyCustomModel(String name, List<Feature> features, List<Normalizer> norms, String featureStoreName,
        List<Feature> allFeatures, Map<String,Object> params) {
      super(name, features, norms, featureStoreName, allFeatures, params);
    }
  }

  @Test
  public void testInnerCustomClassesPersistence() throws Exception {

    final String featureStoreName = "test42";

    final String featureName = "feature42";
    final String featureClassName;
    if (random().nextBoolean()) {
      featureClassName = ValueFeature.class.getName();
    } else {
      featureClassName = DummyCustomFeature.class.getName();
    }

    loadFeature(featureName, featureClassName, "test42",
        "{\"value\":"+random().nextInt(100)+"}");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+featureStoreName,
        "/features/[0]/name=='"+featureName+"'");

    final String modelName = "model42";
    final String modelClassName;
    if (random().nextBoolean()) {
      modelClassName = LinearModel.class.getName();
    } else {
      modelClassName = DummyCustomModel.class.getName();
    }

    loadModel(modelName, modelClassName,
        new String[] { featureName }, featureStoreName,
        "{\"weights\":{\""+featureName+"\":1.0}}");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='"+modelName+"'");

    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+featureStoreName,
        "/features/[0]/name=='"+featureName+"'");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/[0]/name=='"+modelName+"'");

    assertJDelete(ManagedModelStore.REST_END_POINT + "/"+modelName,
        "/responseHeader/status==0");
    assertJQ(ManagedModelStore.REST_END_POINT,
        "/models/==[]");

    assertJDelete(ManagedFeatureStore.REST_END_POINT + "/"+featureStoreName,
        "/responseHeader/status==0");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/"+featureStoreName,
        "/features/==[]");
  }

}
