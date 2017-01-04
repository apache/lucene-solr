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

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.model.LinearModel;
import org.junit.Before;
import org.junit.Test;
import org.noggit.ObjectBuilder;

public class TestModelManagerPersistence extends TestRerankBase {

  @Before
  public void init() throws Exception {
    setupPersistenttest(true);
  }

  // executed first
  @Test
  public void testFeaturePersistence() throws Exception {

    loadFeature("feature", ValueFeature.class.getCanonicalName(), "test",
        "{\"value\":2}");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test",
        "/features/[0]/name=='feature'");
    loadFeature("feature1", ValueFeature.class.getCanonicalName(), "test1",
        "{\"value\":2}");
    loadFeature("feature2", ValueFeature.class.getCanonicalName(), "test",
        "{\"value\":2}");
    loadFeature("feature3", ValueFeature.class.getCanonicalName(), "test2",
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
    loadModel("test-model", LinearModel.class.getCanonicalName(),
        new String[] {"feature"}, "test", "{\"weights\":{\"feature\":1.0}}");
    loadModel("test-model2", LinearModel.class.getCanonicalName(),
        new String[] {"feature1"}, "test1", "{\"weights\":{\"feature1\":1.0}}");
    final String fstorecontent = FileUtils
        .readFileToString(fstorefile, "UTF-8");
    final String mstorecontent = FileUtils
        .readFileToString(mstorefile, "UTF-8");

    //check feature/model stores on deletion
    final ArrayList<Object> fStore = (ArrayList<Object>) ((Map<String,Object>)
        ObjectBuilder.fromJSON(fstorecontent)).get("managedList");
    for (int idx = 0;idx < fStore.size(); ++ idx) {
      String store = (String) ((Map<String,Object>)fStore.get(idx)).get("store");
      assertTrue(store.equals("test") || store.equals("test2") || store.equals("test1"));
    }

    final ArrayList<Object> mStore = (ArrayList<Object>) ((Map<String,Object>)
        ObjectBuilder.fromJSON(mstorecontent)).get("managedList");
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
    assertJQ(ManagedModelStore.REST_END_POINT + "/test-model2",
        "/models/[0]/name=='test-model'");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test2",
        "/features/==[]");
    assertJQ(ManagedModelStore.REST_END_POINT + "/test-model2",
        "/models/[0]/name=='test-model'");

    assertJDelete(ManagedModelStore.REST_END_POINT + "/*",
        "/responseHeader/status==0");
    assertJDelete(ManagedFeatureStore.REST_END_POINT + "/*",
        "/responseHeader/status==0");
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/==[]");
    restTestHarness.reload();
    assertJQ(ManagedFeatureStore.REST_END_POINT + "/test1",
        "/features/==[]");

  }

}
