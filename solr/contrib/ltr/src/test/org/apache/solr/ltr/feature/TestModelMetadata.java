package org.apache.solr.ltr.feature;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.ranking.RankSVMModel;
import org.apache.solr.ltr.rest.ManagedModelStore;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.ModelException;
import org.apache.solr.ltr.util.NamedParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestModelMetadata extends TestRerankBase {

  static ManagedModelStore store = null;
  static FeatureStore fstore = null;

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    // loadFeatures("features-store-test-model.json");
    store = getNewManagedModelStore();
    fstore = getNewManagedFeatureStore().getFeatureStore("test");

  }

  @Test
  public void getInstanceTest() throws FeatureException, ModelException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    ModelMetadata meta = new RankSVMModel("test1",
        RankSVMModel.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));

    store.addMetadataModel(meta);
    ModelMetadata m = store.getModel("test1");
    assertEquals(meta, m);
  }

  @Test(expected = ModelException.class)
  public void getInvalidTypeTest() throws ModelException, FeatureException {
    ModelMetadata meta = new RankSVMModel("test2",
        "org.apache.solr.ltr.model.LOLModel", getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(), null);
    store.addMetadataModel(meta);
    ModelMetadata m = store.getModel("test38290156821076");
  }

  @Test(expected = ModelException.class)
  public void getInvalidNameTest() throws ModelException, FeatureException {
    ModelMetadata meta = new RankSVMModel("!!!??????????",
        RankSVMModel.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(), null);
    store.addMetadataModel(meta);
    store.getModel("!!!??????????");
  }

  @Test(expected = ModelException.class)
  public void existingNameTest() throws ModelException, FeatureException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    ModelMetadata meta = new RankSVMModel("test3",
        RankSVMModel.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));
    store.addMetadataModel(meta);
    ModelMetadata m = store.getModel("test3");
    assertEquals(meta, m);
    store.addMetadataModel(meta);
  }

  @Test(expected = ModelException.class)
  public void duplicateFeatureTest() throws ModelException, FeatureException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    ModelMetadata meta = new RankSVMModel("test4",
        RankSVMModel.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant1"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));
    store.addMetadataModel(meta);

  }

  @Test(expected = ModelException.class)
  public void missingFeatureTest() throws ModelException, FeatureException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5missing", 1d);

    ModelMetadata meta = new RankSVMModel("test5",
        RankSVMModel.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant1"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));
    store.addMetadataModel(meta);

  }

  @Test(expected = ModelException.class)
  public void notExistingClassTest() throws ModelException, FeatureException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5missing", 1d);

    ModelMetadata meta = new RankSVMModel("test6",
        "com.hello.im.a.bad.model.class", getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));
    store.addMetadataModel(meta);

  }

  private class WrongClass {};

  @Test(expected = ModelException.class)
  public void badModelClassTest() throws ModelException, FeatureException {
    Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5missing", 1d);

    ModelMetadata meta = new RankSVMModel("test7",
        WrongClass.class.getCanonicalName(), getFeatures(new String[] {
            "constant1", "constant5"}), "test", fstore.getFeatures(),
        new NamedParams().add("weights", weights));
    store.addMetadataModel(meta);

  }

}
