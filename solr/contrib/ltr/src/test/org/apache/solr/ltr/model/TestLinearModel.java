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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.store.FeatureStore;
import org.apache.solr.ltr.store.rest.ManagedModelStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLinearModel extends TestRerankBase {

  public static LTRScoringModel createLinearModel(String name, List<Feature> features,
      List<Normalizer> norms,
      String featureStoreName, List<Feature> allFeatures,
      Map<String,Object> params) throws ModelException {
    final LTRScoringModel model = LTRScoringModel.getInstance(solrResourceLoader,
        LinearModel.class.getName(),
        name,
        features, norms, featureStoreName, allFeatures, params);
    return model;
  }

  public static Map<String,Object> makeFeatureWeights(List<Feature> features) {
    final Map<String,Object> nameParams = new HashMap<String,Object>();
    final HashMap<String,Double> modelWeights = new HashMap<String,Double>();
    for (final Feature feat : features) {
      modelWeights.put(feat.getName(), 0.1);
    }
    nameParams.put("weights", modelWeights);
    return nameParams;
  }

  static ManagedModelStore store = null;
  static FeatureStore fstore = null;

  @Before
  public void setup() throws Exception {
    setuptest(true);
    // loadFeatures("features-store-test-model.json");
    store = getManagedModelStore();
    fstore = getManagedFeatureStore().getFeatureStore("test");

  }
  @After
  public void cleanup() throws Exception {
    store = null;
    fstore = null;
    aftertest();
  }
  
  @Test
  public void getInstanceTest() {
    final Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    Map<String,Object> params = new HashMap<String,Object>();
    final List<Feature> features = getFeatures(new String[] {
        "constant1", "constant5"});
    final List<Normalizer> norms =
        new ArrayList<Normalizer>(
            Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    params.put("weights", weights);
    final LTRScoringModel ltrScoringModel = createLinearModel("test1",
        features, norms, "test", fstore.getFeatures(),
        params);

    store.addModel(ltrScoringModel);
    final LTRScoringModel m = store.getModel("test1");
    assertEquals(ltrScoringModel, m);
  }

  @Test
  public void nullFeatureWeightsTest() {
    final ModelException expectedException =
        new ModelException("Model test2 doesn't contain any weights");

    final List<Feature> features = getFeatures(new String[]
        {"constant1", "constant5"});
    final List<Normalizer> norms =
        new ArrayList<>(Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    ModelException ex = expectThrows(ModelException.class, () -> {
      createLinearModel("test2",
          features, norms, "test", fstore.getFeatures(), null);
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void existingNameTest() {
    final SolrException expectedException =
        new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            ModelException.class.getName()+": model 'test3' already exists. Please use a different name");

    final List<Feature> features = getFeatures(new String[]
        {"constant1", "constant5"});
    final List<Normalizer> norms =
        new ArrayList<>(Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    final Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    Map<String,Object> params = new HashMap<>();
    params.put("weights", weights);
    SolrException ex = expectThrows(SolrException.class, () -> {
      final LTRScoringModel ltrScoringModel = createLinearModel("test3",
          features, norms, "test", fstore.getFeatures(), params);
      store.addModel(ltrScoringModel);
      final LTRScoringModel m = store.getModel("test3");
      assertEquals(ltrScoringModel, m);
      store.addModel(ltrScoringModel);
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void duplicateFeatureTest() {
    final ModelException expectedException =
        new ModelException("duplicated feature constant1 in model test4");
    final List<Feature> features = getFeatures(new String[]
        {"constant1", "constant1"});
    final List<Normalizer> norms =
        new ArrayList<>(Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    final Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5", 1d);

    Map<String,Object> params = new HashMap<>();
    params.put("weights", weights);
    ModelException ex = expectThrows(ModelException.class, () -> {
      final LTRScoringModel ltrScoringModel = createLinearModel("test4",
          features, norms, "test", fstore.getFeatures(), params);
      store.addModel(ltrScoringModel);
    });
    assertEquals(expectedException.toString(), ex.toString());

  }

  @Test
  public void missingFeatureWeightTest() {
    final ModelException expectedException =
        new ModelException("Model test5 lacks weight(s) for [constant5]");
    final List<Feature> features = getFeatures(new String[]
        {"constant1", "constant5"});
    final List<Normalizer> norms =
        new ArrayList<>(Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));

    final Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5missing", 1d);

    Map<String,Object> params = new HashMap<>();
    params.put("weights", weights);
    ModelException ex = expectThrows(ModelException.class, () -> {
      createLinearModel("test5",
          features, norms, "test", fstore.getFeatures(), params);
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void emptyFeaturesTest() {
    final ModelException expectedException =
        new ModelException("no features declared for model test6");
    final List<Feature> features = getFeatures(new String[] {});
    final List<Normalizer> norms =
        new ArrayList<>(Collections.nCopies(features.size(),IdentityNormalizer.INSTANCE));
    final Map<String,Object> weights = new HashMap<>();
    weights.put("constant1", 1d);
    weights.put("constant5missing", 1d);

    Map<String,Object> params = new HashMap<>();
    params.put("weights", weights);
    ModelException ex = expectThrows(ModelException.class, () -> {
      final LTRScoringModel ltrScoringModel = createLinearModel("test6",
          features, norms, "test", fstore.getFeatures(),
          params);
      store.addModel(ltrScoringModel);
    });
    assertEquals(expectedException.toString(), ex.toString());
  }

}
