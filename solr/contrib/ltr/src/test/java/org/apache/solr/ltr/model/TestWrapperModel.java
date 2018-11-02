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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.norm.IdentityNormalizer;
import org.apache.solr.ltr.norm.Normalizer;
import org.apache.solr.ltr.store.FeatureStore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestWrapperModel extends TestRerankBase {

  private static class StubWrapperModel extends WrapperModel {

    private StubWrapperModel(String name) {
      this(name, Collections.emptyList(), Collections.emptyList());
    }

    private StubWrapperModel(String name, List<Feature> features, List<Normalizer> norms) {
      super(name, features, norms, FeatureStore.DEFAULT_FEATURE_STORE_NAME, features, Collections.emptyMap());
    }

    @Override
    public Map<String, Object> fetchModelMap() throws ModelException {
      return null;
    }
  }

  private static LTRScoringModel createMockWrappedModel(String featureStoreName,
      List<Feature> features, List<Normalizer> norms) {
      LTRScoringModel wrappedModel = Mockito.mock(LTRScoringModel.class);
      Mockito.doReturn(featureStoreName).when(wrappedModel).getFeatureStoreName();
      Mockito.doReturn(features).when(wrappedModel).getFeatures();
      Mockito.doReturn(norms).when(wrappedModel).getNorms();
      return wrappedModel;
  }

  @Test
  public void testValidate() throws Exception {
    WrapperModel wrapperModel = new StubWrapperModel("testModel");
    try {
      wrapperModel.validate();
    } catch (ModelException e) {
      fail("Validation must succeed if no wrapped model is set");
    }

    // wrapper model with features
    WrapperModel wrapperModelWithFeatures = new StubWrapperModel("testModel",
        Collections.singletonList(new ValueFeature("val", Collections.emptyMap())), Collections.emptyList());
    try {
      wrapperModelWithFeatures.validate();
      fail("Validation must fail if features of the wrapper model isn't empty");
    } catch (ModelException e) {
      assertEquals("features must be empty for the wrapper model testModel", e.getMessage());
    }

    // wrapper model with norms
    WrapperModel wrapperModelWithNorms = new StubWrapperModel("testModel",
        Collections.emptyList(), Collections.singletonList(IdentityNormalizer.INSTANCE));
    try {
      wrapperModelWithNorms.validate();
      fail("Validation must fail if norms of the wrapper model isn't empty");
    } catch (ModelException e) {
      assertEquals("norms must be empty for the wrapper model testModel", e.getMessage());
    }

    assumeWorkingMockito();

    // update valid model
    {
      LTRScoringModel wrappedModel = 
          createMockWrappedModel(FeatureStore.DEFAULT_FEATURE_STORE_NAME,
              Arrays.asList(
                  new ValueFeature("v1", Collections.emptyMap()),
                  new ValueFeature("v2", Collections.emptyMap())),
              Arrays.asList(
                  IdentityNormalizer.INSTANCE,
                  IdentityNormalizer.INSTANCE)
              );
      try {
        wrapperModel.updateModel(wrappedModel);
      } catch (ModelException e) {
        fail("Validation must succeed if the wrapped model is valid");
      }
    }

    // update invalid model (feature store mismatch)
    {
      LTRScoringModel wrappedModel = 
          createMockWrappedModel("wrappedFeatureStore",
              Arrays.asList(
                  new ValueFeature("v1", Collections.emptyMap()),
                  new ValueFeature("v2", Collections.emptyMap())),
              Arrays.asList(
                  IdentityNormalizer.INSTANCE,
                  IdentityNormalizer.INSTANCE)
              );
      try {
        wrapperModel.updateModel(wrappedModel);
        fail("Validation must fail if wrapped model feature store differs from wrapper model feature store");
      } catch (ModelException e) {
        assertEquals("wrapper feature store name (_DEFAULT_) must match the wrapped feature store name (wrappedFeatureStore)", e.getMessage());
      }
    }

    // update invalid model (no features)
    {
      LTRScoringModel wrappedModel = 
          createMockWrappedModel(FeatureStore.DEFAULT_FEATURE_STORE_NAME,
              Collections.emptyList(),
              Arrays.asList(
                  IdentityNormalizer.INSTANCE,
                  IdentityNormalizer.INSTANCE)
              );
      try {
        wrapperModel.updateModel(wrappedModel);
        fail("Validation must fail if the wrapped model is invalid");
      } catch (ModelException e) {
        assertEquals("no features declared for model testModel", e.getMessage());
      }
    }

    // update invalid model (no norms)
    {
      LTRScoringModel wrappedModel = 
          createMockWrappedModel(FeatureStore.DEFAULT_FEATURE_STORE_NAME,
              Arrays.asList(
                  new ValueFeature("v1", Collections.emptyMap()),
                  new ValueFeature("v2", Collections.emptyMap())),
              Collections.emptyList()
              );
      try {
        wrapperModel.updateModel(wrappedModel);
        fail("Validation must fail if the wrapped model is invalid");
      } catch (ModelException e) {
        assertEquals("counted 2 features and 0 norms in model testModel", e.getMessage());
      }
    }
  }

  @Test
  public void testMethodOverridesAndDelegation() throws Exception {
    assumeWorkingMockito();
    final int overridableMethodCount = testOverwrittenMethods();
    final int methodCount = testDelegateMethods();
    assertEquals("method count mismatch", overridableMethodCount, methodCount);
  }

  private int testOverwrittenMethods() throws Exception {
    int overridableMethodCount = 0;
    for (final Method superClassMethod : LTRScoringModel.class.getDeclaredMethods()) {
      final int modifiers = superClassMethod.getModifiers();
      if (Modifier.isFinal(modifiers)) continue;
      if (Modifier.isStatic(modifiers)) continue;

      ++overridableMethodCount;
      if (Arrays.asList(
          "getName",  // the wrapper model's name is its own name i.e. _not_ the name of the wrapped model
          "getFeatureStoreName", // wrapper and wrapped model feature store should match, so need not override
          "getParams" // the wrapper model's params are its own params i.e. _not_ the params of the wrapped model
          ).contains(superClassMethod.getName())) {
        try {
          final Method subClassMethod = WrapperModel.class.getDeclaredMethod(
              superClassMethod.getName(),
              superClassMethod.getParameterTypes());
          fail(WrapperModel.class + " need not override\n'" + superClassMethod + "'"
               + " but it does override\n'" + subClassMethod + "'");
        } catch (NoSuchMethodException e) {
          // ok
        }
      } else {
        try {
          final Method subClassMethod = WrapperModel.class.getDeclaredMethod(
              superClassMethod.getName(),
              superClassMethod.getParameterTypes());
          assertEquals("getReturnType() difference",
              superClassMethod.getReturnType(),
              subClassMethod.getReturnType());
        } catch (NoSuchMethodException e) {
          fail(WrapperModel.class + " needs to override '" + superClassMethod + "'");
        }
      }
    }
    return overridableMethodCount;
  }

  private int testDelegateMethods() throws Exception {
    int methodCount = 0;
    WrapperModel wrapperModel = Mockito.spy(new StubWrapperModel("testModel"));

    // ignore validate in this test case
    Mockito.doNothing().when(wrapperModel).validate();
    ++methodCount;

    LTRScoringModel wrappedModel = Mockito.mock(LTRScoringModel.class);
    wrapperModel.updateModel(wrappedModel);

    // cannot be stubbed or verified
    ++methodCount; // toString
    ++methodCount; // hashCode
    ++methodCount; // equals

    // getFeatureStoreName : not delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getFeatureStoreName();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(0)).getFeatureStoreName();

    // getName : not delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getName();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(0)).getName();

    // getParams : not delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getParams();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(0)).getParams();

    // getNorms : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getNorms();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).getNorms();

    // getFeatures : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getFeatures();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).getFeatures();

    // getAllFeatures : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getAllFeatures();
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).getAllFeatures();

    // score : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.score(null);
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).score(null);

    // normalizeFeaturesInPlace : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.normalizeFeaturesInPlace(null);
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).normalizeFeaturesInPlace(null);

    // getNormalizerExplanation : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.getNormalizerExplanation(null, 0);
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).getNormalizerExplanation(null, 0);

    // explain : delegate
    Mockito.reset(wrappedModel);
    wrapperModel.explain(null, 0, 0.0f, null);
    ++methodCount;
    Mockito.verify(wrappedModel, Mockito.times(1)).explain(null, 0, 0.0f, null);

    return methodCount;
  }
}
