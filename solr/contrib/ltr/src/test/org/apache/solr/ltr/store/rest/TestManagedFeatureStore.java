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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.ltr.feature.Feature;
import org.apache.solr.ltr.feature.FeatureException;
import org.apache.solr.ltr.feature.OriginalScoreFeature;
import org.apache.solr.ltr.feature.ValueFeature;
import org.apache.solr.ltr.store.FeatureStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestManagedFeatureStore extends SolrTestCaseJ4 {

  private ManagedFeatureStore fstore = null;

  @Before
  public void setup() throws Exception {
    initCore("solrconfig-ltr.xml", "schema.xml");
    fstore = ManagedFeatureStore.getManagedFeatureStore(h.getCore());
  }
  @After
  public void cleanup() throws Exception {
    fstore = null;
    deleteCore();
  }
    

  private static Map<String,Object> createMap(String name, String className, Map<String,Object> params) {
    final Map<String,Object> map = new HashMap<String,Object>();
    map.put(ManagedFeatureStore.NAME_KEY, name);
    map.put(ManagedFeatureStore.CLASS_KEY, className);
    if (params != null) {
      map.put(ManagedFeatureStore.PARAMS_KEY, params);
    }
    return map;
  }

  @Test
  public void testDefaultFeatureStoreName()
  {
    assertEquals("_DEFAULT_", FeatureStore.DEFAULT_FEATURE_STORE_NAME);
    final FeatureStore expectedFeatureStore = fstore.getFeatureStore(FeatureStore.DEFAULT_FEATURE_STORE_NAME);
    final FeatureStore actualFeatureStore = fstore.getFeatureStore(null);
    assertEquals("getFeatureStore(null) should return the default feature store", expectedFeatureStore, actualFeatureStore);
  }

  @Test
  public void testFeatureStoreAdd() throws FeatureException
  {
    final FeatureStore fs = fstore.getFeatureStore("fstore-testFeature");
    for (int i = 0; i < 5; i++) {
      final String name = "c" + i;

      fstore.addFeature(createMap(name,
          OriginalScoreFeature.class.getName(), null),
          "fstore-testFeature");

      final Feature f = fs.get(name);
      assertNotNull(f);

    }
    assertEquals(5, fs.getFeatures().size());

  }

  @Test
  public void testFeatureStoreGet() throws FeatureException
  {
    final FeatureStore fs = fstore.getFeatureStore("fstore-testFeature2");
    for (int i = 0; i < 5; i++) {
      Map<String,Object> params = new HashMap<String,Object>();
      params.put("value", i);
      final String name = "c" + i;

      fstore.addFeature(createMap(name,
          ValueFeature.class.getName(), params),
          "fstore-testFeature2");

    }

    for (int i = 0; i < 5; i++) {
      final Feature f = fs.get("c" + i);
      assertEquals("c" + i, f.getName());
      assertTrue(f instanceof ValueFeature);
      final ValueFeature vf = (ValueFeature)f;
      assertEquals(i, vf.getValue());
    }
  }

  @Test
  public void testMissingFeatureReturnsNull() {
    final FeatureStore fs = fstore.getFeatureStore("fstore-testFeature3");
    for (int i = 0; i < 5; i++) {
      Map<String,Object> params = new HashMap<String,Object>();
      params.put("value", i);
      final String name = "testc" + (float) i;
      fstore.addFeature(createMap(name,
          ValueFeature.class.getName(), params),
          "fstore-testFeature3");

    }
    assertNull(fs.get("missing_feature_name"));
  }

  @Test
  public void getInstanceTest() throws FeatureException
  {
    fstore.addFeature(createMap("test",
        OriginalScoreFeature.class.getName(), null),
        "testFstore");
    final Feature feature = fstore.getFeatureStore("testFstore").get("test");
    assertNotNull(feature);
    assertEquals("test", feature.getName());
    assertEquals(OriginalScoreFeature.class.getName(), feature
        .getClass().getName());
  }

  @Test
  public void getInvalidInstanceTest() {
    final String nonExistingClassName = "org.apache.solr.ltr.feature.LOLFeature";
    final ClassNotFoundException expectedException =
        new ClassNotFoundException(nonExistingClassName);
    Exception ex = expectThrows(Exception.class, () -> {
      fstore.addFeature(createMap("test", nonExistingClassName, null), "testFstore2");
    });
    Throwable rootError = getRootCause(ex);
    assertEquals(expectedException.toString(), rootError.toString());
  }

}
