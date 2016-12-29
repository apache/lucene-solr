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
package org.apache.solr.ltr.feature;

import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.store.rest.ManagedFeatureStore;
import org.apache.solr.ltr.store.rest.TestManagedFeatureStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFeatureLtrScoringModel extends TestRerankBase {

  static ManagedFeatureStore store = null;

  @BeforeClass
  public static void setup() throws Exception {
    setuptest(true);
    store = getManagedFeatureStore();
  }

  @AfterClass
  public static void after() throws Exception {
    aftertest();
  }

  @Test
  public void getInstanceTest() throws FeatureException
  {
    store.addFeature(TestManagedFeatureStore.createMap("test",
        OriginalScoreFeature.class.getCanonicalName(), null),
        "testFstore");
    final Feature feature = store.getFeatureStore("testFstore").get("test");
    assertNotNull(feature);
    assertEquals("test", feature.getName());
    assertEquals(OriginalScoreFeature.class.getCanonicalName(), feature
        .getClass().getCanonicalName());
  }

  @Test
  public void getInvalidInstanceTest()
  {
    final String nonExistingClassName = "org.apache.solr.ltr.feature.LOLFeature";
    final ClassNotFoundException expectedException =
        new ClassNotFoundException(nonExistingClassName);
    try {
      store.addFeature(TestManagedFeatureStore.createMap("test",
          nonExistingClassName, null),
          "testFstore2");
      fail("getInvalidInstanceTest failed to throw exception: "+expectedException);
    } catch (Exception actualException) {
      Throwable rootError = getRootCause(actualException);
      assertEquals(expectedException.toString(), rootError.toString());
    }
  }

}
