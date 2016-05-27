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

import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.impl.FieldValueFeature;
import org.apache.solr.ltr.feature.impl.OriginalScoreFeature;
import org.apache.solr.ltr.feature.impl.ValueFeature;
import org.apache.solr.ltr.ranking.Feature;
import org.apache.solr.ltr.rest.ManagedFeatureStore;
import org.apache.solr.ltr.util.FeatureException;
import org.apache.solr.ltr.util.InvalidFeatureNameException;
import org.apache.solr.ltr.util.NamedParams;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFeatureStore extends TestRerankBase {

  static ManagedFeatureStore fstore = null;

  @BeforeClass
  public static void setup() throws Exception {
    setuptest();
    fstore = getNewManagedFeatureStore();
  }

  @Test
  public void testFeatureStoreAdd() throws InvalidFeatureNameException,
      FeatureException {
    FeatureStore fs = fstore.getFeatureStore("fstore-testFeature");
    for (int i = 0; i < 5; i++) {
      fstore.addFeature("c" + i, OriginalScoreFeature.class.getCanonicalName(),
          "fstore-testFeature", NamedParams.EMPTY);

      assertTrue(fs.containsFeature("c" + i));

    }
    assertEquals(5, fs.size());

  }

  @Test
  public void testFeatureStoreGet() throws FeatureException,
      InvalidFeatureNameException {
    FeatureStore fs = fstore.getFeatureStore("fstore-testFeature2");
    for (int i = 0; i < 5; i++) {

      fstore.addFeature("c" + (float) i, ValueFeature.class.getCanonicalName(),
          "fstore-testFeature2", new NamedParams().add("value", i));

    }

    for (float i = 0; i < 5; i++) {
      Feature f = fs.get("c" + (float) i);
      assertEquals("c" + i, f.getName());
      assertEquals(i, f.getParams().getFloat("value"), 0.0001);
    }
  }

  @Test(expected = FeatureException.class)
  public void testMissingFeature() throws InvalidFeatureNameException,
      FeatureException {
    FeatureStore fs = fstore.getFeatureStore("fstore-testFeature3");
    for (int i = 0; i < 5; i++) {
      fstore.addFeature("testc" + (float) i,
          ValueFeature.class.getCanonicalName(), "fstore-testFeature3",
          new NamedParams().add("value", i));

    }
    fs.get("missing_feature_name");
  }

  @Test(expected = FeatureException.class)
  public void testMissingFeature2() throws InvalidFeatureNameException,
      FeatureException {
    FeatureStore fs = fstore.getFeatureStore("fstore-testFeature4");
    for (int i = 0; i < 5; i++) {
      fstore.addFeature("testc" + (float) i,
          ValueFeature.class.getCanonicalName(), "fstore-testFeature4",
          new NamedParams().add("value", i));

    }
    fstore.addFeature("invalidparam",
        FieldValueFeature.class.getCanonicalName(), "fstore-testFeature4",
        NamedParams.EMPTY);
  }

}
