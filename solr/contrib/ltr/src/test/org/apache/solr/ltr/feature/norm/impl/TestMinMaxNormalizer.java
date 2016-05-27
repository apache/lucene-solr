package org.apache.solr.ltr.feature.norm.impl;

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

import static org.junit.Assert.assertEquals;

import org.apache.solr.ltr.feature.norm.Normalizer;
import org.apache.solr.ltr.rest.ManagedModelStore;
import org.apache.solr.ltr.util.NamedParams;
import org.apache.solr.ltr.util.NormalizerException;
import org.junit.Test;

public class TestMinMaxNormalizer {

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxNoParams() throws NormalizerException {
    ManagedModelStore.getNormalizerInstance(
        MinMaxNormalizer.class.getCanonicalName(), new NamedParams());

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingMax() throws NormalizerException {

    ManagedModelStore.getNormalizerInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("min", 0f));

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingMin() throws NormalizerException {

    ManagedModelStore.getNormalizerInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("max", 0f));

  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingInvalidDelta() throws NormalizerException {
    ManagedModelStore.getNormalizerInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("max", 0f).add("min", 10f));
  }

  @Test(expected = NormalizerException.class)
  public void testInvalidMinMaxMissingInvalidDelta2()
      throws NormalizerException {

    ManagedModelStore.getNormalizerInstance(
        "org.apache.solr.ltr.feature.norm.impl.MinMaxNormalizer",
        new NamedParams().add("min", 10f).add("max", 10f));
    // min == max
  }

  @Test
  public void testNormalizer() throws NormalizerException {
    Normalizer n = ManagedModelStore.getNormalizerInstance(
        MinMaxNormalizer.class.getCanonicalName(),
        new NamedParams().add("min", 5f).add("max", 10f));

    float value = 8;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 100;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 150;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = -1;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
    value = 5;
    assertEquals((value - 5f) / (10f - 5f), n.normalize(value), 0.0001);
  }
}
