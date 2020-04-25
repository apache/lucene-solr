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
package org.apache.solr.ltr.norm;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.core.SolrResourceLoader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMinMaxNormalizer {

  private final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  private Normalizer implTestMinMax(Map<String,Object> params,
      float expectedMin, float expectedMax) {
    final Normalizer n = Normalizer.getInstance(
        solrResourceLoader,
        MinMaxNormalizer.class.getName(),
        params);
    assertTrue(n instanceof MinMaxNormalizer);
    final MinMaxNormalizer mmn = (MinMaxNormalizer)n;
    assertEquals(mmn.getMin(), expectedMin, 0.0);
    assertEquals(mmn.getMax(), expectedMax, 0.0);
    assertEquals("{min="+expectedMin+", max="+expectedMax+"}", mmn.paramsToMap().toString());
    return n;
  }

  @Test
  public void testInvalidMinMaxNoParams() {
    implTestMinMax(new HashMap<String,Object>(),
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY);
  }

  @Test
  public void testInvalidMinMaxMissingMax() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("min", "0.0f");
    implTestMinMax(params,
        0.0f,
        Float.POSITIVE_INFINITY);
  }

  @Test
  public void testInvalidMinMaxMissingMin() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("max", "0.0f");
    implTestMinMax(params,
        Float.NEGATIVE_INFINITY,
        0.0f);
  }

  @Test
  public void testMinMaxNormalizerMinLargerThanMax() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("min", "10.0f");
    params.put("max", "0.0f");
    implTestMinMax(params,
        10.0f,
        0.0f);
  }

  @Test
  public void testMinMaxNormalizerMinEqualToMax() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("min", "10.0f");
    params.put("max", "10.0f");
    final NormalizerException expectedException =
        new NormalizerException("MinMax Normalizer delta must not be zero "
            + "| min = 10.0,max = 10.0,delta = 0.0");
    NormalizerException ex = SolrTestCaseJ4.expectThrows(NormalizerException.class,
        () -> implTestMinMax(params, 10.0f, 10.0f)
    );
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void testNormalizer() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("min", "5.0f");
    params.put("max", "10.0f");
    final Normalizer n =
        implTestMinMax(params,
            5.0f,
            10.0f);

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

  @Test
  public void testParamsToMap() {
    final MinMaxNormalizer n1 = new MinMaxNormalizer();
    n1.setMin(5.0f);
    n1.setMax(10.0f);

    final Map<String,Object> params = n1.paramsToMap();
    final MinMaxNormalizer n2 = (MinMaxNormalizer) Normalizer.getInstance(
        new SolrResourceLoader(),
        MinMaxNormalizer.class.getName(),
        params);
    assertEquals(n1.getMin(), n2.getMin(), 1e-6);
    assertEquals(n1.getMax(), n2.getMax(), 1e-6);
  }
}
