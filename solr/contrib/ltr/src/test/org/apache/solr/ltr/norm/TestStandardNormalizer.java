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

public class TestStandardNormalizer {

  private final SolrResourceLoader solrResourceLoader = new SolrResourceLoader();

  private Normalizer implTestStandard(Map<String,Object> params,
      float expectedAvg, float expectedStd) {
    final Normalizer n = Normalizer.getInstance(
        solrResourceLoader,
        StandardNormalizer.class.getName(),
        params);
    assertTrue(n instanceof StandardNormalizer);
    final StandardNormalizer sn = (StandardNormalizer)n;
    assertEquals(sn.getAvg(), expectedAvg, 0.0);
    assertEquals(sn.getStd(), expectedStd, 0.0);
    assertEquals("{avg="+expectedAvg+", std="+expectedStd+"}", sn.paramsToMap().toString());
    return n;
  }

  @Test
  public void testNormalizerNoParams() {
    implTestStandard(new HashMap<String,Object>(),
        0.0f,
        1.0f);
  }

  @Test
  public void testInvalidSTD() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("std", "0f");
    final NormalizerException expectedException =
        new NormalizerException("Standard Normalizer standard deviation must be positive "
            + "| avg = 0.0,std = 0.0");
    NormalizerException ex = SolrTestCaseJ4.expectThrows(NormalizerException.class,
        () -> implTestStandard(params, 0.0f, 0.0f)
    );
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void testInvalidSTD2() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("std", "-1f");
    final NormalizerException expectedException =
        new NormalizerException("Standard Normalizer standard deviation must be positive "
            + "| avg = 0.0,std = -1.0");

    NormalizerException ex = SolrTestCaseJ4.expectThrows(NormalizerException.class,
        () -> implTestStandard(params, 0.0f, -1f)
    );
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void testInvalidSTD3() {
    final Map<String,Object> params = new HashMap<String,Object>();
    params.put("avg", "1f");
    params.put("std", "0f");
    final NormalizerException expectedException =
        new NormalizerException("Standard Normalizer standard deviation must be positive "
            + "| avg = 1.0,std = 0.0");

    NormalizerException ex = SolrTestCaseJ4.expectThrows(NormalizerException.class,
        () -> implTestStandard(params, 1f, 0f)
    );
    assertEquals(expectedException.toString(), ex.toString());
  }

  @Test
  public void testNormalizer() {
    Map<String,Object> params = new HashMap<String,Object>();
    params.put("avg", "0f");
    params.put("std", "1f");
    final Normalizer identity =
        implTestStandard(params,
            0f,
            1f);

    float value = 8;
    assertEquals(value, identity.normalize(value), 0.0001);
    value = 150;
    assertEquals(value, identity.normalize(value), 0.0001);
    params = new HashMap<String,Object>();
    params.put("avg", "10f");
    params.put("std", "1.5f");
    final Normalizer norm = Normalizer.getInstance(
        solrResourceLoader,
        StandardNormalizer.class.getName(),
        params);

    for (final float v : new float[] {10f, 20f, 25f, 30f, 31f, 40f, 42f, 100f,
        10000000f}) {
      assertEquals((v - 10f) / (1.5f), norm.normalize(v), 0.0001);
    }
  }

  @Test
  public void testParamsToMap() {
    final StandardNormalizer n1 = new StandardNormalizer();
    n1.setAvg(2.0f);
    n1.setStd(3.0f);

    final Map<String, Object> params = n1.paramsToMap();
    final StandardNormalizer n2 = (StandardNormalizer) Normalizer.getInstance(
        new SolrResourceLoader(),
        StandardNormalizer.class.getName(),
        params);
    assertEquals(n1.getAvg(), n2.getAvg(), 1e-6);
    assertEquals(n1.getStd(), n2.getStd(), 1e-6);
  }
}
