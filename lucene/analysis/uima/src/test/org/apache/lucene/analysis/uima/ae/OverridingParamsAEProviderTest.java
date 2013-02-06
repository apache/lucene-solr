package org.apache.lucene.analysis.uima.ae;

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

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.resource.ResourceInitializationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * TestCase for {@link OverridingParamsAEProvider}
 */
public class OverridingParamsAEProviderTest {

  @Test
  public void testNullMapInitialization() throws Exception {
    try {
      AEProvider aeProvider = new OverridingParamsAEProvider("/uima/TestEntityAnnotatorAE.xml", null);
      aeProvider.getAE();
      fail("should fail due to null Map passed");
    } catch (ResourceInitializationException e) {
      // everything ok
    }
  }

  @Test
  public void testEmptyMapInitialization() throws Exception {
    AEProvider aeProvider = new OverridingParamsAEProvider("/uima/TestEntityAnnotatorAE.xml", new HashMap<String, Object>());
    AnalysisEngine analysisEngine = aeProvider.getAE();
    assertNotNull(analysisEngine);
  }

  @Test
  public void testOverridingParamsInitialization() throws Exception {
    Map<String, Object> runtimeParameters = new HashMap<String, Object>();
    runtimeParameters.put("ngramsize", "3");
    AEProvider aeProvider = new OverridingParamsAEProvider("/uima/AggregateSentenceAE.xml", runtimeParameters);
    AnalysisEngine analysisEngine = aeProvider.getAE();
    assertNotNull(analysisEngine);
    Object parameterValue = analysisEngine.getConfigParameterValue("ngramsize");
    assertNotNull(parameterValue);
    assertEquals(Integer.valueOf(3), Integer.valueOf(parameterValue.toString()));
  }
}
