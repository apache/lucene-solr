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
package org.apache.lucene.analysis.uima.ae;


import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;

/**
 * Testcase for {@link AEProviderFactory}
 */
public class AEProviderFactoryTest {

  @Test
  public void testCorrectCaching() throws Exception {
    AEProvider aeProvider = AEProviderFactory.getInstance().getAEProvider("/uima/TestAggregateSentenceAE.xml");
    assertTrue(aeProvider == AEProviderFactory.getInstance().getAEProvider("/uima/TestAggregateSentenceAE.xml"));
  }

  @Test
  public void testCorrectCachingWithParameters() throws Exception {
    AEProvider aeProvider = AEProviderFactory.getInstance().getAEProvider("prefix", "/uima/TestAggregateSentenceAE.xml",
        new HashMap<String, Object>());
    assertTrue(aeProvider == AEProviderFactory.getInstance().getAEProvider("prefix", "/uima/TestAggregateSentenceAE.xml",
        new HashMap<String, Object>()));
  }
}
