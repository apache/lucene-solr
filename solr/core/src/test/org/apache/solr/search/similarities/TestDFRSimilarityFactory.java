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
package org.apache.solr.search.similarities;

import org.apache.lucene.search.similarities.AfterEffectB;
import org.apache.lucene.search.similarities.AfterEffectL;
import org.apache.lucene.search.similarities.BasicModelG;
import org.apache.lucene.search.similarities.BasicModelIF;
import org.apache.lucene.search.similarities.DFRSimilarity;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.BeforeClass;

/**
 * Tests {@link DFRSimilarityFactory}
 */
public class TestDFRSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-dfr.xml");
  }
  
  /** dfr with default parameters */
  public void test() throws Exception {
    Similarity sim = getSimilarity("text");
    assertEquals(DFRSimilarity.class, sim.getClass());
    DFRSimilarity dfr = (DFRSimilarity) sim;
    assertEquals(BasicModelIF.class, dfr.getBasicModel().getClass());
    assertEquals(AfterEffectB.class, dfr.getAfterEffect().getClass());
    assertEquals(NormalizationH2.class, dfr.getNormalization().getClass());
  }
  
  /** dfr with parametrized normalization */
  public void testParameters() throws Exception {
    Similarity sim = getSimilarity("text_params");
    assertEquals(DFRSimilarity.class, sim.getClass());
    DFRSimilarity dfr = (DFRSimilarity) sim;
    assertEquals(BasicModelIF.class, dfr.getBasicModel().getClass());
    assertEquals(AfterEffectB.class, dfr.getAfterEffect().getClass());
    assertEquals(NormalizationH3.class, dfr.getNormalization().getClass());
    NormalizationH3 norm = (NormalizationH3) dfr.getNormalization();
    assertEquals(900f, norm.getMu(), 0.01f);
  }
  
  /** LUCENE-3566 */
  public void testParameterC() throws Exception {
    Similarity sim = getSimilarity("text_paramc");
    assertEquals(DFRSimilarity.class, sim.getClass());
    DFRSimilarity dfr = (DFRSimilarity) sim;
    assertEquals(BasicModelG.class, dfr.getBasicModel().getClass());
    assertEquals(AfterEffectL.class, dfr.getAfterEffect().getClass());
    assertEquals(NormalizationH2.class, dfr.getNormalization().getClass());
    NormalizationH2 norm = (NormalizationH2) dfr.getNormalization();
    assertEquals(7f, norm.getC(), 0.01f);
  }
}
