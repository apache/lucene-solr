package org.apache.solr.search.similarities;

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

import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.NormalizationH2;
import org.apache.lucene.search.similarities.NormalizationH3;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.BeforeClass;

/**
 * Tests {@link IBSimilarityFactory}
 */
public class TestIBSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-ib.xml");
  }
  
  /** spl/df/h2 with default parameters */
  public void test() throws Exception {
    Similarity sim = getSimilarity("text");
    assertEquals(IBSimilarity.class, sim.getClass());
    IBSimilarity ib = (IBSimilarity) sim;
    assertEquals(DistributionSPL.class, ib.getDistribution().getClass());
    assertEquals(LambdaDF.class, ib.getLambda().getClass());
    assertEquals(NormalizationH2.class, ib.getNormalization().getClass());
  }
  
  /** ll/ttf/h3 with parameterized normalization */
  public void testParameters() throws Exception {
    Similarity sim = getSimilarity("text_params");
    assertEquals(IBSimilarity.class, sim.getClass());
    IBSimilarity ib = (IBSimilarity) sim;
    assertEquals(DistributionLL.class, ib.getDistribution().getClass());
    assertEquals(LambdaTTF.class, ib.getLambda().getClass());
    assertEquals(NormalizationH3.class, ib.getNormalization().getClass());
    NormalizationH3 norm = (NormalizationH3) ib.getNormalization();
    assertEquals(900f, norm.getMu(), 0.01f);
  }
}
