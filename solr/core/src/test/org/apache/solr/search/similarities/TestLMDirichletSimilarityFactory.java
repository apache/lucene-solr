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

import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.junit.BeforeClass;

/**
 * Tests {@link LMDirichletSimilarityFactory}
 */
public class TestLMDirichletSimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-lmdirichlet.xml");
  }
  
  /** dirichlet with default parameters */
  public void test() throws Exception {
    assertEquals(LMDirichletSimilarity.class, getSimilarity("text").getClass());
  }
  
  /** dirichlet with parameters */
  public void testParameters() throws Exception {
    Similarity sim = getSimilarity("text_params");
    assertEquals(LMDirichletSimilarity.class, sim.getClass());
    LMDirichletSimilarity lm = (LMDirichletSimilarity) sim;
    assertEquals(1000f, lm.getMu(), 0.01f);
  }
}
