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

import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.junit.BeforeClass;

/**
 * Tests {@link LegacyBM25SimilarityFactory}
 */
public class TestLegacyBM25SimilarityFactory extends BaseSimilarityTestCase {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-basic.xml","schema-bm25.xml");
  }
  
  /** bm25 with default parameters */
  public void test() throws Exception {
    assertEquals(LegacyBM25Similarity.class, getSimilarity("legacy_text").getClass());
  }
  
  /** bm25 with parameters */
  public void testParameters() throws Exception {
    Similarity sim = getSimilarity("legacy_text_params");
    assertEquals(LegacyBM25Similarity.class, sim.getClass());
    LegacyBM25Similarity bm25 = (LegacyBM25Similarity) sim;
    assertEquals(1.2f, bm25.getK1(), 0.01f);
    assertEquals(0.76f, bm25.getB(), 0.01f);
  }
}
