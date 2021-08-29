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

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for BM25Similarity. This is the default similarity since 8.x.
 * If you need the exact same formula as in 6.x and 7.x you should instead look at
 * {@link LegacyBM25SimilarityFactory} noting that it is deprecated as of 8.10.0
 * and will be removed in 9.x.
 * <p>
 * Parameters:
 * <ul>
 *   <li>k1 (float): Controls non-linear term frequency normalization (saturation).
 *                   The default is <code>1.2</code>
 *   <li>b (float): Controls to what degree document length normalizes tf values.
 *                  The default is <code>0.75</code>
 * </ul>
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link BM25Similarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @lucene.experimental
 * @since 8.0.0
 */
public class BM25SimilarityFactory extends SimilarityFactory {
  private boolean discountOverlaps;
  private float k1;
  private float b;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", true);
    k1 = params.getFloat("k1", 1.2f);
    b = params.getFloat("b", 0.75f);
  }

  @Override
  public Similarity getSimilarity() {
    BM25Similarity sim = new BM25Similarity(k1, b);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
