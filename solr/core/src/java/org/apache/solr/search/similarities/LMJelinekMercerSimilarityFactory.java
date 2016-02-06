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

import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link LMJelinekMercerSimilarity}
 * <p>
 * Parameters:
 * <ul>
 *     <li>parameter lambda (float): smoothing parameter &lambda;. The default
 *         is <code>0.7</code></li>
 * </ul>
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link LMJelinekMercerSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @lucene.experimental
 */

public class LMJelinekMercerSimilarityFactory extends SimilarityFactory {
  private boolean discountOverlaps;
  private float lambda;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", true);
    lambda = params.getFloat("lambda", 0.7f);
  }

  @Override
  public Similarity getSimilarity() {
    LMJelinekMercerSimilarity sim = new LMJelinekMercerSimilarity(lambda);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
