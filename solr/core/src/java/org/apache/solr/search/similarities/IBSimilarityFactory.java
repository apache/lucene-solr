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

import org.apache.lucene.search.similarities.Distribution;
import org.apache.lucene.search.similarities.DistributionLL;
import org.apache.lucene.search.similarities.DistributionSPL;
import org.apache.lucene.search.similarities.IBSimilarity;
import org.apache.lucene.search.similarities.Lambda;
import org.apache.lucene.search.similarities.LambdaDF;
import org.apache.lucene.search.similarities.LambdaTTF;
import org.apache.lucene.search.similarities.Normalization;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link IBSimilarity}
 * <p>
 * You must specify the implementations for all three components of the
 * Information-Based model (strings).
 * <ol>
 *     <li>{@link Distribution distribution}: Probabilistic distribution used to
 *         model term occurrence
 *         <ul>
 *             <li>{@link DistributionLL LL}: Log-logistic</li>
 *             <li>{@link DistributionLL SPL}: Smoothed power-law</li>
 *         </ul>
 *     </li>
 *     <li>{@link Lambda lambda}: &lambda;<sub>w</sub> parameter of the
 *         probability distribution
 *         <ul>
 *             <li>{@link LambdaDF DF}: <code>N<sub>w</sub>/N</code> or average
 *                 number of documents where w occurs</li>
 *             <li>{@link LambdaTTF TTF}: <code>F<sub>w</sub>/N</code> or
 *                 average number of occurrences of w in the collection</li>
 *         </ul>
 *     </li>
 *     <li>{@link Normalization normalization}: Term frequency normalization 
 *         <blockquote>Any supported DFR normalization listed in
                       {@link DFRSimilarityFactory}</blockquote>
       </li>
 * </ol>
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link IBSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @lucene.experimental
 */

public class IBSimilarityFactory extends SimilarityFactory {
  private boolean discountOverlaps;
  private Distribution distribution;
  private Lambda lambda;
  private Normalization normalization;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", true);
    distribution = parseDistribution(params.get("distribution"));
    lambda = parseLambda(params.get("lambda"));
    normalization = DFRSimilarityFactory.parseNormalization(
        params.get("normalization"), params.get("c"), params.get("mu"), params.get("z"));
  }
  
  private Distribution parseDistribution(String expr) {
    if ("LL".equals(expr)) {
      return new DistributionLL();
    } else if ("SPL".equals(expr)) {
      return new DistributionSPL();
    } else {
      throw new RuntimeException("Invalid distribution: " + expr);
    }
  }
  
  private Lambda parseLambda(String expr) {
    if ("DF".equals(expr)) {
      return new LambdaDF();
    } else if ("TTF".equals(expr)) {
      return new LambdaTTF();
    } else {
      throw new RuntimeException("Invalid lambda: " + expr);
    }
  }

  @Override
  public Similarity getSimilarity() {
    IBSimilarity sim = new IBSimilarity(distribution, lambda, normalization);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
