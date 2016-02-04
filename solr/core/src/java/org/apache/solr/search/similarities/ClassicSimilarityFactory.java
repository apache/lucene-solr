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

import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity; // javadoc
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link ClassicSimilarity}
 * <p>
 * ClassicSimilarity is Lucene's original scoring implementation, based
 * upon the Vector Space Model.
 * <p>
 * Optional settings:
 * <ul>
 *   <li>discountOverlaps (bool): Sets
 *       {@link ClassicSimilarity#setDiscountOverlaps(boolean)}</li>
 * </ul>
 * @see TFIDFSimilarity
 * @lucene.experimental
 */
public class ClassicSimilarityFactory extends SimilarityFactory {

  /** Init param name for specifying the value to use in 
   * {@link ClassicSimilarity#setDiscountOverlaps(boolean)} 
   */
  public static final String DISCOUNT_OVERLAPS = "discountOverlaps";

  /** 
   * Controls the value of {@link ClassicSimilarity#setDiscountOverlaps(boolean)} 
   * on newly constructed instances of {@link ClassicSimilarity}
   */
  protected boolean discountOverlaps = true;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool(DISCOUNT_OVERLAPS, true);
  }

  @Override
  public Similarity getSimilarity() {
    ClassicSimilarity sim = new ClassicSimilarity();
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
