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

import java.text.ParseException;

import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.search.similarity.LegacyBM25Similarity;
import org.apache.lucene.util.Version;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * Factory for BM25. If luceneMatchVersion is &lt; 8.0.0 then
 * an instance of {@link LegacyBM25Similarity} is returned, as this uses the same formula
 * as used for BM25Similarity shipped with those versions. If luceneMatchVersion is &gt;= 8.0.0 then
 * the new {@link BM25Similarity} without (k1 + 1) in in numerator is used.
 * This is the default similarity since 8.x 
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
public class BM25SimilarityFactory extends SimilarityFactory implements SolrCoreAware {
  private boolean discountOverlaps;
  private float k1;
  private float b;
  private Version coreVersion;
  private SolrCore core;

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    this.coreVersion = this.core.getSolrConfig().luceneMatchVersion;
  }
  
  @Override
  public void init(SolrParams params) {
    super.init(params);
    discountOverlaps = params.getBool("discountOverlaps", true);
    k1 = params.getFloat("k1", 1.2f);
    b = params.getFloat("b", 0.75f);
  }

  @Override
  public Similarity getSimilarity() {
    if (null == core) {
      throw new IllegalStateException("BM25SimilarityFactory can not be used until SolrCoreAware.inform has been called");
    }
    if (!coreVersion.onOrAfter(Version.LUCENE_8_0_0)) {
      LegacyBM25Similarity sim = new LegacyBM25Similarity(k1, b);
      sim.setDiscountOverlaps(discountOverlaps);
      return sim;
    }

    BM25Similarity sim = new BM25Similarity(k1, b);
    sim.setDiscountOverlaps(discountOverlaps);
    return sim;
  }
}
