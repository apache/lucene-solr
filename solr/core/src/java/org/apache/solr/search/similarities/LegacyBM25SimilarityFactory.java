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
import org.apache.lucene.misc.search.similarity.LegacyBM25Similarity;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.schema.SimilarityFactory;

/**
 * Factory for {@link LegacyBM25Similarity}. 
 * Use this to force explicit creation of the BM25 formula that was used by BM25Similarity before Solr/Lucene 8.0.0.
 * Note that {@link SchemaSimilarityFactory} will automatically create an instance of LegacyBM25Similarity if luceneMatchVersion is &lt; 8.0.0
 * <p>
 * Parameters:
 * <ul>
 *   <li>k1 (float): Controls non-linear term frequency normalization (saturation).
 *                   The default is <code>1.2</code>
 *   <li>b (float): Controls to what degree document length normalizes tf values.
 *                  The default is <code>0.75</code>
 *   <li>discountOverlaps (bool): True if overlap tokens (tokens with a position of increment of zero) are
 *                                discounted from the document's length.
 *                                The default is <code>true</code>
 * </ul>
 * @lucene.experimental
 * @since 8.0.0
 */
public class LegacyBM25SimilarityFactory extends SimilarityFactory {
    private LegacyBM25Similarity similarity;

  @Override
  public void init(SolrParams params) {
    super.init(params);
    boolean discountOverlaps = params.getBool("discountOverlaps", true);
    float k1 = params.getFloat("k1", 1.2f);
    float b = params.getFloat("b", 0.75f);
    similarity = new LegacyBM25Similarity(k1, b, discountOverlaps);
  }

  @Override
  public Similarity getSimilarity() {
    return similarity;
  }
}
