package org.apache.solr.search.similarities;

/**
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

import org.apache.lucene.search.similarities.SimilarityProvider; // javadoc
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SimilarityProviderFactory;
import org.apache.solr.search.SolrSimilarityProvider;

/**
 * This class is aimed at non-VSM models, and therefore both the 
 * {@link SimilarityProvider#coord} and
 * {@link SimilarityProvider#queryNorm} methods return {@code 1}.
 * @lucene.experimental
 */
public class BasicSimilarityProviderFactory extends SimilarityProviderFactory {

  @Override
  public SolrSimilarityProvider getSimilarityProvider(IndexSchema schema) {
    return new SolrSimilarityProvider(schema) {
      @Override
      public float coord(int overlap, int maxOverlap) {
        return 1f;
      }

      @Override
      public float queryNorm(float sumOfSquaredWeights) {
        return 1f;
      }
    };
  }
}
