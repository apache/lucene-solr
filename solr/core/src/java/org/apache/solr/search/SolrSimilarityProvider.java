package org.apache.solr.search;

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

import org.apache.lucene.search.similarities.DefaultSimilarityProvider;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;

/**
 * SimilarityProvider that uses the default Lucene implementation, unless
 * otherwise specified by the fieldtype.
 * <p>
 * You can extend this class to customize the behavior of the parts
 * of lucene's ranking system that are not field-specific, such as
 * {@link #coord(int, int)} and {@link #queryNorm(float)}.
 */
public class SolrSimilarityProvider extends DefaultSimilarityProvider {
  private final IndexSchema schema;

  public SolrSimilarityProvider(IndexSchema schema) {
    this.schema = schema;
  }
  
  /** 
   * Solr implementation delegates to the fieldtype's similarity.
   * If this does not exist, uses the schema's default similarity.
   */
  // note: this is intentionally final, to maintain consistency with
  // whatever is specified in the the schema!
  @Override
  public final Similarity get(String field) {
    FieldType fieldType = schema.getFieldTypeNoEx(field);
    if (fieldType == null) {
      return schema.getFallbackSimilarity();
    } else {
      Similarity similarity = fieldType.getSimilarity();
      return similarity == null ? schema.getFallbackSimilarity() : similarity;
    }
  }
}
