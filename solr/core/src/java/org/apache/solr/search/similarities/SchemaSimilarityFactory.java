package org.apache.solr.search.similarities;

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

import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaAware;
import org.apache.solr.schema.SimilarityFactory;

/**
 * SimilarityFactory that returns a PerFieldSimilarityWrapper
 * that delegates to the fieldtype, if its configured, otherwise
 * {@link DefaultSimilarity}.
 */
public class SchemaSimilarityFactory extends SimilarityFactory implements SchemaAware {
  private Similarity similarity;
  private Similarity defaultSimilarity = new DefaultSimilarity();
  
  @Override
  public void inform(final IndexSchema schema) {
    similarity = new PerFieldSimilarityWrapper() {
      @Override
      public Similarity get(String name) {
        FieldType fieldType = schema.getFieldTypeNoEx(name);
        if (fieldType == null) {
          return defaultSimilarity;
        } else {
          Similarity similarity = fieldType.getSimilarity();
          return similarity == null ? defaultSimilarity : similarity;
        }
      }
    };
  }

  @Override
  public Similarity getSimilarity() {
    assert similarity != null : "inform must be called first";
    return similarity;
  }
}
