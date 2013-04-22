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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * SimilarityFactory that returns a {@link PerFieldSimilarityWrapper}
 * that delegates to the field type, if it's configured, otherwise
 * {@link DefaultSimilarity}.
 *
 * <p>
 * <b>NOTE:</b> Users should be aware that in addition to supporting 
 * <code>Similarity</code> configurations specified on individual 
 * field types, this factory also differs in behavior from 
 * {@link DefaultSimilarityFactory} because of other differences in the 
 * implementations of <code>PerFieldSimilarityWrapper</code> and 
 * <code>DefaultSimilarity</code> - notably in methods such as 
 * {@link Similarity#coord} and {@link Similarity#queryNorm}.  
 * </p>
 *
 * @see FieldType#getSimilarity
 */
public class SchemaSimilarityFactory extends SimilarityFactory implements SolrCoreAware {
  private Similarity similarity;
  private Similarity defaultSimilarity = new DefaultSimilarity();
  private volatile SolrCore core;

  @Override
  public void inform(SolrCore core) {
    this.core = core;
  }
  
  @Override
  public void init(SolrParams args) {
    super.init(args);
    similarity = new PerFieldSimilarityWrapper() {
      @Override
      public Similarity get(String name) {
        FieldType fieldType = core.getLatestSchema().getFieldTypeNoEx(name);
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
    assert core != null : "inform must be called first";
    return similarity;
  }
}
