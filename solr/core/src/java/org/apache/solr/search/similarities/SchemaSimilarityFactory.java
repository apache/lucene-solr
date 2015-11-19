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

import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Version;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SimilarityFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * SimilarityFactory that returns a {@link PerFieldSimilarityWrapper}
 * that delegates to the field type, if it's configured, otherwise
 * returns a sensible default depending on the {@link Version} matching configured.
 * </p>
 * <ul>
 *  <li><code>luceneMatchVersion &lt; 6.0</code> = {@link ClassicSimilarity}</li>
 *  <li><code>luceneMatchVersion &gt;= 6.0</code> = {@link BM25Similarity}</li>
 * </ul>
 * <p>
 * <b>NOTE:</b> Users should be aware that in addition to supporting 
 * <code>Similarity</code> configurations specified on individual 
 * field types, this factory also differs in behavior from 
 * {@link ClassicSimilarityFactory} because of other differences in the 
 * implementations of <code>PerFieldSimilarityWrapper</code> and 
 * {@link ClassicSimilarity} - notably in methods such as 
 * {@link Similarity#coord} and {@link Similarity#queryNorm}.  
 * </p>
 *
 * @see FieldType#getSimilarity
 */
public class SchemaSimilarityFactory extends SimilarityFactory implements SolrCoreAware {
  
  private Similarity similarity; // set by init
  private Similarity defaultSimilarity; // set by inform(SolrCore)
  private volatile SolrCore core;

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    this.defaultSimilarity = this.core.getSolrConfig().luceneMatchVersion.onOrAfter(Version.LUCENE_6_0_0)
      ? new BM25Similarity()
      : new ClassicSimilarity();
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
    if (null == core) {
      throw new IllegalStateException("SchemaSimilarityFactory can not be used until SolrCoreAware.inform has been called");
    }
    return similarity;
  }
}
