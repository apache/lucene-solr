package org.apache.solr.analysis;

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

import java.io.IOException;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.stempel.StempelFilter;
import org.apache.lucene.analysis.stempel.StempelStemmer;
import org.apache.solr.common.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.util.plugin.ResourceLoaderAware;
import org.egothor.stemmer.Trie;

/**
 * Factory for {@link StempelFilter} using a Polish stemming table.
 */
public class StempelPolishStemFilterFactory extends BaseTokenFilterFactory implements ResourceLoaderAware {
  private Trie stemmer = null;
  private static final String STEMTABLE = "org/apache/lucene/analysis/pl/stemmer_20000.tbl";
  
  public TokenStream create(TokenStream input) {
    return new StempelFilter(input, new StempelStemmer(stemmer));
  }

  public void inform(ResourceLoader loader) {
    try {
      stemmer = StempelStemmer.load(loader.openResource(STEMTABLE));
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Could not load stem table: " + STEMTABLE);
    }
  }
}
