package org.apache.solr.spelling;
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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.index.IndexReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.Collection;


/**
 * <p>
 * Refer to <a href="http://wiki.apache.org/solr/SpellCheckComponent">SpellCheckComponent</a>
 * for more details.
 * </p>
 * 
 * @since solr 1.3
 */
public abstract class SolrSpellChecker {
  public static final String DICTIONARY_NAME = "name";
  public static final String DEFAULT_DICTIONARY_NAME = "default";
  /** Dictionary name */
  protected String name;
  protected Analyzer analyzer;

  public String init(NamedList config, SolrCore core) {
    name = (String) config.get(DICTIONARY_NAME);
    if (name == null) {
      name = DEFAULT_DICTIONARY_NAME;
    }
    return name;
  }
  
  public Analyzer getQueryAnalyzer() {
    return analyzer;
  }

  public String getDictionaryName() {
    return name;
  }

  /**
   * Reloads the index.  Useful if an external process is responsible for building the spell checker.
   *
   * @throws java.io.IOException
   */
  public abstract void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException;

  /**
   * (re)Builds the spelling index.  May be a NOOP if the implementation doesn't require building, or can't be rebuilt.
   */
  public abstract void build(SolrCore core, SolrIndexSearcher searcher);

  /**
   * Assumes count = 1, onlyMorePopular = false, extendedResults = false
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   *
   * @deprecated This method will be removed in 4.x in favor of {@link #getSuggestions(org.apache.solr.spelling.SpellingOptions)}
   */
  @Deprecated
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader) throws IOException {
    return getSuggestions(tokens, reader, 1, false, false);
  }

  /**
   * Assumes onlyMorePopular = false, extendedResults = false
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   *
   * @deprecated This method will be removed in 4.x in favor of {@link #getSuggestions(org.apache.solr.spelling.SpellingOptions)}
   */
  @Deprecated
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, int count) throws IOException {
    return getSuggestions(tokens, reader, count, false, false);
  }


  /**
   * Assumes count = 1.
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   *
   * @deprecated This method will be removed in 4.x in favor of {@link #getSuggestions(org.apache.solr.spelling.SpellingOptions)}
   */
  @Deprecated
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, boolean onlyMorePopular, boolean extendedResults) throws IOException {
    return getSuggestions(tokens, reader, 1, onlyMorePopular, extendedResults);
  }

  /**
   * Get suggestions for the given query.  Tokenizes the query using a field appropriate Analyzer.
   * The {@link SpellingResult#getSuggestions()} suggestions must be ordered by best suggestion first.
   *
   * @param tokens          The Tokens to be spell checked.
   * @param reader          The (optional) IndexReader.  If there is not IndexReader, than extendedResults are not possible
   * @param count The maximum number of suggestions to return
   * @param onlyMorePopular  TODO
   * @param extendedResults  TODO
   * @throws IOException
   *
   * @deprecated This method will be removed in 4.x in favor of {@link #getSuggestions(org.apache.solr.spelling.SpellingOptions)}
   */
  @Deprecated
  public abstract SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, int count,
                                                boolean onlyMorePopular, boolean extendedResults)
          throws IOException;

   /**
   * Get suggestions for the given query.  Tokenizes the query using a field appropriate Analyzer.
   * The {@link SpellingResult#getSuggestions()} suggestions must be ordered by best suggestion first.
   * <p/>
    * Note: This method is abstract in Solr 4.0 and beyond and is the recommended way of implementing the spell checker.  For now,
    * it calls {@link #getSuggestions(java.util.Collection, org.apache.lucene.index.IndexReader, boolean, boolean)}.
    *
   *
   * @param options The {@link SpellingOptions} to use
   * @return The {@link SpellingResult} suggestions
   * @throws IOException if there is an error producing suggestions
   */
  public SpellingResult getSuggestions(SpellingOptions options) throws IOException{
     return getSuggestions(options.tokens, options.reader, options.count, options.onlyMorePopular, options.extendedResults);
   }

}
