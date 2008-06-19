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
import org.apache.solr.core.SolrResourceLoader;

import java.io.IOException;
import java.util.Collection;


/**
 * <p>
 * Refer to http://wiki.apache.org/solr/SpellCheckComponent for more details
 * </p>
 * 
 * @since solr 1.3
 */
public abstract class SolrSpellChecker {
  public static final String DICTIONARY_NAME = "name";
  public static final String DEFAULT_DICTIONARY_NAME = "default";
  protected String name;
  protected Analyzer analyzer;

  public String init(NamedList config, SolrResourceLoader loader){
    name = (String) config.get(DICTIONARY_NAME);
    if (name == null) {
      name = DEFAULT_DICTIONARY_NAME;
    }
    return name;
  }
  
  public Analyzer getQueryAnalyzer()    {
    return analyzer;
  }


  public String getDictionaryName() {
    return name;
  }


  /**
   * Reload the index.  Useful if an external process is responsible for building the spell checker.
   *
   * @throws java.io.IOException
   */
  public abstract void reload() throws IOException;

  /**
   * (re)Build The Spelling index.  May be a NOOP if the ipmlementation doesn't require building, or can't be rebuilt
   *
   * @param core The SolrCore
   */
  public abstract void build(SolrCore core);

  /**
   * Assumes count = 1, onlyMorePopular = false, extendedResults = false
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   */
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader) throws IOException {
    return getSuggestions(tokens, reader, 1, false, false);
  }

  /**
   * Assumes onlyMorePopular = false, extendedResults = false
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   */
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, int count) throws IOException {
    return getSuggestions(tokens, reader, count, false, false);
  }


  /**
   * Assumes count = 1.
   *
   * @see #getSuggestions(Collection, org.apache.lucene.index.IndexReader, int, boolean, boolean)
   */
  public SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, boolean onlyMorePopular, boolean extendedResults) throws IOException {
    return getSuggestions(tokens, reader, 1, onlyMorePopular, extendedResults);
  }

  /**
   * Get suggestions for the given query.  Tokenizes the query using a field appropriate Analyzer.  The {@link SpellingResult#getSuggestions()} suggestions must be ordered by 
   * best suggestion first
   *
   * @param tokens          The Tokens to be spell checked.
   * @param reader          The (optional) IndexReader.  If there is not IndexReader, than extendedResults are not possible
   * @param count The maximum number of suggestions to return
   * @param onlyMorePopular  TODO
   * @param extendedResults  TODO
   * @return
   * @throws IOException
   */
  public abstract SpellingResult getSuggestions(Collection<Token> tokens, IndexReader reader, int count,
                                                boolean onlyMorePopular, boolean extendedResults)
          throws IOException;
}
