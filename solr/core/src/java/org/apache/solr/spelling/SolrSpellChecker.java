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
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;


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
  public static final String FIELD = "field";
  public static final String FIELD_TYPE = "fieldType";
  /** Dictionary name */
  protected String name;
  protected Analyzer analyzer;
  protected String field;
  protected String fieldTypeName;

  public String init(NamedList config, SolrCore core) {
    name = (String) config.get(DICTIONARY_NAME);
    if (name == null) {
      name = DEFAULT_DICTIONARY_NAME;
    }
    field = (String)config.get(FIELD);
    if (field != null && core.getSchema().getFieldTypeNoEx(field) != null)  {
      analyzer = core.getSchema().getFieldType(field).getQueryAnalyzer();
    }
    fieldTypeName = (String) config.get(FIELD_TYPE);
    if (core.getSchema().getFieldTypes().containsKey(fieldTypeName))  {
      FieldType fieldType = core.getSchema().getFieldTypes().get(fieldTypeName);
      analyzer = fieldType.getQueryAnalyzer();
    }
    if (analyzer == null)   {
      analyzer = new WhitespaceAnalyzer(core.getSolrConfig().luceneMatchVersion);
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
   * Get suggestions for the given query.  Tokenizes the query using a field appropriate Analyzer.
   * The {@link SpellingResult#getSuggestions()} suggestions must be ordered by best suggestion first.
   * <p/>
   *
   * @param options The {@link SpellingOptions} to use
   * @return The {@link SpellingResult} suggestions
   * @throws IOException if there is an error producing suggestions
   */
  public abstract SpellingResult getSuggestions(SpellingOptions options) throws IOException;
}
