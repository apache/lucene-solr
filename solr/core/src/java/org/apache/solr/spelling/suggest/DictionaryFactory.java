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
package org.apache.solr.spelling.suggest;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Encapsulates shared fields for all types of dictionaryFactory classes
 */
public abstract class DictionaryFactory {
  
  /** Default dictionary implementation to use for FileBasedDictionaries */
  public static String DEFAULT_FILE_BASED_DICT = FileDictionaryFactory.class.getName();
  
  /** Default dictionary implementation to use for IndexBasedDictionaries */
  public static String DEFAULT_INDEX_BASED_DICT = HighFrequencyDictionaryFactory.class.getName(); 
  
  @SuppressWarnings({"rawtypes"})
  protected NamedList params;
  
  /** Sets the parameters available to SolrSuggester for use in Dictionary creation */
  public void setParams(@SuppressWarnings({"rawtypes"})NamedList params) {
    this.params = params;
  }
  
  /**
   * Create a Dictionary using options in <code>core</code> and optionally
   * uses <code>searcher</code>, in case of index based dictionaries
   */
  public abstract Dictionary create(SolrCore core, SolrIndexSearcher searcher);
  
}
