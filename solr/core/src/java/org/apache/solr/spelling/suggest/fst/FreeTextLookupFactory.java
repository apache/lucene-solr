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
package org.apache.solr.spelling.suggest.fst;

import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.FreeTextSuggester;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.spelling.suggest.LookupFactory;

/** 
 * LookupFactory implementation for {@link FreeTextSuggester}
 * */
public class FreeTextLookupFactory extends LookupFactory {
  
  /**
   * The analyzer used at "query-time" and "build-time" to analyze suggestions.
   */
  public static final String QUERY_ANALYZER = "suggestFreeTextAnalyzerFieldType";
  
  /** 
   * The n-gram model to use in the underlying suggester; Default value is 2.
   * */
  public static final String NGRAMS = "ngrams";
  
  /**
   * The separator to use in the underlying suggester;
   * */
  public static final String SEPARATOR = "separator";
  
  /**
   * File name for the automaton.
   */
  private static final String FILENAME = "ftsta.bin";
  
  
  @Override
  public Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core) {
    Object fieldTypeName = params.get(QUERY_ANALYZER);
    if (fieldTypeName == null) {
      throw new IllegalArgumentException("Error in configuration: " + QUERY_ANALYZER + " parameter is mandatory");
    }
    FieldType ft = core.getLatestSchema().getFieldTypeByName(fieldTypeName.toString());
    if (ft == null) {
      throw new IllegalArgumentException("Error in configuration: " + fieldTypeName.toString() + " is not defined in the schema");
    }
    
    Analyzer indexAnalyzer = ft.getIndexAnalyzer();
    Analyzer queryAnalyzer = ft.getQueryAnalyzer();
    
    int grams = (params.get(NGRAMS) != null) 
        ? Integer.parseInt(params.get(NGRAMS).toString()) 
        : FreeTextSuggester.DEFAULT_GRAMS;
    
    byte separator = (params.get(SEPARATOR) != null) 
        ? params.get(SEPARATOR).toString().getBytes(StandardCharsets.UTF_8)[0]
        : FreeTextSuggester.DEFAULT_SEPARATOR;
    
    return new FreeTextSuggester(indexAnalyzer, queryAnalyzer, grams, separator);
  }
  
  @Override
  public String storeFileName() {
    return FILENAME;
  }
  
}
