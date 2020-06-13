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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.spelling.suggest.LookupFactory;

/**
 * Factory for {@link AnalyzingSuggester}
 * @lucene.experimental
 */
public class AnalyzingLookupFactory extends LookupFactory {
  /**
   * If <code>true</code>, exact suggestions are returned first, even if they are prefixes
   * of other strings in the automaton (possibly with larger weights). 
   */
  public static final String EXACT_MATCH_FIRST = "exactMatchFirst";
  
  /**
   * If <code>true</code>, then a separator between tokens is preserved. This means that
   * suggestions are sensitive to tokenization (e.g. baseball is different from base ball).
   */
  public static final String PRESERVE_SEP = "preserveSep";
  
  /**
   * When multiple suggestions collide to the same analyzed form, this is the limit of
   * how many unique surface forms we keep.
   */
  public static final String MAX_SURFACE_FORMS = "maxSurfaceFormsPerAnalyzedForm";
  
  /**
   * When building the FST ("index-time"), we add each path through the tokenstream graph
   * as an individual entry. This places an upper-bound on how many expansions will be added
   * for a single suggestion.
   */
  public static final String MAX_EXPANSIONS = "maxGraphExpansions";
  
  // confusingly: the queryAnalyzerFieldType parameter is something totally different, this
  // is solr's "analysis" of the queries before they even reach the suggester (really makes
  // little sense for suggest at all, only for spellcheck). So we pick different names.
  
  /**
   * The analyzer used at "query-time" and "build-time" to analyze suggestions.
   */
  public static final String QUERY_ANALYZER = "suggestAnalyzerFieldType";

  /**
   * Whether position holes should appear in the automaton.
   */
  public static final String PRESERVE_POSITION_INCREMENTS = "preservePositionIncrements";
  
  /**
   * File name for the automaton.
   * 
   */
  private static final String FILENAME = "wfsta.bin";

  @Override
  public Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core) {
    // mandatory parameter
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
    
    // optional parameters
    
    boolean exactMatchFirst = params.get(EXACT_MATCH_FIRST) != null
    ? Boolean.valueOf(params.get(EXACT_MATCH_FIRST).toString())
    : true;
    
    boolean preserveSep = params.get(PRESERVE_SEP) != null
    ? Boolean.valueOf(params.get(PRESERVE_SEP).toString())
    : true;
    
    int flags = 0;
    if (exactMatchFirst) {
      flags |= AnalyzingSuggester.EXACT_FIRST;
    }
    if (preserveSep) {
      flags |= AnalyzingSuggester.PRESERVE_SEP;
    }
    
    int maxSurfaceFormsPerAnalyzedForm = params.get(MAX_SURFACE_FORMS) != null
    ? Integer.parseInt(params.get(MAX_SURFACE_FORMS).toString())
    : 256;
    
    int maxGraphExpansions = params.get(MAX_EXPANSIONS) != null
    ? Integer.parseInt(params.get(MAX_EXPANSIONS).toString())
    : -1;
    
    boolean preservePositionIncrements = params.get(PRESERVE_POSITION_INCREMENTS) != null
    ? Boolean.valueOf(params.get(PRESERVE_POSITION_INCREMENTS).toString())
    : false;

    return new AnalyzingSuggester(getTempDir(), "suggester", indexAnalyzer, queryAnalyzer, flags, maxSurfaceFormsPerAnalyzedForm,
        maxGraphExpansions, preservePositionIncrements);
  }

  @Override
  public String storeFileName() {
    return FILENAME;
  }
}
