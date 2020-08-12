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
import org.apache.lucene.search.suggest.analyzing.FuzzySuggester;
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.spelling.suggest.LookupFactory;

/**
 * Factory for {@link FuzzySuggester}
 * @lucene.experimental
 */
public class FuzzyLookupFactory extends LookupFactory {

  /**
   * If <code>true</code>, maxEdits, minFuzzyLength, transpositions and nonFuzzyPrefix 
   * will be measured in Unicode code points (actual letters) instead of bytes.
   */
  public static final String UNICODE_AWARE = "unicodeAware";

  /**
   * Maximum number of edits allowed, used by {@link LevenshteinAutomata#toAutomaton(int)}
   * in bytes or Unicode code points (if {@link #UNICODE_AWARE} option is set to true).
   */
  public static final String MAX_EDITS = "maxEdits";
  
  /**
   * If transpositions are allowed, Fuzzy suggestions will be computed based on a primitive 
   * edit operation. If it is false, it will be based on the classic Levenshtein algorithm.
   * Transpositions of bytes or Unicode code points (if {@link #UNICODE_AWARE} option is set to true).
   */
  public static final String TRANSPOSITIONS = "transpositions";
  
  /**
   * Length of common (non-fuzzy) prefix for the suggestions
   * in bytes or Unicode code points (if {@link #UNICODE_AWARE} option is set to true).
   */
  public static final String NON_FUZZY_PREFIX = "nonFuzzyPrefix";
  
  /**
   * Minimum length of lookup key before any edits are allowed for the suggestions
   * in bytes or Unicode code points (if {@link #UNICODE_AWARE} option is set to true).
   */
  public static final String MIN_FUZZY_LENGTH = "minFuzzyLength";
  
  /**
   * File name for the automaton.
   */
  private static final String FILENAME = "fwfsta.bin";
  
  @Override
  public Lookup create(@SuppressWarnings({"rawtypes"})NamedList params, SolrCore core) {
    
    // mandatory parameter
    Object fieldTypeName = params.get(AnalyzingLookupFactory.QUERY_ANALYZER);
    if (fieldTypeName == null) {
      throw new IllegalArgumentException("Error in configuration: " + AnalyzingLookupFactory.QUERY_ANALYZER + " parameter is mandatory");
    }
    // retrieve index and query analyzers for the field
    FieldType ft = core.getLatestSchema().getFieldTypeByName(fieldTypeName.toString());
    if (ft == null) {
      throw new IllegalArgumentException("Error in configuration: " + fieldTypeName.toString() + " is not defined in the schema");
    }
    Analyzer indexAnalyzer = ft.getIndexAnalyzer();
    Analyzer queryAnalyzer = ft.getQueryAnalyzer();
    
    // optional parameters
    boolean exactMatchFirst = (params.get(AnalyzingLookupFactory.EXACT_MATCH_FIRST) != null)
    ? Boolean.valueOf(params.get(AnalyzingLookupFactory.EXACT_MATCH_FIRST).toString())
    : true;
        
    boolean preserveSep = (params.get(AnalyzingLookupFactory.PRESERVE_SEP) != null)
    ? Boolean.valueOf(params.get(AnalyzingLookupFactory.PRESERVE_SEP).toString())
    : true;
        
    int options = 0;
    if (exactMatchFirst) {
      options |= FuzzySuggester.EXACT_FIRST;
    }
    if (preserveSep) {
      options |= FuzzySuggester.PRESERVE_SEP;
    }
    
    int maxSurfaceFormsPerAnalyzedForm = (params.get(AnalyzingLookupFactory.MAX_SURFACE_FORMS) != null)
    ? Integer.parseInt(params.get(AnalyzingLookupFactory.MAX_SURFACE_FORMS).toString())
    : 256;
        
    int maxGraphExpansions = (params.get(AnalyzingLookupFactory.MAX_EXPANSIONS) != null)
    ? Integer.parseInt(params.get(AnalyzingLookupFactory.MAX_EXPANSIONS).toString())
    : -1;

    boolean preservePositionIncrements = params.get(AnalyzingLookupFactory.PRESERVE_POSITION_INCREMENTS) != null
    ? Boolean.valueOf(params.get(AnalyzingLookupFactory.PRESERVE_POSITION_INCREMENTS).toString())
    : false;
    
    int maxEdits = (params.get(MAX_EDITS) != null)
    ? Integer.parseInt(params.get(MAX_EDITS).toString())
    : FuzzySuggester.DEFAULT_MAX_EDITS;
    
    boolean transpositions = (params.get(TRANSPOSITIONS) != null)
    ? Boolean.parseBoolean(params.get(TRANSPOSITIONS).toString())
    : FuzzySuggester.DEFAULT_TRANSPOSITIONS;
        
    int nonFuzzyPrefix = (params.get(NON_FUZZY_PREFIX) != null)
    ? Integer.parseInt(params.get(NON_FUZZY_PREFIX).toString())
    :FuzzySuggester.DEFAULT_NON_FUZZY_PREFIX;
    
    
    int minFuzzyLength = (params.get(MIN_FUZZY_LENGTH) != null)
    ? Integer.parseInt(params.get(MIN_FUZZY_LENGTH).toString())
    :FuzzySuggester.DEFAULT_MIN_FUZZY_LENGTH;
    
    boolean unicodeAware = (params.get(UNICODE_AWARE) != null)
    ? Boolean.valueOf(params.get(UNICODE_AWARE).toString())
    : FuzzySuggester.DEFAULT_UNICODE_AWARE;
    
    return new FuzzySuggester(getTempDir(), "suggester", indexAnalyzer, queryAnalyzer, options, maxSurfaceFormsPerAnalyzedForm,
        maxGraphExpansions, preservePositionIncrements, maxEdits, transpositions, nonFuzzyPrefix,
        minFuzzyLength, unicodeAware);
  }

  @Override
  public String storeFileName() {
    return FILENAME;
  }
  
}
