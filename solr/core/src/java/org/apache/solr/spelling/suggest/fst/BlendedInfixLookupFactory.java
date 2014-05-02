package org.apache.solr.spelling.suggest.fst;

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

import java.io.File;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.search.suggest.analyzing.BlendedInfixSuggester.BlenderType;
import org.apache.lucene.search.suggest.analyzing.BlendedInfixSuggester;
import org.apache.lucene.store.FSDirectory;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;

/**
 * Factory for {@link BlendedInfixLookupFactory}
 * @lucene.experimental
 */
public class BlendedInfixLookupFactory extends AnalyzingInfixLookupFactory {
  
  /**
   * Blender type used to calculate weight coefficient using the position
   * of the first matching word
   * </p>
   * Available blender types are: </br>
   *  linear: weight*(1 - 0.10*position) [default]</br>
   *  reciprocal: weight/(1+position) 
   */
  private static final String BLENDER_TYPE = "blenderType";
  
  /** 
   * Factor to multiply the number of searched elements
   * Default is 10
   */
  private static final String NUM_FACTOR = "numFactor";

  /** 
   * Default path where the index for the suggester is stored/loaded from
   * */
  private static final String DEFAULT_INDEX_PATH = "blendedInfixSuggesterIndexDir";

  /**
   * File name for the automaton.
   */
  private static final String FILENAME = "bifsta.bin";
  
  
  @Override
  public Lookup create(NamedList params, SolrCore core) {
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
    
    String indexPath = params.get(INDEX_PATH) != null
    ? params.get(INDEX_PATH).toString()
    : DEFAULT_INDEX_PATH;
    
    int minPrefixChars = params.get(MIN_PREFIX_CHARS) != null
    ? Integer.parseInt(params.get(MIN_PREFIX_CHARS).toString())
    : AnalyzingInfixSuggester.DEFAULT_MIN_PREFIX_CHARS;

    BlenderType blenderType = getBlenderType(params.get(BLENDER_TYPE));
    
    int numFactor = params.get(NUM_FACTOR) != null
    ? Integer.parseInt(params.get(NUM_FACTOR).toString())
    : BlendedInfixSuggester.DEFAULT_NUM_FACTOR;
    
    try {
      return new BlendedInfixSuggester(core.getSolrConfig().luceneMatchVersion, 
                                       FSDirectory.open(new File(indexPath)),
                                       indexAnalyzer, queryAnalyzer, minPrefixChars,
                                       blenderType, numFactor);
    } catch (IOException e) {
      throw new RuntimeException();
    }
  }

  @Override
  public String storeFileName() {
    return FILENAME;
  }
  
  private BlenderType getBlenderType(Object blenderTypeParam) {
    BlenderType blenderType = BlenderType.POSITION_LINEAR;
    if (blenderTypeParam != null) {
      String blenderTypeStr = blenderTypeParam.toString();
      if (blenderTypeStr.equalsIgnoreCase("reciprocal")) {
        blenderType = BlenderType.POSITION_RECIPROCAL;
      }
    }
    return blenderType;
  }
}
