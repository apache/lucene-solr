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
import org.apache.lucene.search.spell.HighFrequencyDictionary;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.SolrSpellChecker;

/**
 * Factory for {@link HighFrequencyDictionary}
 */
public class HighFrequencyDictionaryFactory extends DictionaryFactory {
    /**
   * Minimum frequency of terms to consider when building the dictionary.
   */
  public static final String THRESHOLD_TOKEN_FREQUENCY = "threshold";

  @Override
  public Dictionary create(SolrCore core, SolrIndexSearcher searcher) {
    if(params == null) {
      // should not happen; implies setParams was not called
      throw new IllegalStateException("Value of params not set");
    }
    String field = (String)params.get(SolrSpellChecker.FIELD);
    
    if (field == null) {
      throw new IllegalArgumentException(SolrSpellChecker.FIELD + " is a mandatory parameter");
    }
    
    float threshold = params.get(THRESHOLD_TOKEN_FREQUENCY) == null ? 0.0f
        : (Float)params.get(THRESHOLD_TOKEN_FREQUENCY);
    
    return new HighFrequencyDictionary(searcher.getIndexReader(), field, threshold);
  }
  
}
