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

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.suggest.FileDictionary;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Factory for {@link FileDictionary}
 */
public class FileDictionaryFactory extends DictionaryFactory {

  /** Label for defining fieldDelimiter to be used */
  public static final String FIELD_DELIMITER = "fieldDelimiter";
  
  @Override
  public Dictionary create(SolrCore core, SolrIndexSearcher searcher) {
    if (params == null) {
      // should not happen; implies setParams was not called
      throw new IllegalStateException("Value of params not set");
    }
    
    String sourceLocation = (String)params.get(Suggester.LOCATION);
    
    if (sourceLocation == null) {
      throw new IllegalArgumentException(Suggester.LOCATION + " parameter is mandatory for using FileDictionary");
    }
    
    String fieldDelimiter = (params.get(FIELD_DELIMITER) != null)
        ? (String) params.get(FIELD_DELIMITER) : 
        FileDictionary.DEFAULT_FIELD_DELIMITER;
    
    try {
      return new FileDictionary(new InputStreamReader(
          core.getResourceLoader().openResource(sourceLocation), StandardCharsets.UTF_8), fieldDelimiter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
}
