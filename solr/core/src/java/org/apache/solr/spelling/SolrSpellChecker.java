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
package org.apache.solr.spelling;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.search.spell.LevenshteinDistance;
import org.apache.lucene.search.spell.StringDistance;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.SuggestWordQueue;
import org.apache.solr.client.solrj.response.SpellCheckResponse;
import org.apache.solr.common.params.SpellingParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.SpellCheckMergeData;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.SolrIndexSearcher;


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

  public String init(@SuppressWarnings({"rawtypes"})NamedList config, SolrCore core) {
    name = (String) config.get(DICTIONARY_NAME);
    if (name == null) {
      name = DEFAULT_DICTIONARY_NAME;
    }
    field = (String)config.get(FIELD);
    IndexSchema schema = core.getLatestSchema();
    if (field != null && schema.getFieldTypeNoEx(field) != null)  {
      analyzer = schema.getFieldType(field).getQueryAnalyzer();
    }
    fieldTypeName = (String) config.get(FIELD_TYPE);
    if (schema.getFieldTypes().containsKey(fieldTypeName))  {
      FieldType fieldType = schema.getFieldTypes().get(fieldTypeName);
      analyzer = fieldType.getQueryAnalyzer();
    }
    if (analyzer == null)   {
      analyzer = new WhitespaceAnalyzer();
    }
    return name;
  }
  /**
   * Integrate spelling suggestions from the various shards in a distributed environment.
   */
  public SpellingResult mergeSuggestions(SpellCheckMergeData mergeData, int numSug, int count, boolean extendedResults) {
    float min = 0.5f;
    try {
      min = getAccuracy();
    } catch(UnsupportedOperationException uoe) {
      //just use .5 as a default
    }
    
    StringDistance sd = null;
    try {
      sd = getStringDistance() == null ? new LevenshteinDistance() : getStringDistance();    
    } catch(UnsupportedOperationException uoe) {
      sd = new LevenshteinDistance();
    }
    
    SpellingResult result = new SpellingResult();
    for (Map.Entry<String, HashSet<String>> entry : mergeData.origVsSuggested.entrySet()) {
      String original = entry.getKey();
      
      //Only use this suggestion if all shards reported it as misspelled, 
      //unless it was not a term original to the user's query
      //(WordBreakSolrSpellChecker can add new terms to the response, and we want to keep these)
      Integer numShards = mergeData.origVsShards.get(original);
      if(numShards<mergeData.totalNumberShardResponses && mergeData.isOriginalToQuery(original)) {
        continue;
      }
      
      HashSet<String> suggested = entry.getValue();
      SuggestWordQueue sugQueue = new SuggestWordQueue(numSug);
      for (String suggestion : suggested) {
        SuggestWord sug = mergeData.suggestedVsWord.get(suggestion);
        sug.score = sd.getDistance(original, sug.string);
        if (sug.score < min) continue;
        sugQueue.insertWithOverflow(sug);
        if (sugQueue.size() == numSug) {
          // if queue full, maintain the minScore score
          min = sugQueue.top().score;
        }
      }

      // create token
      SpellCheckResponse.Suggestion suggestion = mergeData.origVsSuggestion.get(original);
      Token token = new Token(original, suggestion.getStartOffset(), suggestion.getEndOffset());

      // get top 'count' suggestions out of 'sugQueue.size()' candidates
      SuggestWord[] suggestions = new SuggestWord[Math.min(count, sugQueue.size())];
      // skip the first sugQueue.size() - count elements
      for (int k=0; k < sugQueue.size() - count; k++) sugQueue.pop();
      // now collect the top 'count' responses
      for (int k = Math.min(count, sugQueue.size()) - 1; k >= 0; k--)  {
        suggestions[k] = sugQueue.pop();
      }

      if (extendedResults) {
        Integer o = mergeData.origVsFreq.get(original);
        if (o != null) result.addFrequency(token, o);
        for (SuggestWord word : suggestions)
          result.add(token, word.string, word.freq);
      } else {
        List<String> words = new ArrayList<>(sugQueue.size());
        for (SuggestWord word : suggestions) words.add(word.string);
        result.add(token, words);
      }
    }
    return result;
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
   * @throws IOException If there is a low-level I/O error.
   */
  public abstract void reload(SolrCore core, SolrIndexSearcher searcher) throws IOException;

  /**
   * (re)Builds the spelling index.  May be a NOOP if the implementation doesn't require building, or can't be rebuilt.
   */
  public abstract void build(SolrCore core, SolrIndexSearcher searcher) throws IOException;
  
  /**
   * Get the value of {@link SpellingParams#SPELLCHECK_ACCURACY} if supported.  
   * Otherwise throws UnsupportedOperationException.
   */
  protected float getAccuracy() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Get the distance implementation used by this spellchecker, or NULL if not applicable.
   */
  protected StringDistance getStringDistance()  {
    throw new UnsupportedOperationException();
  }


  /**
   * Get suggestions for the given query.  Tokenizes the query using a field appropriate Analyzer.
   * The {@link SpellingResult#getSuggestions()} suggestions must be ordered by best suggestion first.
   *
   * @param options The {@link SpellingOptions} to use
   * @return The {@link SpellingResult} suggestions
   * @throws IOException if there is an error producing suggestions
   */
  public abstract SpellingResult getSuggestions(SpellingOptions options) throws IOException;
  
  public boolean isSuggestionsMayOverlap() {
    return false;
  }
}
