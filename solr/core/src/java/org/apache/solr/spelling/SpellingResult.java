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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;


/**
 * Implementations of SolrSpellChecker must return suggestions as SpellResult instance.
 * This is converted into the required NamedList format in SpellCheckComponent.
 * 
 * @since solr 1.3
 */
public class SpellingResult {
  private Collection<Token> tokens;

  /**
   * Key == token
   * Value = Map  -> key is the suggestion, value is the frequency of the token in the collection
   */
  private Map<Token, LinkedHashMap<String, Integer>> suggestions = new LinkedHashMap<>();
  private Map<Token, Integer> tokenFrequency;
  public static final int NO_FREQUENCY_INFO = -1;


  public SpellingResult() {
  }

  public SpellingResult(Collection<Token> tokens) {
    this.tokens = tokens;
  }

  /**
   * Adds a whole bunch of suggestions, and does not worry about frequency.
   *
   * @param token The token to associate the suggestions with
   * @param suggestions The suggestions
   */
  public void add(Token token, List<String> suggestions) {
    LinkedHashMap<String, Integer> map = this.suggestions.get(token);
    if (map == null ) {
      map = new LinkedHashMap<>();
      this.suggestions.put(token, map);
    }
    for (String suggestion : suggestions) {
      map.put(suggestion, NO_FREQUENCY_INFO);
    }
  }

  /**
   * Adds an original token with its document frequency
   * 
   * @param token original token
   * @param docFreq original token's document frequency
   */
  public void addFrequency(Token token, int docFreq) {
    if (tokenFrequency == null) {
      tokenFrequency = new LinkedHashMap<>();
    }
    tokenFrequency.put(token, docFreq);
  }

  /**
   * Suggestions must be added with the best suggestion first.  ORDER is important.
   * @param token The {@link Token}
   * @param suggestion The suggestion for the Token
   * @param docFreq The document frequency
   */
  public void add(Token token, String suggestion, int docFreq) {
    LinkedHashMap<String, Integer> map = this.suggestions.get(token);
    //Don't bother adding if we already have this token
    if (map == null) {
      map = new LinkedHashMap<>();
      this.suggestions.put(token, map);
    }
    map.put(suggestion, docFreq);
  }

  /**
   * Gets the suggestions for the given token.
   *
   * @param token The {@link Token} to look up
   * @return A LinkedHashMap of the suggestions.  Key is the suggestion, value is the token frequency in the index, else {@link #NO_FREQUENCY_INFO}.
   *
   * The suggestions are added in sorted order (i.e. best suggestion first) then the iterator will return the suggestions in order
   */
  public LinkedHashMap<String, Integer> get(Token token) {
    return suggestions.get(token);
  }

  /**
   * The token frequency of the input token in the collection
   *
   * @param token The token
   * @return The frequency or null
   */
  public Integer getTokenFrequency(Token token) {
    return tokenFrequency.get(token);
  }

  public boolean hasTokenFrequencyInfo() {
    return tokenFrequency != null && !tokenFrequency.isEmpty();
  }

  /**
   * All the suggestions.  The ordering of the inner LinkedHashMap is by best suggestion first.
   * @return The Map of suggestions for each Token.  Key is the token, value is a LinkedHashMap whose key is the Suggestion and the value is the frequency or {@link #NO_FREQUENCY_INFO} if frequency info is not available.
   *
   */
  public Map<Token, LinkedHashMap<String, Integer>> getSuggestions() {
    return suggestions;
  }

  public Map<Token, Integer> getTokenFrequency() {
    return tokenFrequency;
  }

  /**
   * @return The original tokens
   */
  public Collection<Token> getTokens() {
    return tokens;
  }

  public void setTokens(Collection<Token> tokens) {
    this.tokens = tokens;
  }
}
