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
package org.apache.solr.client.solrj.response;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

/**
 * Encapsulates responses from the Suggester Component
 */
public class SuggesterResponse {

  private static final String SUGGESTIONS_NODE_NAME = "suggestions";
  private static final String TERM_NODE_NAME = "term";
  private static final String WEIGHT_NODE_NAME = "weight";
  private static final String PAYLOAD_NODE_NAME = "payload";

  private final Map<String, List<Suggestion>> suggestionsPerDictionary = new LinkedHashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SuggesterResponse(NamedList<NamedList<Object>> suggestInfo) {
    for (int i = 0 ; i < suggestInfo.size(); i++) {
      final String outerName = suggestInfo.getName(i);
      final NamedList<Object> outerValue = suggestInfo.getVal(i);

      SimpleOrderedMap suggestionsNode = (SimpleOrderedMap) outerValue.getVal(0);
      List<SimpleOrderedMap> suggestionListToParse;
      List<Suggestion> suggestionList = new LinkedList<>();
      if (suggestionsNode != null) {

        suggestionListToParse = (List<SimpleOrderedMap>) suggestionsNode.get(SUGGESTIONS_NODE_NAME);
        for (SimpleOrderedMap suggestion : suggestionListToParse) {
          String term = (String) suggestion.get(TERM_NODE_NAME);
          long weight = (long) suggestion.get(WEIGHT_NODE_NAME);
          String payload = (String) suggestion.get(PAYLOAD_NODE_NAME);

          Suggestion parsedSuggestion = new Suggestion(term, weight, payload);
          suggestionList.add(parsedSuggestion);
        }
        suggestionsPerDictionary.put(outerName, suggestionList);
      }
    }
  }

  /**
   * get the suggestions provided by each
   *
   * @return a Map dictionary name : List of Suggestion
   */
  public Map<String, List<Suggestion>> getSuggestions() {
    return suggestionsPerDictionary;
  }

  /**
   * This getter is lazily initialized and returns a simplified map dictionary : List of suggested terms
   * This is useful for simple use cases when you simply need the suggested terms and no weight or payload
   *
   * @return a Map dictionary name : List of suggested terms
   */
  public Map<String, List<String>> getSuggestedTerms() {
    Map<String, List<String>> suggestedTermsPerDictionary = new LinkedHashMap<>();
    for (Map.Entry<String, List<Suggestion>> entry : suggestionsPerDictionary.entrySet()) {
      List<Suggestion> suggestions = entry.getValue();
      List<String> suggestionTerms = new LinkedList<String>();
      for (Suggestion s : suggestions) {
        suggestionTerms.add(s.getTerm());
      }
      suggestedTermsPerDictionary.put(entry.getKey(), suggestionTerms);
    }
    return suggestedTermsPerDictionary;
  }
}
