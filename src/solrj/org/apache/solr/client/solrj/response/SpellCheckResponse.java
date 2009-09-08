package org.apache.solr.client.solrj.response;
/**
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

import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates responses from SpellCheckComponent
 *
 * @version $Id$
 * @since solr 1.3
 */
public class SpellCheckResponse {
  private boolean correctlySpelled;
  private String collation;
  private List<Suggestion> suggestions = new ArrayList<Suggestion>();
  Map<String, Suggestion> suggestionMap = new LinkedHashMap<String, Suggestion>();

  public SpellCheckResponse(NamedList<Object> spellInfo) {
    NamedList<Object> sugg = (NamedList<Object>) spellInfo.get("suggestions");
    if (sugg == null) {
      correctlySpelled = true;
      return;
    }
    for (int i = 0; i < sugg.size(); i++) {
      String n = sugg.getName(i);
      if ("correctlySpelled".equals(n)) {
        correctlySpelled = (Boolean) sugg.getVal(i);
      } else if ("collation".equals(n)) {
        collation = (String) sugg.getVal(i);
      } else {
        Suggestion s = new Suggestion(n, (NamedList<Object>) sugg.getVal(i));
        suggestionMap.put(n, s);
        suggestions.add(s);
      }
    }
  }

  public boolean isCorrectlySpelled() {
    return correctlySpelled;
  }

  public List<Suggestion> getSuggestions() {
    return suggestions;
  }

  public Map<String, Suggestion> getSuggestionMap() {
    return suggestionMap;
  }

  public Suggestion getSuggestion(String token) {
    return suggestionMap.get(token);
  }

  public String getFirstSuggestion(String token) {
    Suggestion s = suggestionMap.get(token);
    if (s==null || s.getAlternatives().isEmpty()) return null;
    return s.getAlternatives().get(0);
  }

  public String getCollatedResult() {
    return collation;
  }

  public static class Suggestion {
    private String token;
    private int numFound;
    private int startOffset;
    private int endOffset;
    private int originalFrequency;
    private List<String> alternatives = new ArrayList<String>();
    private List<Integer> alternativeFrequencies;

    public Suggestion(String token, NamedList<Object> suggestion) {
      this.token = token;
      for (int i = 0; i < suggestion.size(); i++) {
        String n = suggestion.getName(i);

        if ("numFound".equals(n)) {
          numFound = (Integer) suggestion.getVal(i);
        } else if ("startOffset".equals(n)) {
          startOffset = (Integer) suggestion.getVal(i);
        } else if ("endOffset".equals(n)) {
          endOffset = (Integer) suggestion.getVal(i);
        } else if ("origFreq".equals(n)) {
          originalFrequency = (Integer) suggestion.getVal(i);
        } else if ("suggestion".equals(n)) {
          List list = (List)suggestion.getVal(i);
          if (list.size() > 0 && list.get(0) instanceof NamedList) {
            // extended results detected
            alternativeFrequencies = new ArrayList<Integer>();
            for (NamedList nl : (List<NamedList>)list) {
              alternatives.add((String)nl.get("word"));
              alternativeFrequencies.add((Integer)nl.get("freq"));
            }
          } else {
            alternatives.addAll(list);
          }
        }
      }
    }

    public String getToken() {
      return token;
    }

    public int getNumFound() {
      return numFound;
    }

    public int getStartOffset() {
      return startOffset;
    }

    public int getEndOffset() {
      return endOffset;
    }

    public int getOriginalFrequency() {
      return originalFrequency;
    }

    /** The list of alternatives */
    public List<String> getAlternatives() {
      return alternatives;
    }

    /** The frequencies of the alternatives in the corpus, or null if this information was not returned */
    public List<Integer> getAlternativeFrequencies() {
      return alternativeFrequencies;
    }

    @Deprecated
    /** @see #getAlternatives */
    public List<String> getSuggestions() {
      return alternatives;
    }

    @Deprecated
    /** @see #getAlternativeFrequencies */
    public List<Integer> getSuggestionFrequencies() {
      return alternativeFrequencies;
    }

  }
}
