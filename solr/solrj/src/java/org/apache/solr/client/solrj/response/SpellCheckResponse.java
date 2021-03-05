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
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates responses from SpellCheckComponent
 *
 *
 * @since solr 1.3
 */
public class SpellCheckResponse {
  private boolean correctlySpelled;
  private List<Collation> collations;
  private List<Suggestion> suggestions = new ArrayList<>();
  Map<String, Suggestion> suggestionMap = new LinkedHashMap<>();

  public SpellCheckResponse(NamedList<Object> spellInfo) {
    @SuppressWarnings("unchecked")
    NamedList<Object> sugg = (NamedList<Object>) spellInfo.get("suggestions");
    if (sugg == null) {
      correctlySpelled = true;
      return;
    }
    for (int i = 0; i < sugg.size(); i++) {
      String n = sugg.getName(i);
      @SuppressWarnings("unchecked")
      Suggestion s = new Suggestion(n, (NamedList<Object>) sugg.getVal(i));
      suggestionMap.put(n, s);
      suggestions.add(s);
    }
    
    Boolean correctlySpelled = (Boolean) spellInfo.get("correctlySpelled");
    if (correctlySpelled != null) {
      this.correctlySpelled = correctlySpelled;
    }
    
    @SuppressWarnings("unchecked")
    NamedList<Object> coll = (NamedList<Object>) spellInfo.get("collations");
    if (coll != null) {
      // The 'collationInternalRank' values are ignored so we only care 'collation's.
      List<Object> collationInfo = coll.getAll("collation");
      collations = new ArrayList<>(collationInfo.size());
      for (Object o : collationInfo) {
        if (o instanceof String) {
          collations.add(new Collation()
              .setCollationQueryString((String) o));
        } else if (o instanceof NamedList) {
          @SuppressWarnings("unchecked")
          NamedList<Object> expandedCollation = (NamedList<Object>) o;
          String collationQuery
            = (String) expandedCollation.get("collationQuery");
          long hits = ((Number) expandedCollation.get("hits")).longValue();
          @SuppressWarnings("unchecked")
          NamedList<String> misspellingsAndCorrections
            = (NamedList<String>) expandedCollation.get("misspellingsAndCorrections");

          Collation collation = new Collation();
          collation.setCollationQueryString(collationQuery);
          collation.setNumberOfHits(hits);

          for (int ii = 0; ii < misspellingsAndCorrections.size(); ii++) {
            String misspelling = misspellingsAndCorrections.getName(ii);
            String correction = misspellingsAndCorrections.getVal(ii);
            collation.addMisspellingsAndCorrection(new Correction(
                misspelling, correction));
          }
          collations.add(collation);
        } else {
          throw new AssertionError(
              "Should get Lists of Strings or List of NamedLists here.");
        }
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

  /**
   * <p>
   *  Return the first collated query string.  For convenience and backwards-compatibility.  Use getCollatedResults() for full data.
   * </p>
   * @return first collated query string
   */
  public String getCollatedResult() {
    return collations==null || collations.size()==0 ? null : collations.get(0).collationQueryString;
  }
  
  /**
   * <p>
   *  Return all collations.  
   *  Will include # of hits and misspelling-to-correction details if "spellcheck.collateExtendedResults was true.
   * </p>
   * @return all collations
   */
  public List<Collation> getCollatedResults() {
    return collations;
  }

  public static class Suggestion {
    private String token;
    private int numFound;
    private int startOffset;
    private int endOffset;
    private int originalFrequency;
    private List<String> alternatives = new ArrayList<>();
    private List<Integer> alternativeFrequencies;

    @SuppressWarnings({"rawtypes"})
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
          @SuppressWarnings("unchecked")
          List list = (List)suggestion.getVal(i);
          if (list.size() > 0 && list.get(0) instanceof NamedList) {
            // extended results detected
            @SuppressWarnings("unchecked")
            List<NamedList> extended = (List<NamedList>)list;
            alternativeFrequencies = new ArrayList<>();
            for (NamedList nl : extended) {
              alternatives.add((String)nl.get("word"));
              alternativeFrequencies.add((Integer)nl.get("freq"));
            }
          } else {
            @SuppressWarnings("unchecked")
            List<String> alts = (List<String>) list;
            alternatives.addAll(alts);
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

  }

  public static class Collation {
    private String collationQueryString;
    private List<Correction> misspellingsAndCorrections = new ArrayList<>();
    private long numberOfHits;

    public long getNumberOfHits() {
      return numberOfHits;
    }

    public void setNumberOfHits(long numberOfHits) {
      this.numberOfHits = numberOfHits;
    }

    public String getCollationQueryString() {
      return collationQueryString;
    }

    public Collation setCollationQueryString(String collationQueryString) {
      this.collationQueryString = collationQueryString;
      return this;
    }

    public List<Correction> getMisspellingsAndCorrections() {
      return misspellingsAndCorrections;
    }

    public Collation addMisspellingsAndCorrection(Correction correction) {
      this.misspellingsAndCorrections.add(correction);
      return this;
    }

  }

  public static class Correction {
    private String original;
    private String correction;

    public Correction(String original, String correction) {
      this.original = original;
      this.correction = correction;
    }

    public String getOriginal() {
      return original;
    }

    public void setOriginal(String original) {
      this.original = original;
    }

    public String getCorrection() {
      return correction;
    }

    public void setCorrection(String correction) {
      this.correction = correction;
    }
  }
}
