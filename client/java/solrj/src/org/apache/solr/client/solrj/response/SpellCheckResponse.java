package org.apache.solr.client.solrj.response;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;

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
    if (s != null) {
      return s.getSuggestions().isEmpty() ? null : s.getSuggestions().get(0);
    }
    return null;
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
    private List<String> suggestions = new ArrayList<String>();
    private List<Integer> suggestionFrequencies = new ArrayList<Integer>();

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
          Object o = suggestion.getVal(i);
          if (o instanceof List) {
            List<String> list = (List<String>) o;
            suggestions.addAll(list);
          } else if (o instanceof SimpleOrderedMap) {
            SimpleOrderedMap map = (SimpleOrderedMap) o;
            suggestions.add((String) map.get("word"));
            suggestionFrequencies.add((Integer) map.get("frequency"));
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

    public List<String> getSuggestions() {
      return suggestions;
    }

    public List<Integer> getSuggestionFrequencies() {
      return suggestionFrequencies;
    }
  }
}
