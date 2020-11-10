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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.spell.CombineSuggestion;
import org.apache.lucene.search.spell.SuggestWord;
import org.apache.lucene.search.spell.WordBreakSpellChecker;
import org.apache.lucene.search.spell.WordBreakSpellChecker.BreakSuggestionSortMethod;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * <p>
 * A spellchecker that breaks and combines words.  
 * </p>
 * <p>
 * This will not combine adjacent tokens that do not have 
 * the same required status (prohibited, required, optional).  
 * However, this feature depends on incoming term flags 
 * being properly set. ({@link QueryConverter#PROHIBITED_TERM_FLAG},
 * {@link QueryConverter#REQUIRED_TERM_FLAG}, 
 * {@link QueryConverter#TERM_IN_BOOLEAN_QUERY_FLAG}, and
 * {@link QueryConverter#TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG} )
 * This feature breaks completely if the upstream analyzer or query
 * converter sets flags with the same values but different meanings.
 * The default query converter (if not using "spellcheck.q") 
 * is {@link SpellingQueryConverter}, which properly sets these flags.
 * </p>
 */
public class WordBreakSolrSpellChecker extends SolrSpellChecker {
  /**
   * <p>
   * Try to combine multiple words into one? [true|false]
   * </p>
   */
  public static final String PARAM_COMBINE_WORDS = "combineWords";
  /**
   * <p>
   * Try to break words into multiples? [true|false]
   * </p>
   */
  public static final String PARAM_BREAK_WORDS = "breakWords";
  /**
   * See {@link WordBreakSpellChecker#setMaxChanges}
   */
  public static final String PARAM_MAX_CHANGES = "maxChanges";
  /**
   * See {@link WordBreakSpellChecker#setMaxCombineWordLength}
   */
  public static final String PARAM_MAX_COMBINE_WORD_LENGTH = "maxCombinedLength";
  /**
   * See {@link WordBreakSpellChecker#setMinBreakWordLength}
   */
  public static final String PARAM_MIN_BREAK_WORD_LENGTH = "minBreakLength";
  /**
   * See {@link BreakSuggestionTieBreaker} for options.
   */
  public static final String PARAM_BREAK_SUGGESTION_TIE_BREAKER = "breakSugestionTieBreaker";
  /**
   * See {@link WordBreakSpellChecker#setMaxEvaluations}
   */
  public static final String PARAM_MAX_EVALUATIONS = "maxEvaluations";
  /**
   * See {@link WordBreakSpellChecker#setMinSuggestionFrequency}
   */
  public static final String PARAM_MIN_SUGGESTION_FREQUENCY = "minSuggestionFreq";
  
  /**
   * <p>
   *  Specify a value on the "breakSugestionTieBreaker" parameter.
   *    The default is MAX_FREQ.
   * </p>  
   */
  public enum BreakSuggestionTieBreaker {
    /**
     * See
     * {@link BreakSuggestionSortMethod#NUM_CHANGES_THEN_MAX_FREQUENCY}
     * #
     */
    MAX_FREQ,
    /**
     * See
     * {@link BreakSuggestionSortMethod#NUM_CHANGES_THEN_SUMMED_FREQUENCY}
     */
    SUM_FREQ
  };
  
  private WordBreakSpellChecker wbsp = null;
  private boolean combineWords = false;
  private boolean breakWords = false;
  private BreakSuggestionSortMethod sortMethod = BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY;
  private static final Pattern spacePattern = Pattern.compile("\\s+");

  @Override
  public String init(@SuppressWarnings("rawtypes") NamedList config,
      SolrCore core) {
    String name = super.init(config, core);
    combineWords = boolParam(config, PARAM_COMBINE_WORDS);
    breakWords = boolParam(config, PARAM_BREAK_WORDS);
    wbsp = new WordBreakSpellChecker();
    String bstb = strParam(config, PARAM_BREAK_SUGGESTION_TIE_BREAKER);
    if (bstb != null) {
      bstb = bstb.toUpperCase(Locale.ROOT);
      if (bstb.equals(BreakSuggestionTieBreaker.SUM_FREQ.name())) {
        sortMethod = BreakSuggestionSortMethod.NUM_CHANGES_THEN_SUMMED_FREQUENCY;
      } else if (bstb.equals(BreakSuggestionTieBreaker.MAX_FREQ.name())) {
        sortMethod = BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY;
      } else {
        throw new IllegalArgumentException("Invalid value for parameter "
            + PARAM_BREAK_SUGGESTION_TIE_BREAKER + " : " + bstb);
      }
    } else {
      sortMethod = BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY;
    }
    int mc = intParam(config, PARAM_MAX_CHANGES);
    if (mc > 0) {
      wbsp.setMaxChanges(mc);
    }
    int mcl = intParam(config, PARAM_MAX_COMBINE_WORD_LENGTH);
    if (mcl > 0) {
      wbsp.setMaxCombineWordLength(mcl);
    }
    int mbwl = intParam(config, PARAM_MIN_BREAK_WORD_LENGTH);
    if (mbwl > 0) {
      wbsp.setMinBreakWordLength(mbwl);
    }
    int me = intParam(config, PARAM_MAX_EVALUATIONS);
    if (me > 0) {
      wbsp.setMaxEvaluations(me);
    }
    int msf = intParam(config, PARAM_MIN_SUGGESTION_FREQUENCY);
    if (msf > 0) {
      wbsp.setMinSuggestionFrequency(msf);
    }
    return name;
  }
  
  private String strParam(@SuppressWarnings("rawtypes") NamedList config,
      String paramName) {
    Object o = config.get(paramName);
    return o == null ? null : o.toString();
  }
  
  private boolean boolParam(@SuppressWarnings("rawtypes") NamedList config,
      String paramName) {
    String s = strParam(config, paramName);
    if ("true".equalsIgnoreCase(s) || "on".equalsIgnoreCase(s)) {
      return true;
    }
    return false;
  }
  
  private int intParam(@SuppressWarnings("rawtypes") NamedList config,
      String paramName) {
    Object o = config.get(paramName);
    if (o == null) {
      return 0;
    }
    try {
      return Integer.parseInt(o.toString());
    } catch (NumberFormatException nfe) {
      throw new IllegalArgumentException("Invalid integer for parameter "
          + paramName + " : " + o);
    }
  }
  
  @Override
  public SpellingResult getSuggestions(SpellingOptions options)
      throws IOException {
    IndexReader ir = options.reader;
    int numSuggestions = options.count;
    
    StringBuilder sb = new StringBuilder();
    Token[] tokenArr = options.tokens.toArray(new Token[options.tokens.size()]);
    List<Token> tokenArrWithSeparators = new ArrayList<>(options.tokens.size() + 2);
    List<Term> termArr = new ArrayList<>(options.tokens.size() + 2);
    List<ResultEntry> breakSuggestionList = new ArrayList<>();
    List<ResultEntry> noBreakSuggestionList = new ArrayList<>();
    boolean lastOneProhibited = false;
    boolean lastOneRequired = false;
    boolean lastOneprocedesNewBooleanOp = false;
    for (int i = 0; i < tokenArr.length; i++) {      
      boolean prohibited = 
        (tokenArr[i].getFlags() & QueryConverter.PROHIBITED_TERM_FLAG) == 
          QueryConverter.PROHIBITED_TERM_FLAG;
      boolean required = 
        (tokenArr[i].getFlags() & QueryConverter.REQUIRED_TERM_FLAG) == 
          QueryConverter.REQUIRED_TERM_FLAG;
      boolean procedesNewBooleanOp = 
        (tokenArr[i].getFlags() & QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG) == 
          QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG;
      if (i > 0
          && (prohibited != lastOneProhibited || required != lastOneRequired || lastOneprocedesNewBooleanOp)) {
        termArr.add(WordBreakSpellChecker.SEPARATOR_TERM);
        tokenArrWithSeparators.add(null);
      }
      lastOneProhibited = prohibited;
      lastOneRequired = required;
      lastOneprocedesNewBooleanOp = procedesNewBooleanOp;
      
      Term thisTerm = new Term(field, tokenArr[i].toString());
      termArr.add(thisTerm);
      tokenArrWithSeparators.add(tokenArr[i]);
      if (breakWords) {
        SuggestWord[][] breakSuggestions = wbsp.suggestWordBreaks(thisTerm,
            numSuggestions, ir, options.suggestMode, sortMethod);
        if(breakSuggestions.length==0) {
          noBreakSuggestionList.add(new ResultEntry(tokenArr[i], null, 0));
        }
        for (SuggestWord[] breakSuggestion : breakSuggestions) {
          sb.delete(0, sb.length());
          boolean firstOne = true;
          int freq = 0;
          for (SuggestWord word : breakSuggestion) {
            if (!firstOne) {
              sb.append(" ");
            }
            firstOne = false;
            sb.append(word.string);
            if (sortMethod == BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY) {
              freq = Math.max(freq, word.freq);
            } else {
              freq += word.freq;
            }
          }
          breakSuggestionList.add(new ResultEntry(tokenArr[i], sb.toString(),
              freq));
        }
      }
    }
    breakSuggestionList.addAll(noBreakSuggestionList);
    
    List<ResultEntry> combineSuggestionList = Collections.emptyList();
    CombineSuggestion[] combineSuggestions = wbsp.suggestWordCombinations(
        termArr.toArray(new Term[termArr.size()]), numSuggestions, ir, options.suggestMode);
    if (combineWords) {
      combineSuggestionList = new ArrayList<>(
          combineSuggestions.length);
      for (CombineSuggestion cs : combineSuggestions) {
        int firstTermIndex = cs.originalTermIndexes[0];
        int lastTermIndex = cs.originalTermIndexes[cs.originalTermIndexes.length - 1];
        sb.delete(0, sb.length());
        for (int i = firstTermIndex; i <= lastTermIndex; i++) {
          if (i > firstTermIndex) {
            sb.append(" ");
          }
          sb.append(tokenArrWithSeparators.get(i).toString());
        }
        Token token = new Token(sb.toString(), tokenArrWithSeparators.get(firstTermIndex)
            .startOffset(), tokenArrWithSeparators.get(lastTermIndex).endOffset());
        combineSuggestionList.add(new ResultEntry(token, cs.suggestion.string,
            cs.suggestion.freq));
      }
    }
    
    // Interleave the two lists of suggestions into one SpellingResult
    SpellingResult result = new SpellingResult();
    Iterator<ResultEntry> breakIter = breakSuggestionList.iterator();
    Iterator<ResultEntry> combineIter = combineSuggestionList.iterator();
    ResultEntry lastBreak = breakIter.hasNext() ? breakIter.next() : null;
    ResultEntry lastCombine = combineIter.hasNext() ? combineIter.next() : null;
    int breakCount = 0;
    int combineCount = 0;
    while (lastBreak != null || lastCombine != null) {
      if (lastBreak == null) {
        addToResult(result, lastCombine.token, getCombineFrequency(ir, lastCombine.token), lastCombine.suggestion, lastCombine.freq);
        lastCombine = null;
      } else if (lastCombine == null) {
        addToResult(result, lastBreak.token, ir.docFreq(new Term(field, lastBreak.token.toString())), lastBreak.suggestion, lastBreak.freq);
        lastBreak = null;
      } else if (lastBreak.freq < lastCombine.freq) {
        addToResult(result, lastCombine.token, getCombineFrequency(ir, lastCombine.token), lastCombine.suggestion, lastCombine.freq);        
        lastCombine = null;
      } else if (lastCombine.freq < lastBreak.freq) {
        addToResult(result, lastBreak.token, ir.docFreq(new Term(field, lastBreak.token.toString())), lastBreak.suggestion, lastBreak.freq);
        lastBreak = null;
      } else if (breakCount >= combineCount) { //TODO: Should reverse >= to < ??S
        addToResult(result, lastCombine.token, getCombineFrequency(ir, lastCombine.token), lastCombine.suggestion, lastCombine.freq);        
        lastCombine = null;
      } else {
        addToResult(result, lastBreak.token, ir.docFreq(new Term(field, lastBreak.token.toString())), lastBreak.suggestion, lastBreak.freq);        
        lastBreak = null;
      }
      if (lastBreak == null && breakIter.hasNext()) {
        lastBreak = breakIter.next();
        breakCount++;
      }
      if (lastCombine == null && combineIter.hasNext()) {
        lastCombine = combineIter.next();
        combineCount++;
      }
    }
    return result;
  }
  private void addToResult(SpellingResult result, Token token, int tokenFrequency, String suggestion, int suggestionFrequency) {
    if(suggestion==null) {
      result.add(token, Collections.<String>emptyList());
      result.addFrequency(token, tokenFrequency);
    } else {
      result.add(token, suggestion, suggestionFrequency);
      result.addFrequency(token, tokenFrequency);
    }
  }
  
  private int getCombineFrequency(IndexReader ir, Token token) throws IOException {
    String[] words = spacePattern.split(token.toString());
    int result = 0;
    if(sortMethod==BreakSuggestionSortMethod.NUM_CHANGES_THEN_MAX_FREQUENCY) {      
      for(String word : words) {
        result = Math.max(result, ir.docFreq(new Term(field, word)));
      }
    } else {
      for(String word : words) {
        result += ir.docFreq(new Term(field, word));
      }
    }
    return result;
  }
  
  @Override
  public void build(SolrCore core, SolrIndexSearcher searcher) {
  /* no-op */
  }
  
  @Override
  public void reload(SolrCore core, SolrIndexSearcher searcher)
      throws IOException {
  /* no-op */
  }
  
  @Override
  public boolean isSuggestionsMayOverlap() {
    return true;
  }
}
