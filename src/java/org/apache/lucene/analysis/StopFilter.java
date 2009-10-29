package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.List;

import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.queryParser.QueryParser; // for javadoc
import org.apache.lucene.util.Version;

/**
 * Removes stop words from a token stream.
 */

public final class StopFilter extends TokenFilter {

  // deprecated
  private static boolean ENABLE_POSITION_INCREMENTS_DEFAULT = false;

  private final CharArraySet stopWords;
  private boolean enablePositionIncrements = ENABLE_POSITION_INCREMENTS_DEFAULT;

  private TermAttribute termAtt;
  private PositionIncrementAttribute posIncrAtt;
  
  /**
   * Construct a token stream filtering the given input.
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, String[])} instead
   */
  public StopFilter(TokenStream input, String [] stopWords)
  {
    this(ENABLE_POSITION_INCREMENTS_DEFAULT, input, stopWords, false);
  }

  /**
   * Construct a token stream filtering the given input.
   * @param enablePositionIncrements true if token positions should record the removed stop words
   * @param input input TokenStream
   * @param stopWords array of stop words
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, Set)} instead.
   */
  public StopFilter(boolean enablePositionIncrements, TokenStream input, String [] stopWords)
  {
    this(enablePositionIncrements, input, stopWords, false);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the array of words.
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, String[], boolean)} instead
   */
  public StopFilter(TokenStream in, String[] stopWords, boolean ignoreCase) {
    this(ENABLE_POSITION_INCREMENTS_DEFAULT, in, stopWords, ignoreCase);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the array of words.
   * @param enablePositionIncrements true if token positions should record the removed stop words
   * @param in input TokenStream
   * @param stopWords array of stop words
   * @param ignoreCase true if case is ignored
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, Set, boolean)} instead.
   */
  public StopFilter(boolean enablePositionIncrements, TokenStream in, String[] stopWords, boolean ignoreCase) {
    super(in);
    this.stopWords = (CharArraySet)makeStopSet(stopWords, ignoreCase);
    this.enablePositionIncrements = enablePositionIncrements;
    init();
  }


  /**
   * Construct a token stream filtering the given input.
   * If <code>stopWords</code> is an instance of {@link CharArraySet} (true if
   * <code>makeStopSet()</code> was used to construct the set) it will be directly used
   * and <code>ignoreCase</code> will be ignored since <code>CharArraySet</code>
   * directly controls case sensitivity.
   * <p/>
   * If <code>stopWords</code> is not an instance of {@link CharArraySet},
   * a new CharArraySet will be constructed and <code>ignoreCase</code> will be
   * used to specify the case sensitivity of that set.
   *
   * @param input
   * @param stopWords The set of Stop Words.
   * @param ignoreCase -Ignore case when stopping.
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, Set, boolean)} instead
   */
  public StopFilter(TokenStream input, Set stopWords, boolean ignoreCase)
  {
    this(ENABLE_POSITION_INCREMENTS_DEFAULT, input, stopWords, ignoreCase);
  }

  /**
   * Construct a token stream filtering the given input.
   * If <code>stopWords</code> is an instance of {@link CharArraySet} (true if
   * <code>makeStopSet()</code> was used to construct the set) it will be directly used
   * and <code>ignoreCase</code> will be ignored since <code>CharArraySet</code>
   * directly controls case sensitivity.
   * <p/>
   * If <code>stopWords</code> is not an instance of {@link CharArraySet},
   * a new CharArraySet will be constructed and <code>ignoreCase</code> will be
   * used to specify the case sensitivity of that set.
   *
   * @param enablePositionIncrements true if token positions should record the removed stop words
   * @param input Input TokenStream
   * @param stopWords The set of Stop Words.
   * @param ignoreCase -Ignore case when stopping.
   */
  public StopFilter(boolean enablePositionIncrements, TokenStream input, Set stopWords, boolean ignoreCase)
  {
    super(input);
    if (stopWords instanceof CharArraySet) {
      this.stopWords = (CharArraySet)stopWords;
    } else {
      this.stopWords = new CharArraySet(stopWords.size(), ignoreCase);
      this.stopWords.addAll(stopWords);
    }
    this.enablePositionIncrements = enablePositionIncrements;
    init();
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Set.
   *
   * @see #makeStopSet(java.lang.String[])
   * @deprecated Use {@link #StopFilter(boolean, TokenStream, Set)} instead
   */
  public StopFilter(TokenStream in, Set stopWords) {
    this(ENABLE_POSITION_INCREMENTS_DEFAULT, in, stopWords, false);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Set.
   *
   * @param enablePositionIncrements true if token positions should record the removed stop words
   * @param in Input stream
   * @param stopWords The set of Stop Words.
   * @see #makeStopSet(java.lang.String[])
   */
  public StopFilter(boolean enablePositionIncrements, TokenStream in, Set stopWords) {
    this(enablePositionIncrements, in, stopWords, false);
  }
  
  public void init() {
    termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    posIncrAtt = (PositionIncrementAttribute) addAttribute(PositionIncrementAttribute.class);
  }

  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   * 
   * @see #makeStopSet(java.lang.String[], boolean) passing false to ignoreCase
   */
  public static final Set makeStopSet(String[] stopWords) {
    return makeStopSet(stopWords, false);
  }

  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   *
   * @see #makeStopSet(java.lang.String[], boolean) passing false to ignoreCase
   */
  public static final Set makeStopSet(List/*<String>*/ stopWords) {
    return makeStopSet(stopWords, false);
  }
    
  /**
   * 
   * @param stopWords An array of stopwords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   */    
  public static final Set makeStopSet(String[] stopWords, boolean ignoreCase) {
    CharArraySet stopSet = new CharArraySet(stopWords.length, ignoreCase);
    stopSet.addAll(Arrays.asList(stopWords));
    return stopSet;
  }

  /**
   *
   * @param stopWords A List of Strings representing the stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @return A Set containing the words
   */
  public static final Set makeStopSet(List/*<String>*/ stopWords, boolean ignoreCase){
    CharArraySet stopSet = new CharArraySet(stopWords.size(), ignoreCase);
    stopSet.addAll(stopWords);
    return stopSet;
  }
  
  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  public final boolean incrementToken() throws IOException {
    // return the first non-stop word found
    int skippedPositions = 0;
    while (input.incrementToken()) {
      if (!stopWords.contains(termAtt.termBuffer(), 0, termAtt.termLength())) {
        if (enablePositionIncrements) {
          posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
        }
        return true;
      }
      skippedPositions += posIncrAtt.getPositionIncrement();
    }
    // reached EOS -- return null
    return false;
  }

  /**
   * @see #setEnablePositionIncrementsDefault(boolean). 
   * @deprecated Please specify this when you create the StopFilter
   */
  public static boolean getEnablePositionIncrementsDefault() {
    return ENABLE_POSITION_INCREMENTS_DEFAULT;
  }

  /**
   * Returns version-dependent default for
   * enablePositionIncrements.  Analyzers that embed
   * StopFilter use this method when creating the
   * StopFilter.  Prior to 2.9, this returns {@link #getEnablePositionIncrementsDefault}.
   * On 2.9 or later, it returns true.
   */
  public static boolean getEnablePositionIncrementsVersionDefault(Version matchVersion) {
    if (matchVersion.onOrAfter(Version.LUCENE_29)) {
      return true;
    } else {
      return ENABLE_POSITION_INCREMENTS_DEFAULT;
    }
  }

  /**
   * Set the default position increments behavior of every StopFilter created from now on.
   * <p>
   * Note: behavior of a single StopFilter instance can be modified 
   * with {@link #setEnablePositionIncrements(boolean)}.
   * This static method allows control over behavior of classes using StopFilters internally, 
   * for example {@link org.apache.lucene.analysis.standard.StandardAnalyzer StandardAnalyzer}
   * if used with the no-arg ctor.
   * <p>
   * Default : false.
   * @see #setEnablePositionIncrements(boolean).
   * @deprecated Please specify this when you create the StopFilter
   */
  public static void setEnablePositionIncrementsDefault(boolean defaultValue) {
    ENABLE_POSITION_INCREMENTS_DEFAULT = defaultValue;
  }

  /**
   * @see #setEnablePositionIncrements(boolean). 
   */
  public boolean getEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  /**
   * If <code>true</code>, this StopFilter will preserve
   * positions of the incoming tokens (ie, accumulate and
   * set position increments of the removed stop tokens).
   * Generally, <code>true</code> is best as it does not
   * lose information (positions of the original tokens)
   * during indexing.
   * 
   * <p> When set, when a token is stopped
   * (omitted), the position increment of the following
   * token is incremented.
   *
   * <p> <b>NOTE</b>: be sure to also
   * set {@link QueryParser#setEnablePositionIncrements} if
   * you use QueryParser to create queries.
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }
}
