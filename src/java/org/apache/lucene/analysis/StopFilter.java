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

/**
 * Removes stop words from a token stream.
 */

public final class StopFilter extends TokenFilter {

  private static boolean ENABLE_POSITION_INCREMENTS_DEFAULT = false;

  private final CharArraySet stopWords;
  private boolean enablePositionIncrements = ENABLE_POSITION_INCREMENTS_DEFAULT;

  /**
   * Construct a token stream filtering the given input.
   */
  public StopFilter(TokenStream input, String [] stopWords)
  {
    this(input, stopWords, false);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the array of words.
   */
  public StopFilter(TokenStream in, String[] stopWords, boolean ignoreCase) {
    super(in);
    this.stopWords = (CharArraySet)makeStopSet(stopWords, ignoreCase);
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
   */
  public StopFilter(TokenStream input, Set stopWords, boolean ignoreCase)
  {
    super(input);
    if (stopWords instanceof CharArraySet) {
      this.stopWords = (CharArraySet)stopWords;
    } else {
      this.stopWords = new CharArraySet(stopWords.size(), ignoreCase);
      this.stopWords.addAll(stopWords);
    }
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Set.
   *
   * @see #makeStopSet(java.lang.String[])
   */
  public StopFilter(TokenStream in, Set stopWords) {
    this(in, stopWords, false);
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
   * 
   * @param stopWords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   */    
  public static final Set makeStopSet(String[] stopWords, boolean ignoreCase) {
    CharArraySet stopSet = new CharArraySet(stopWords.length, ignoreCase);
    stopSet.addAll(Arrays.asList(stopWords));
    return stopSet;
  }

  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  public final Token next(final Token reusableToken) throws IOException {
    assert reusableToken != null;
    // return the first non-stop word found
    int skippedPositions = 0;
    for (Token nextToken = input.next(reusableToken); nextToken != null; nextToken = input.next(reusableToken)) {
      if (!stopWords.contains(nextToken.termBuffer(), 0, nextToken.termLength())) {
        if (enablePositionIncrements) {
          nextToken.setPositionIncrement(nextToken.getPositionIncrement() + skippedPositions);
        }
        return nextToken;
      }
      skippedPositions += nextToken.getPositionIncrement();
    }
    // reached EOS -- return null
    return null;
  }

  /**
   * @see #setEnablePositionIncrementsDefault(boolean). 
   */
  public static boolean getEnablePositionIncrementsDefault() {
    return ENABLE_POSITION_INCREMENTS_DEFAULT;
  }

  /**
   * Set the default position increments behavior of every StopFilter created from now on.
   * <p>
   * Note: behavior of a single StopFilter instance can be modified 
   * with {@link #setEnablePositionIncrements(boolean)}.
   * This static method allows control over behavior of classes using StopFilters internally, 
   * for example {@link org.apache.lucene.analysis.standard.StandardAnalyzer StandardAnalyzer}. 
   * <p>
   * Default : false.
   * @see #setEnablePositionIncrements(boolean).
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
   * Set to <code>true</code> to make <b>this</b> StopFilter enable position increments to result tokens.
   * <p>
   * When set, when a token is stopped (omitted), the position increment of 
   * the following token is incremented.  
   * <p>
   * Default: see {@link #setEnablePositionIncrementsDefault(boolean)}.
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }
}
