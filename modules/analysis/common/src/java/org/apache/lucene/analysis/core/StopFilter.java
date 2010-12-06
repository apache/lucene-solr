package org.apache.lucene.analysis.core;

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
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;

/**
 * Removes stop words from a token stream.
 * 
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating StopFilter:
 * <ul>
 *   <li> As of 3.1, StopFilter correctly handles Unicode 4.0
 *         supplementary characters in stopwords and position
 *         increments are preserved
 * </ul>
 */
public final class StopFilter extends TokenFilter {

  private final CharArraySet stopWords;
  private boolean enablePositionIncrements = true;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);


  /**
   * Construct a token stream filtering the given input. If
   * <code>stopWords</code> is an instance of {@link CharArraySet} (true if
   * <code>makeStopSet()</code> was used to construct the set) it will be
   * directly used and <code>ignoreCase</code> will be ignored since
   * <code>CharArraySet</code> directly controls case sensitivity.
   * <p/>
   * If <code>stopWords</code> is not an instance of {@link CharArraySet}, a new
   * CharArraySet will be constructed and <code>ignoreCase</code> will be used
   * to specify the case sensitivity of that set.
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the stop
   *          set if Version > 3.0. See <a href="#version">above</a> for details.
   * @param input
   *          Input TokenStream
   * @param stopWords
   *          A Set of Strings or char[] or any other toString()-able set
   *          representing the stopwords
   * @param ignoreCase
   *          if true, all words are lower cased first
   */
  public StopFilter(Version matchVersion, TokenStream input, Set<?> stopWords, boolean ignoreCase)
  {
    super(input);
    this.stopWords = stopWords instanceof CharArraySet ? (CharArraySet) stopWords : new CharArraySet(matchVersion, stopWords, ignoreCase);
  }
  
  /**
   * Constructs a filter which removes words from the input TokenStream that are
   * named in the Set.
   * 
   * @param matchVersion
   *          Lucene version to enable correct Unicode 4.0 behavior in the stop
   *          set if Version > 3.0.  See <a href="#version">above</a> for details.
   * @param in
   *          Input stream
   * @param stopWords
   *          A Set of Strings or char[] or any other toString()-able set
   *          representing the stopwords
   * @see #makeStopSet(Version, java.lang.String...)
   */
  public StopFilter(Version matchVersion, TokenStream in, Set<?> stopWords) {
    this(matchVersion, in, stopWords, false);
  }

  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   * 
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords An array of stopwords
   * @see #makeStopSet(Version, java.lang.String[], boolean) passing false to ignoreCase
   */
  public static Set<Object> makeStopSet(Version matchVersion, String... stopWords) {
    return makeStopSet(matchVersion, stopWords, false);
  }
  
  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   * 
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the stopwords
   * @return A Set ({@link CharArraySet}) containing the words
   * @see #makeStopSet(Version, java.lang.String[], boolean) passing false to ignoreCase
   */
  public static Set<Object> makeStopSet(Version matchVersion, List<?> stopWords) {
    return makeStopSet(matchVersion, stopWords, false);
  }
    
  /**
   * Creates a stopword set from the given stopword array.
   * 
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords An array of stopwords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   */    
  public static Set<Object> makeStopSet(Version matchVersion, String[] stopWords, boolean ignoreCase) {
    CharArraySet stopSet = new CharArraySet(matchVersion, stopWords.length, ignoreCase);
    stopSet.addAll(Arrays.asList(stopWords));
    return stopSet;
  }
  
  /**
   * Creates a stopword set from the given stopword list.
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @return A Set ({@link CharArraySet}) containing the words
   */
  public static Set<Object> makeStopSet(Version matchVersion, List<?> stopWords, boolean ignoreCase){
    CharArraySet stopSet = new CharArraySet(matchVersion, stopWords.size(), ignoreCase);
    stopSet.addAll(stopWords);
    return stopSet;
  }
  
  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  @Override
  public final boolean incrementToken() throws IOException {
    // return the first non-stop word found
    int skippedPositions = 0;
    while (input.incrementToken()) {
      if (!stopWords.contains(termAtt.buffer(), 0, termAtt.length())) {
        if (enablePositionIncrements) {
          posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
        }
        return true;
      }
      skippedPositions += posIncrAtt.getPositionIncrement();
    }
    // reached EOS -- return false
    return false;
  }

  /**
   * @see #setEnablePositionIncrements(boolean)
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
   * Default is true.
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
