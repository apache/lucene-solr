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
  private boolean enablePositionIncrements = false;

  private TermAttribute termAtt;
  private PositionIncrementAttribute posIncrAtt;

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
   * @param stopWords A Set of Strings or char[] or any other toString()-able set representing the stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @deprecated use {@link #StopFilter(Version, TokenStream, Set, boolean)} instead
   */
  @Deprecated
  public StopFilter(boolean enablePositionIncrements, TokenStream input, Set<?> stopWords, boolean ignoreCase)
  {
    this(Version.LUCENE_30, enablePositionIncrements, input, stopWords, ignoreCase);
  }
  
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
   this(matchVersion, matchVersion.onOrAfter(Version.LUCENE_29), input, stopWords, ignoreCase);
  }
  
  /*
   * convenience ctor to enable deprecated ctors to set posInc explicitly
   */
  private StopFilter(Version matchVersion, boolean enablePositionIncrements, TokenStream input, Set<?> stopWords, boolean ignoreCase){
    super(input);
    this.stopWords = CharArraySet.unmodifiableSet(new CharArraySet(matchVersion, stopWords, ignoreCase));
    this.enablePositionIncrements = enablePositionIncrements;
    termAtt = addAttribute(TermAttribute.class);
    posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Set.
   *
   * @param enablePositionIncrements true if token positions should record the removed stop words
   * @param in Input stream
   * @param stopWords A Set of Strings or char[] or any other toString()-able set representing the stopwords
   * @see #makeStopSet(Version, java.lang.String[])
   * @deprecated use {@link #StopFilter(Version, TokenStream, Set)} instead
   */
  @Deprecated
  public StopFilter(boolean enablePositionIncrements, TokenStream in, Set<?> stopWords) {
    this(Version.LUCENE_CURRENT, enablePositionIncrements, in, stopWords, false);
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
   * @see #makeStopSet(Version, java.lang.String[])
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
   * @see #makeStopSet(Version, java.lang.String[], boolean) passing false to ignoreCase
   * @deprecated use {@link #makeStopSet(Version, String...)} instead
   */
  @Deprecated
  public static final Set<Object> makeStopSet(String... stopWords) {
    return makeStopSet(Version.LUCENE_30, stopWords, false);
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
  public static final Set<Object> makeStopSet(Version matchVersion, String... stopWords) {
    return makeStopSet(matchVersion, stopWords, false);
  }
  
  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the stopwords
   * @return A Set ({@link CharArraySet}) containing the words
   * @see #makeStopSet(Version, java.lang.String[], boolean) passing false to ignoreCase
   * @deprecated use {@link #makeStopSet(Version, List)} instead
   */
  @Deprecated
  public static final Set<Object> makeStopSet(List<?> stopWords) {
    return makeStopSet(Version.LUCENE_30, stopWords, false);
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
  public static final Set<Object> makeStopSet(Version matchVersion, List<?> stopWords) {
    return makeStopSet(matchVersion, stopWords, false);
  }
    
  /**
   * Creates a stopword set from the given stopword array.
   * @param stopWords An array of stopwords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   * @deprecated use {@link #makeStopSet(Version, String[], boolean)} instead;
   */  
  @Deprecated
  public static final Set<Object> makeStopSet(String[] stopWords, boolean ignoreCase) {
    return makeStopSet(Version.LUCENE_30, stopWords, ignoreCase);
  }
  /**
   * Creates a stopword set from the given stopword array.
   * 
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords An array of stopwords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   */    
  public static final Set<Object> makeStopSet(Version matchVersion, String[] stopWords, boolean ignoreCase) {
    CharArraySet stopSet = new CharArraySet(matchVersion, stopWords.length, ignoreCase);
    stopSet.addAll(Arrays.asList(stopWords));
    return stopSet;
  }
  
  /**
   * Creates a stopword set from the given stopword list.
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @return A Set ({@link CharArraySet}) containing the words
   * @deprecated use {@link #makeStopSet(Version, List, boolean)} instead
   */
  @Deprecated
  public static final Set<Object> makeStopSet(List<?> stopWords, boolean ignoreCase){
    return makeStopSet(Version.LUCENE_30, stopWords, ignoreCase);
  }

  /**
   * Creates a stopword set from the given stopword list.
   * @param matchVersion Lucene version to enable correct Unicode 4.0 behavior in the returned set if Version > 3.0
   * @param stopWords A List of Strings or char[] or any other toString()-able list representing the stopwords
   * @param ignoreCase if true, all words are lower cased first
   * @return A Set ({@link CharArraySet}) containing the words
   */
  public static final Set<Object> makeStopSet(Version matchVersion, List<?> stopWords, boolean ignoreCase){
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
      if (!stopWords.contains(termAtt.termBuffer(), 0, termAtt.termLength())) {
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
   * Returns version-dependent default for
   * enablePositionIncrements.  Analyzers that embed
   * StopFilter use this method when creating the
   * StopFilter.  Prior to 2.9, this returns false.  On 2.9
   * or later, it returns true.
   * @deprecated use {@link #StopFilter(Version, TokenStream, Set)} instead
   */
  @Deprecated
  public static boolean getEnablePositionIncrementsVersionDefault(Version matchVersion) {
    return matchVersion.onOrAfter(Version.LUCENE_29);
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
