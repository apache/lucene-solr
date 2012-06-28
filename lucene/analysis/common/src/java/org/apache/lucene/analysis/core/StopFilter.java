package org.apache.lucene.analysis.core;

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

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
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
public final class StopFilter extends FilteringTokenFilter {

  private final CharArraySet stopWords;
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  
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
   *          A {@link CharArraySet} representing the stopwords.
   * @see #makeStopSet(Version, java.lang.String...)
   */
  public StopFilter(Version matchVersion, TokenStream in, CharArraySet stopWords) {
    super(true, in);
    this.stopWords = stopWords;
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
  public static CharArraySet makeStopSet(Version matchVersion, String... stopWords) {
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
  public static CharArraySet makeStopSet(Version matchVersion, List<?> stopWords) {
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
  public static CharArraySet makeStopSet(Version matchVersion, String[] stopWords, boolean ignoreCase) {
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
  public static CharArraySet makeStopSet(Version matchVersion, List<?> stopWords, boolean ignoreCase){
    CharArraySet stopSet = new CharArraySet(matchVersion, stopWords.size(), ignoreCase);
    stopSet.addAll(stopWords);
    return stopSet;
  }
  
  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  @Override
  protected boolean accept() {
    return !stopWords.contains(termAtt.buffer(), 0, termAtt.length());
  }

}
