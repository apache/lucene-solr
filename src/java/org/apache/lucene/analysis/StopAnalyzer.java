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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Set;

import org.apache.lucene.util.Version;

/** Filters {@link LetterTokenizer} with {@link
 * LowerCaseFilter} and {@link StopFilter}.
 *
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating StopAnalyzer:
 * <ul>
 *   <li> As of 2.9, position increments are preserved
 * </ul>
*/

public final class StopAnalyzer extends Analyzer {
  private final Set/*<String>*/ stopWords;
  // @deprecated
  private final boolean useDefaultStopPositionIncrement;
  private final boolean enablePositionIncrements;

  /** An array containing some common English words that are not usually useful
    for searching. 
    @deprecated Use {@link #ENGLISH_STOP_WORDS_SET} instead */
  public static final String[] ENGLISH_STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by",
    "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such",
    "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with"
  };
  
  /** An unmodifiable set containing some common English words that are not usually useful
  for searching.*/
  public static final Set/*<String>*/ ENGLISH_STOP_WORDS_SET;
  
  static {
	  final String[] stopWords = new String[]{
  	    "a", "an", "and", "are", "as", "at", "be", "but", "by",
	    "for", "if", "in", "into", "is", "it",
	    "no", "not", "of", "on", "or", "such",
	    "that", "the", "their", "then", "there", "these",
	    "they", "this", "to", "was", "will", "with"
	  };
	  final CharArraySet stopSet = new CharArraySet(stopWords.length, false);
    stopSet.addAll(Arrays.asList(stopWords));  
	  ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet); 
  }
  
  /** Builds an analyzer which removes words in
   * ENGLISH_STOP_WORDS.
   * @deprecated Use {@link #StopAnalyzer(Version)} instead */
  public StopAnalyzer() {
    stopWords = ENGLISH_STOP_WORDS_SET;
    useDefaultStopPositionIncrement = true;
    enablePositionIncrements = false;
  }

  /** Builds an analyzer which removes words in
   * ENGLISH_STOP_WORDS.*/
  public StopAnalyzer(Version matchVersion) {
    stopWords = ENGLISH_STOP_WORDS_SET;
    useDefaultStopPositionIncrement = false;
    enablePositionIncrements = StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion);
  }

  /** Builds an analyzer which removes words in
   *  ENGLISH_STOP_WORDS.
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} 
   * @deprecated Use {@link #StopAnalyzer(Version)} instead */
  public StopAnalyzer(boolean enablePositionIncrements) {
    stopWords = ENGLISH_STOP_WORDS_SET;
    this.enablePositionIncrements = enablePositionIncrements;
    useDefaultStopPositionIncrement = false;
  }

  /** Builds an analyzer with the stop words from the given set.
   * @deprecated Use {@link #StopAnalyzer(Version, Set)} instead */
  public StopAnalyzer(Set stopWords) {
    this.stopWords = stopWords;
    useDefaultStopPositionIncrement = true;
    enablePositionIncrements = false;
  }

  /** Builds an analyzer with the stop words from the given
   * set. */
  public StopAnalyzer(Version matchVersion, Set stopWords) {
    this.stopWords = stopWords;
    useDefaultStopPositionIncrement = false;
    enablePositionIncrements = StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion);
  }

  /** Builds an analyzer with the stop words from the given set.
   * @param stopWords Set of stop words
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements}
   * @deprecated Use {@link #StopAnalyzer(Version, Set)} instead */
  public StopAnalyzer(Set stopWords, boolean enablePositionIncrements) {
    this.stopWords = stopWords;
    this.enablePositionIncrements = enablePositionIncrements;
    useDefaultStopPositionIncrement = false;
  }

  /** Builds an analyzer which removes words in the provided array.
   * @deprecated Use {@link #StopAnalyzer(Version, Set)} instead */
  public StopAnalyzer(String[] stopWords) {
    this.stopWords = StopFilter.makeStopSet(stopWords);
    useDefaultStopPositionIncrement = true;
    enablePositionIncrements = false;
  }
  
  /** Builds an analyzer which removes words in the provided array.
   * @param stopWords Array of stop words
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} 
   * @deprecated Use {@link #StopAnalyzer(Version, Set)} instead*/
  public StopAnalyzer(String[] stopWords, boolean enablePositionIncrements) {
    this.stopWords = StopFilter.makeStopSet(stopWords);
    this.enablePositionIncrements = enablePositionIncrements;
    useDefaultStopPositionIncrement = false;
  }
  
  /** Builds an analyzer with the stop words from the given file.
   * @see WordlistLoader#getWordSet(File)
   * @deprecated Use {@link #StopAnalyzer(Version, File)} instead */
  public StopAnalyzer(File stopwordsFile) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwordsFile);
    useDefaultStopPositionIncrement = true;
    enablePositionIncrements = false;
  }

  /** Builds an analyzer with the stop words from the given file.
   * @see WordlistLoader#getWordSet(File)
   * @param stopwordsFile File to load stop words from
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements}
   * @deprecated Use {@link #StopAnalyzer(Version, File)} instead */
  public StopAnalyzer(File stopwordsFile, boolean enablePositionIncrements) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwordsFile);
    this.enablePositionIncrements = enablePositionIncrements;
    useDefaultStopPositionIncrement = false;
  }

  /** Builds an analyzer with the stop words from the given file.
   * @see WordlistLoader#getWordSet(File)
   * @param matchVersion See <a href="#version">above</a>
   * @param stopwordsFile File to load stop words from */
  public StopAnalyzer(Version matchVersion, File stopwordsFile) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwordsFile);
    this.enablePositionIncrements = StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion);
    useDefaultStopPositionIncrement = false;
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @deprecated Use {@link #StopAnalyzer(Version, Reader)} instead
   */
  public StopAnalyzer(Reader stopwords) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwords);
    useDefaultStopPositionIncrement = true;
    enablePositionIncrements = false;
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @param stopwords Reader to load stop words from
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements}
   * @deprecated Use {@link #StopAnalyzer(Version, Reader)} instead */
  public StopAnalyzer(Reader stopwords, boolean enablePositionIncrements) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwords);
    this.enablePositionIncrements = enablePositionIncrements;
    useDefaultStopPositionIncrement = false;
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @param matchVersion See <a href="#version">above</a>
   * @param stopwords Reader to load stop words from */
  public StopAnalyzer(Version matchVersion, Reader stopwords) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwords);
    this.enablePositionIncrements = StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion);
    useDefaultStopPositionIncrement = false;
  }

  /** Filters LowerCaseTokenizer with StopFilter. */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    if (useDefaultStopPositionIncrement) {
      return new StopFilter(new LowerCaseTokenizer(reader), stopWords);
    } else {
      return new StopFilter(enablePositionIncrements, new LowerCaseTokenizer(reader), stopWords);
    }
  }

  /** Filters LowerCaseTokenizer with StopFilter. */
  private class SavedStreams {
    Tokenizer source;
    TokenStream result;
  };
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new LowerCaseTokenizer(reader);
      if (useDefaultStopPositionIncrement) {
        streams.result = new StopFilter(streams.source, stopWords);
      } else {
        streams.result = new StopFilter(enablePositionIncrements, streams.source, stopWords);
      }
      setPreviousTokenStream(streams);
    } else
      streams.source.reset(reader);
    return streams.result;
  }
}

