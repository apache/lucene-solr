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
import java.util.List;

/** Filters {@link LetterTokenizer} with {@link LowerCaseFilter} and {@link StopFilter}. */

public final class StopAnalyzer extends Analyzer {
  private final Set<?> stopWords;
  private final boolean enablePositionIncrements;
  
  /** An unmodifiable set containing some common English words that are not usually useful
  for searching.*/
  public static final Set<?> ENGLISH_STOP_WORDS_SET;
  
  static {
    final List<String> stopWords = Arrays.asList(
      "a", "an", "and", "are", "as", "at", "be", "but", "by",
      "for", "if", "in", "into", "is", "it",
      "no", "not", "of", "on", "or", "such",
      "that", "the", "their", "then", "there", "these",
      "they", "this", "to", "was", "will", "with"
    );
    final CharArraySet stopSet = new CharArraySet(stopWords.size(), false);
    stopSet.addAll(stopWords);  
    ENGLISH_STOP_WORDS_SET = CharArraySet.unmodifiableSet(stopSet); 
  }
  
  /** Builds an analyzer which removes words in
   *  {@link #ENGLISH_STOP_WORDS}.
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} */
  public StopAnalyzer(boolean enablePositionIncrements) {
    stopWords = ENGLISH_STOP_WORDS_SET;
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /** Builds an analyzer with the stop words from the given set.
   * @param stopWords Set of stop words
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} */
  public StopAnalyzer(Set<?> stopWords, boolean enablePositionIncrements) {
    this.stopWords = stopWords;
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /** Builds an analyzer with the stop words from the given file.
   * @see WordlistLoader#getWordSet(File)
   * @param stopwordsFile File to load stop words from
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} */
  public StopAnalyzer(File stopwordsFile, boolean enablePositionIncrements) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwordsFile);
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @param stopwords Reader to load stop words from
   * @param enablePositionIncrements See {@link
   * StopFilter#setEnablePositionIncrements} */
  public StopAnalyzer(Reader stopwords, boolean enablePositionIncrements) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwords);
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /** Filters LowerCaseTokenizer with StopFilter. */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(enablePositionIncrements, new LowerCaseTokenizer(reader), stopWords);
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
      streams.result = new StopFilter(enablePositionIncrements, streams.source, stopWords);
      setPreviousTokenStream(streams);
    } else
      streams.source.reset(reader);
    return streams.result;
  }
}

