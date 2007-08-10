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
import java.util.Set;

/** Filters LetterTokenizer with LowerCaseFilter and StopFilter. */

public final class StopAnalyzer extends Analyzer {
  private Set stopWords;

  /** An array containing some common English words that are not usually useful
    for searching. */
  public static final String[] ENGLISH_STOP_WORDS = {
    "a", "an", "and", "are", "as", "at", "be", "but", "by",
    "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such",
    "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with"
  };

  /** Builds an analyzer which removes words in ENGLISH_STOP_WORDS. */
  public StopAnalyzer() {
    stopWords = StopFilter.makeStopSet(ENGLISH_STOP_WORDS);
  }

  /** Builds an analyzer with the stop words from the given set.
   */
  public StopAnalyzer(Set stopWords) {
    this.stopWords = stopWords;
  }

  /** Builds an analyzer which removes words in the provided array. */
  public StopAnalyzer(String[] stopWords) {
    this.stopWords = StopFilter.makeStopSet(stopWords);
  }
  
  /** Builds an analyzer with the stop words from the given file.
   * @see WordlistLoader#getWordSet(File)
   */
  public StopAnalyzer(File stopwordsFile) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwordsFile);
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   */
  public StopAnalyzer(Reader stopwords) throws IOException {
    stopWords = WordlistLoader.getWordSet(stopwords);
  }

  /** Filters LowerCaseTokenizer with StopFilter. */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(new LowerCaseTokenizer(reader), stopWords);
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
      streams.result = new StopFilter(streams.source, stopWords);
      setPreviousTokenStream(streams);
    } else
      streams.source.reset(reader);
    return streams.result;
  }
}

