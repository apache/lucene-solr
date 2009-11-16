package org.apache.lucene.analysis.cjk;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Set;


/**
 * An {@link Analyzer} that tokenizes text with {@link CJKTokenizer} and
 * filters with {@link StopFilter}
 *
 */
public class CJKAnalyzer extends Analyzer {
  //~ Static fields/initializers ---------------------------------------------

  /**
   * An array containing some common English words that are not usually
   * useful for searching and some double-byte interpunctions.
   * @deprecated use {@link #getDefaultStopSet()} instead
   */
  // TODO make this final in 3.1 -
  // this might be revised and merged with StopFilter stop words too
  public final static String[] STOP_WORDS = {
    "a", "and", "are", "as", "at", "be",
    "but", "by", "for", "if", "in",
    "into", "is", "it", "no", "not",
    "of", "on", "or", "s", "such", "t",
    "that", "the", "their", "then",
    "there", "these", "they", "this",
    "to", "was", "will", "with", "",
    "www"
  };

  //~ Instance fields --------------------------------------------------------

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET = CharArraySet
        .unmodifiableSet(new CharArraySet(Arrays.asList(STOP_WORDS),
            false));
  }
  /**
   * stop word list
   */
  private final Set<?> stopTable;
  private final Version matchVersion;

  //~ Constructors -----------------------------------------------------------

  /**
   * Builds an analyzer which removes words in {@link #STOP_WORDS}.
   */
  public CJKAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public CJKAnalyzer(Version matchVersion, Set<?> stopwords){
    stopTable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an analyzer which removes words in the provided array.
   *
   * @param stopWords stop word array
   * @deprecated use {@link #CJKAnalyzer(Version, Set)} instead
   */
  public CJKAnalyzer(Version matchVersion, String... stopWords) {
    stopTable = StopFilter.makeStopSet(stopWords);
    this.matchVersion = matchVersion;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
   *
   * @param fieldName lucene field name
   * @param reader    input {@link Reader}
   * @return A {@link TokenStream} built from {@link CJKTokenizer}, filtered with
   *    {@link StopFilter}
   */
  @Override
  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                          new CJKTokenizer(reader), stopTable);
  }
  
  private class SavedStreams {
    Tokenizer source;
    TokenStream result;
  };
  
  /**
   * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
   * in the provided {@link Reader}.
   *
   * @param fieldName lucene field name
   * @param reader    Input {@link Reader}
   * @return A {@link TokenStream} built from {@link CJKTokenizer}, filtered with
   *    {@link StopFilter}
   */
  @Override
  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    /* tokenStream() is final, no back compat issue */
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new CJKTokenizer(reader);
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.source, stopTable);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
