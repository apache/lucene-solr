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
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

import java.io.IOException;
import java.io.Reader;
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
   */
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
   * stop word list
   */
  private final Set stopTable;

  //~ Constructors -----------------------------------------------------------

  /**
   * Builds an analyzer which removes words in {@link #STOP_WORDS}.
   */
  public CJKAnalyzer() {
    stopTable = StopFilter.makeStopSet(STOP_WORDS);
  }

  /**
   * Builds an analyzer which removes words in the provided array.
   *
   * @param stopWords stop word array
   */
  public CJKAnalyzer(String... stopWords) {
    stopTable = StopFilter.makeStopSet(stopWords);
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
  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(false, new CJKTokenizer(reader), stopTable);
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
  public final TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    /* tokenStream() is final, no back compat issue */
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new CJKTokenizer(reader);
      streams.result = new StopFilter(false, streams.source, stopTable);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
