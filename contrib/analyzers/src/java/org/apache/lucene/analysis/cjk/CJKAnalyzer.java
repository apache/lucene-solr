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

import java.io.Reader;
import java.util.Set;


/**
 * Filters CJKTokenizer with StopFilter.
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
  private Set stopTable;

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
  public CJKAnalyzer(String[] stopWords) {
    stopTable = StopFilter.makeStopSet(stopWords);
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * get token stream from input
   *
   * @param fieldName lucene field name
   * @param reader    input reader
   * @return TokenStream
   */
  public final TokenStream tokenStream(String fieldName, Reader reader) {
    return new StopFilter(new CJKTokenizer(reader), stopTable);
  }
}
