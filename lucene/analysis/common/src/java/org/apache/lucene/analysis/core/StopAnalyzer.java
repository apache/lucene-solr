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
package org.apache.lucene.analysis.core;


import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;

/** 
 * Filters {@link LetterTokenizer} with {@link LowerCaseFilter} and {@link StopFilter}.
 *
 * @since 3.1
 */
public final class StopAnalyzer extends StopwordAnalyzerBase {

  /** Builds an analyzer with the stop words from the given set.
   * @param stopWords Set of stop words */
  public StopAnalyzer(CharArraySet stopWords) {
    super(stopWords);
  }

  /** Builds an analyzer with the stop words from the given path.
   * @see WordlistLoader#getWordSet(Reader)
   * @param stopwordsFile File to load stop words from */
  public StopAnalyzer(Path stopwordsFile) throws IOException {
    this(loadStopwordSet(stopwordsFile));
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see WordlistLoader#getWordSet(Reader)
   * @param stopwords Reader to load stop words from */
  public StopAnalyzer(Reader stopwords) throws IOException {
    this(loadStopwordSet(stopwords));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link LetterTokenizer} filtered with
   *         {@link StopFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new LetterTokenizer();
    return new TokenStreamComponents(source, new StopFilter(new LowerCaseFilter(source), stopwords));
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}

