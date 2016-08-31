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
package org.apache.lucene.analysis.standard;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.StopAnalyzer;

/**
 * Filters {@link org.apache.lucene.analysis.standard.UAX29URLEmailTokenizer}
 * with {@link org.apache.lucene.analysis.standard.StandardFilter},
 * {@link org.apache.lucene.analysis.LowerCaseFilter} and
 * {@link org.apache.lucene.analysis.StopFilter}, using a list of
 * English stop words.
 */
public final class UAX29URLEmailAnalyzer extends StopwordAnalyzerBase {
  
  /** Default maximum allowed token length */
  public static final int DEFAULT_MAX_TOKEN_LENGTH = StandardAnalyzer.DEFAULT_MAX_TOKEN_LENGTH;

  private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

  /** An unmodifiable set containing some common English words that are usually not
  useful for searching. */
  public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;

  /** Builds an analyzer with the given stop words.
   * @param stopWords stop words */
  public UAX29URLEmailAnalyzer(CharArraySet stopWords) {
    super(stopWords);
  }

  /** Builds an analyzer with the default stop words ({@link
   * #STOP_WORDS_SET}).
   */
  public UAX29URLEmailAnalyzer() {
    this(STOP_WORDS_SET);
  }

  /** Builds an analyzer with the stop words from the given reader.
   * @see org.apache.lucene.analysis.WordlistLoader#getWordSet(java.io.Reader)
   * @param stopwords Reader to read stop words from */
  public UAX29URLEmailAnalyzer(Reader stopwords) throws IOException {
    this(loadStopwordSet(stopwords));
  }

  /**
   * Set maximum allowed token length.  If a token is seen
   * that exceeds this length then it is discarded.  This
   * setting only takes effect the next time tokenStream or
   * tokenStream is called.
   */
  public void setMaxTokenLength(int length) {
    maxTokenLength = length;
  }
    
  /**
   * @see #setMaxTokenLength
   */
  public int getMaxTokenLength() {
    return maxTokenLength;
  }

  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final UAX29URLEmailTokenizer src = new UAX29URLEmailTokenizer();
    src.setMaxTokenLength(maxTokenLength);
    TokenStream tok = new StandardFilter(src);
    tok = new LowerCaseFilter(tok);
    tok = new StopFilter(tok, stopwords);
    return new TokenStreamComponents(src, tok) {
      @Override
      protected void setReader(final Reader reader) {
        src.setMaxTokenLength(UAX29URLEmailAnalyzer.this.maxTokenLength);
        super.setReader(reader);
      }
    };
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
