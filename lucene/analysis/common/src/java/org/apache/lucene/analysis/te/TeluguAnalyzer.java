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
package org.apache.lucene.analysis.te;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * Analyzer for Telugu.
 *
 * @since 9.0.0
 */
public final class TeluguAnalyzer extends StopwordAnalyzerBase {
  private final CharArraySet stemExclusionSet;

  /** File containing default Telugu stopwords. */
  public static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  private static final String STOPWORDS_COMMENT = "#";

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   *
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet() {
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }

  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class accesses the
   * static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET =
            loadStopwordSet(false, TeluguAnalyzer.class, DEFAULT_STOPWORD_FILE, STOPWORDS_COMMENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        throw new UncheckedIOException("Unable to load default stopword set", ex);
      }
    }
  }

  /**
   * Builds an analyzer with the given stop words
   *
   * @param stopwords a stopword set
   * @param stemExclusionSet a stemming exclusion set
   */
  public TeluguAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
    super(stopwords);
    this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
  }

  /**
   * Builds an analyzer with the given stop words
   *
   * @param stopwords a stopword set
   */
  public TeluguAnalyzer(CharArraySet stopwords) {
    this(stopwords, CharArraySet.EMPTY_SET);
  }

  /** Builds an analyzer with the default stop words: {@link #DEFAULT_STOPWORD_FILE}. */
  public TeluguAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }

  /**
   * Creates {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} used to tokenize all
   * the text in the provided {@link Reader}.
   *
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} built from a {@link
   *     StandardTokenizer} filtered with {@link DecimalDigitFilter}, {@link
   *     IndicNormalizationFilter}, {@link TeluguNormalizationFilter}, {@link
   *     SetKeywordMarkerFilter} if a stem exclusion set is provided, {@link TeluguStemFilter}, and
   *     Telugu Stop words
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new DecimalDigitFilter(source);
    if (!stemExclusionSet.isEmpty()) {
      result = new SetKeywordMarkerFilter(result, stemExclusionSet);
    }
    result = new IndicNormalizationFilter(result);
    result = new TeluguNormalizationFilter(result);
    result = new StopFilter(result, stopwords);
    result = new TeluguStemFilter(result);
    return new TokenStreamComponents(source, result);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = new DecimalDigitFilter(in);
    result = new IndicNormalizationFilter(in);
    result = new TeluguNormalizationFilter(in);
    return result;
  }
}
