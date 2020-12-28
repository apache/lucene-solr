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
package org.apache.lucene.analysis.sr;

import java.io.IOException;
import java.io.Reader;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.tartarus.snowball.ext.SerbianStemmer;

/**
 * {@link Analyzer} for Serbian.
 *
 * @since 8.6
 */
public class SerbianAnalyzer extends StopwordAnalyzerBase {
  private final CharArraySet stemExclusionSet;

  /** File containing default Serbian stopwords. */
  public static final String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /** The comment character in the stopwords file. All lines prefixed with this will be ignored. */
  private static final String STOPWORDS_COMMENT = "#";

  /**
   * Returns an unmodifiable instance of the default stop words set.
   *
   * @return default stop words set.
   */
  public static CharArraySet getDefaultStopSet() {
    return SerbianAnalyzer.DefaultSetHolder.DEFAULT_STOP_SET;
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
            loadStopwordSet(false, SerbianAnalyzer.class, DEFAULT_STOPWORD_FILE, STOPWORDS_COMMENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

  /** Builds an analyzer with the default stop words: {@link #DEFAULT_STOPWORD_FILE}. */
  public SerbianAnalyzer() {
    this(SerbianAnalyzer.DefaultSetHolder.DEFAULT_STOP_SET);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords a stopword set
   */
  public SerbianAnalyzer(CharArraySet stopwords) {
    this(stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * Builds an analyzer with the given stop words. If a non-empty stem exclusion set is provided
   * this analyzer will add a {@link SetKeywordMarkerFilter} before stemming.
   *
   * @param stopwords a stopword set
   * @param stemExclusionSet a set of terms not to be stemmed
   */
  public SerbianAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
    super(stopwords);
    this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
  }

  /**
   * Creates a {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} which tokenizes all
   * the text in the provided {@link Reader}.
   *
   * @return A {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents} built from an
   *     {@link StandardTokenizer} filtered with {@link LowerCaseFilter}, {@link StopFilter}, {@link
   *     SetKeywordMarkerFilter} if a stem exclusion set is provided, {@link SnowballFilter} ({@link
   *     SerbianStemmer} https://snowballstem.org/algorithms/serbian/stemmer.html), and {@link
   *     SerbianNormalizationFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new LowerCaseFilter(source);
    result = new StopFilter(result, stopwords);
    if (!stemExclusionSet.isEmpty()) result = new SetKeywordMarkerFilter(result, stemExclusionSet);
    result = new SnowballFilter(result, new SerbianStemmer());
    result = new SerbianNormalizationFilter(result);
    return new TokenStreamComponents(source, result);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
