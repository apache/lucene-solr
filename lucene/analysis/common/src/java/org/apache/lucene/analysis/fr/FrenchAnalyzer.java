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
package org.apache.lucene.analysis.fr;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * {@link Analyzer} for French language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link org.apache.lucene.util.Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class FrenchAnalyzer extends StopwordAnalyzerBase {

  /** File containing default French stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "french_stop.txt";
  
  /** Default set of articles for ElisionFilter */
  public static final CharArraySet DEFAULT_ARTICLES = CharArraySet.unmodifiableSet(
      new CharArraySet(Arrays.asList(
          "l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoiqu", "lorsqu", "puisqu"), true));

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private final CharArraySet excltable;

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;
    static {
      try {
        DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, 
                DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet}).
   */
  public FrenchAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param stopwords
   *          a stopword set
   */
  public FrenchAnalyzer(CharArraySet stopwords){
    this(stopwords, CharArraySet.EMPTY_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param stopwords
   *          a stopword set
   * @param stemExclutionSet
   *          a stemming exclusion set
   */
  public FrenchAnalyzer(CharArraySet stopwords,
      CharArraySet stemExclutionSet) {
    super(stopwords);
    this.excltable = CharArraySet.unmodifiableSet(CharArraySet
        .copy(stemExclutionSet));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link ElisionFilter},
   *         {@link LowerCaseFilter}, {@link StopFilter},
   *         {@link SetKeywordMarkerFilter} if a stem exclusion set is
   *         provided, and {@link FrenchLightStemFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source;
    if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
      source = new StandardTokenizer();
    } else {
      source = new StandardTokenizer40();
    }
    TokenStream result = new StandardFilter(source);
    result = new ElisionFilter(result, DEFAULT_ARTICLES);
    result = new LowerCaseFilter(result);
    result = new StopFilter(result, stopwords);
    if(!excltable.isEmpty())
      result = new SetKeywordMarkerFilter(result, excltable);
    result = new FrenchLightStemFilter(result);
    return new TokenStreamComponents(source, result);
  }
}

