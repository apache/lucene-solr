package org.apache.lucene.analysis.de;
// This file is encoded in UTF-8

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

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.German2Stemmer;

/**
 * {@link Analyzer} for German language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 * 
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating GermanAnalyzer:
 * <ul>
 *   <li> As of 3.6, GermanLightStemFilter is used for less aggressive stemming.
 *   <li> As of 3.1, Snowball stemming is done with SnowballFilter, and 
 *        Snowball stopwords are used by default.
 *   <li> As of 2.9, StopFilter preserves position
 *        increments
 * </ul>
 * 
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class GermanAnalyzer extends StopwordAnalyzerBase {
  
  /** @deprecated in 3.1, remove in Lucene 5.0 (index bw compat) */
  @Deprecated
  private final static String[] GERMAN_STOP_WORDS = {
    "einer", "eine", "eines", "einem", "einen",
    "der", "die", "das", "dass", "daß",
    "du", "er", "sie", "es",
    "was", "wer", "wie", "wir",
    "und", "oder", "ohne", "mit",
    "am", "im", "in", "aus", "auf",
    "ist", "sein", "war", "wird",
    "ihr", "ihre", "ihres",
    "als", "für", "von", "mit",
    "dich", "dir", "mich", "mir",
    "mein", "sein", "kein",
    "durch", "wegen", "wird"
  };
  
  /** File containing default German stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "german_stop.txt";
  
  /**
   * Returns a set of default German-stopwords 
   * @return a set of default German-stopwords 
   */
  public static final CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_SET;
  }
  
  private static class DefaultSetHolder {
    /** @deprecated in 3.1, remove in Lucene 5.0 (index bw compat) */
    @Deprecated
    private static final CharArraySet DEFAULT_SET_30 = CharArraySet.unmodifiableSet(new CharArraySet(
        Version.LUCENE_CURRENT, Arrays.asList(GERMAN_STOP_WORDS), false));
    private static final CharArraySet DEFAULT_SET;
    static {
      try {
        DEFAULT_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, 
            DEFAULT_STOPWORD_FILE, IOUtils.CHARSET_UTF_8), Version.LUCENE_CURRENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

  /**
   * Contains the stopwords used with the {@link StopFilter}.
   */
 
  /**
   * Contains words that should be indexed but not stemmed.
   */
  private final CharArraySet exclusionSet;

  /**
   * Builds an analyzer with the default stop words:
   * {@link #getDefaultStopSet()}.
   */
  public GermanAnalyzer(Version matchVersion) {
    this(matchVersion,
        matchVersion.onOrAfter(Version.LUCENE_31) ? DefaultSetHolder.DEFAULT_SET
            : DefaultSetHolder.DEFAULT_SET_30);
  }
  
  /**
   * Builds an analyzer with the given stop words 
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public GermanAnalyzer(Version matchVersion, CharArraySet stopwords) {
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   * @param stemExclusionSet
   *          a stemming exclusion set
   */
  public GermanAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionSet) {
    super(matchVersion, stopwords);
    exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionSet));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}
   *         , {@link SetKeywordMarkerFilter} if a stem exclusion set is
   *         provided, {@link GermanNormalizationFilter} and {@link GermanLightStemFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new StandardFilter(matchVersion, source);
    result = new LowerCaseFilter(matchVersion, result);
    result = new StopFilter( matchVersion, result, stopwords);
    result = new SetKeywordMarkerFilter(result, exclusionSet);
    if (matchVersion.onOrAfter(Version.LUCENE_36)) {
      result = new GermanNormalizationFilter(result);
      result = new GermanLightStemFilter(result);
    } else if (matchVersion.onOrAfter(Version.LUCENE_31)) {
      result = new SnowballFilter(result, new German2Stemmer());
    } else {
      result = new GermanStemFilter(result);
    }
    return new TokenStreamComponents(source, result);
  }
}
