package org.apache.lucene.analysis.cz;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * {@link Analyzer} for Czech language.
 * <p>
 * Supports an external list of stopwords (words that will not be indexed at
 * all). A default set of stopwords is used unless an alternative list is
 * specified.
 * </p>
 * 
 * <a name="version"/>
 * <p>
 * You may specify the {@link Version} compatibility when creating
 * CzechAnalyzer:
 * <ul>
 * <li>As of 3.1, words are stemmed with {@link CzechStemFilter}
 * <li>As of 2.9, StopFilter preserves position increments
 * <li>As of 2.4, Tokens incorrectly identified as acronyms are corrected (see
 * <a href="https://issues.apache.org/jira/browse/LUCENE-1068">LUCENE-1068</a>)
 * </ul>
 */
public final class CzechAnalyzer extends StopwordAnalyzerBase {
  /** File containing default Czech stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
  /**
   * Returns a set of default Czech-stopwords
   * 
   * @return a set of default Czech-stopwords
   */
  public static final CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_SET;
  }

  private static class DefaultSetHolder {
    private static final CharArraySet DEFAULT_SET;
  
    static {
      try {
        DEFAULT_SET = WordlistLoader.getWordSet(IOUtils.getDecodingReader(CzechAnalyzer.class, 
            DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8), "#");
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

 
  private final CharArraySet stemExclusionTable;

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}).
   */
  public CzechAnalyzer() {
    this(DefaultSetHolder.DEFAULT_SET);
  }

  /**
   * @deprecated Use {@link #CzechAnalyzer()}
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_SET);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords a stopword set
   */
  public CzechAnalyzer(CharArraySet stopwords) {
    this(stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * @deprecated Use {@link #CzechAnalyzer(CharArraySet)}
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion, CharArraySet stopwords) {
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * Builds an analyzer with the given stop words and a set of work to be
   * excluded from the {@link CzechStemFilter}.
   *
   * @param stopwords a stopword set
   * @param stemExclusionTable a stemming exclusion set
   */
  public CzechAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionTable) {
    super(stopwords);
    this.stemExclusionTable = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionTable));
  }

  /**
   * @deprecated Use {@link #CzechAnalyzer(CharArraySet, CharArraySet)}
   */
  @Deprecated
  public CzechAnalyzer(Version matchVersion, CharArraySet stopwords, CharArraySet stemExclusionTable) {
    super(matchVersion, stopwords);
    this.stemExclusionTable = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionTable));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}
   *         , and {@link CzechStemFilter} (only if version is >= LUCENE_3_1). If
   *         a version is >= LUCENE_3_1 and a stem exclusion set is provided via
   *         {@link #CzechAnalyzer(CharArraySet, CharArraySet)} a
   *         {@link SetKeywordMarkerFilter} is added before
   *         {@link CzechStemFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = new StandardTokenizer(getVersion(), reader);
    TokenStream result = new StandardFilter(getVersion(), source);
    result = new LowerCaseFilter(getVersion(), result);
    result = new StopFilter(getVersion(), result, stopwords);
    if (getVersion().onOrAfter(Version.LUCENE_3_1_0)) {
      if(!this.stemExclusionTable.isEmpty())
        result = new SetKeywordMarkerFilter(result, stemExclusionTable);
      result = new CzechStemFilter(result);
    }
    return new TokenStreamComponents(source, result);
  }
}

