package org.apache.lucene.analysis.br;

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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Brazilian Portuguese language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (words that will
 * not be stemmed, but indexed).
 * </p>
 *
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class BrazilianAnalyzer extends StopwordAnalyzerBase {
  /** File containing default Brazilian Portuguese stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
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
        DEFAULT_STOP_SET = WordlistLoader.getWordSet(IOUtils.getDecodingReader(BrazilianAnalyzer.class, 
            DEFAULT_STOPWORD_FILE, IOUtils.CHARSET_UTF_8), "#", Version.LUCENE_CURRENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }


  /**
   * Contains words that should be indexed but not stemmed.
   */
  private CharArraySet excltable = CharArraySet.EMPTY_SET;

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}).
   */
  public BrazilianAnalyzer(Version matchVersion) {
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
  public BrazilianAnalyzer(Version matchVersion, CharArraySet stopwords) {
     super(matchVersion, stopwords);
  }

  /**
   * Builds an analyzer with the given stop words and stemming exclusion words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public BrazilianAnalyzer(Version matchVersion, CharArraySet stopwords,
      CharArraySet stemExclusionSet) {
    this(matchVersion, stopwords);
    excltable = CharArraySet.unmodifiableSet(CharArraySet
        .copy(matchVersion, stemExclusionSet));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link LowerCaseFilter}, {@link StandardFilter}, {@link StopFilter}
   *         , and {@link BrazilianStemFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    Tokenizer source = new StandardTokenizer(matchVersion, reader);
    TokenStream result = new LowerCaseFilter(matchVersion, source);
    result = new StandardFilter(matchVersion, result);
    result = new StopFilter(matchVersion, result, stopwords);
    if(excltable != null && !excltable.isEmpty())
      result = new SetKeywordMarkerFilter(result, excltable);
    return new TokenStreamComponents(source, new BrazilianStemFilter(result));
  }
}

