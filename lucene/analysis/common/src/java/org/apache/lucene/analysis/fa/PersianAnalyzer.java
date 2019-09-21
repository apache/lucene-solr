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
package org.apache.lucene.analysis.fa;


import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * {@link Analyzer} for Persian.
 * <p>
 * This Analyzer uses {@link PersianCharFilter} which implies tokenizing around
 * zero-width non-joiner in addition to whitespace. Some persian-specific variant forms (such as farsi
 * yeh and keheh) are standardized. "Stemming" is accomplished via stopwords.
 * </p>
 *
 * @since 3.1
 */
public final class PersianAnalyzer extends StopwordAnalyzerBase {

  /**
   * File containing default Persian stopwords.
   * 
   * Default stopword list is from
   * http://members.unine.ch/jacques.savoy/clef/index.html The stopword list is
   * BSD-Licensed.
   * 
   */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /**
   * The comment character in the stopwords file. All lines prefixed with this
   * will be ignored
   */
  public static final String STOPWORDS_COMMENT = "#";
  
  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
   * accesses the static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET = loadStopwordSet(false, PersianAnalyzer.class, DEFAULT_STOPWORD_FILE, STOPWORDS_COMMENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

  /**
   * Builds an analyzer with the default stop words:
   * {@link #DEFAULT_STOPWORD_FILE}.
   */
  public PersianAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words 
   * 
   * @param stopwords
   *          a stopword set
   */
  public PersianAnalyzer(CharArraySet stopwords){
    super(stopwords);
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link LowerCaseFilter}, {@link DecimalDigitFilter}, {@link ArabicNormalizationFilter},
   *         {@link PersianNormalizationFilter} and Persian Stop words
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new LowerCaseFilter(source);
    result = new DecimalDigitFilter(result);
    result = new ArabicNormalizationFilter(result);
    /* additional persian-specific normalization */
    result = new PersianNormalizationFilter(result);
    /*
     * the order here is important: the stopword list is normalized with the
     * above!
     */
    return new TokenStreamComponents(source, new StopFilter(result, stopwords));
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = new LowerCaseFilter(in);
    result = new DecimalDigitFilter(result);
    result = new ArabicNormalizationFilter(result);
    /* additional persian-specific normalization */
    result = new PersianNormalizationFilter(result);
    return result;
  }

  /** 
   * Wraps the Reader with {@link PersianCharFilter}
   */
  @Override
  protected Reader initReader(String fieldName, Reader reader) {
    return new PersianCharFilter(reader); 
  }
}
