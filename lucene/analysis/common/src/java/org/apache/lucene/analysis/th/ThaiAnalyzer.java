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
package org.apache.lucene.analysis.th;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Thai language. It uses {@link java.text.BreakIterator} to break words.
 */
public final class ThaiAnalyzer extends StopwordAnalyzerBase {
  
  /** File containing default Thai stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  /**
   * The comment character in the stopwords file.  
   * All lines prefixed with this will be ignored.
   */
  private static final String STOPWORDS_COMMENT = "#";
  
  /**
   * Returns an unmodifiable instance of the default stop words set.
   * @return default stop words set.
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
        DEFAULT_STOP_SET = loadStopwordSet(false, ThaiAnalyzer.class, 
            DEFAULT_STOPWORD_FILE, STOPWORDS_COMMENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }

  /**
   * Builds an analyzer with the default stop words.
   */
  public ThaiAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords a stopword set
   */
  public ThaiAnalyzer(CharArraySet stopwords) {
    super(stopwords);
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link ThaiTokenizer} filtered with
   *         {@link LowerCaseFilter}, {@link DecimalDigitFilter} and {@link StopFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    if (getVersion().onOrAfter(Version.LUCENE_4_8_0)) {
      final Tokenizer source = new ThaiTokenizer();
      TokenStream result = new LowerCaseFilter(source);
      if (getVersion().onOrAfter(Version.LUCENE_5_4_0)) {
        result = new DecimalDigitFilter(result);
      }
      result = new StopFilter(result, stopwords);
      return new TokenStreamComponents(source, result);
    } else {
      final Tokenizer source;
      if (getVersion().onOrAfter(Version.LUCENE_4_7_0)) {
        source = new StandardTokenizer();
      } else {
        source = new StandardTokenizer40();
      }
      TokenStream result = new StandardFilter(source);
      result = new LowerCaseFilter(result);
      result = new ThaiWordFilter(result);
      return new TokenStreamComponents(source, new StopFilter(result, stopwords));
    }
  }
}
