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
package org.apache.lucene.analysis.el;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

/**
 * {@link Analyzer} for the Greek language. 
 * <p>
 * Supports an external list of stopwords (words
 * that will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 * 
 * <p><b>NOTE</b>: This class uses the same {@link org.apache.lucene.util.Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class GreekAnalyzer extends StopwordAnalyzerBase {
  /** File containing default Greek stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";
  
  /**
   * Returns a set of default Greek-stopwords 
   * @return a set of default Greek-stopwords 
   */
  public static final CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_SET;
  }
  
  private static class DefaultSetHolder {
    private static final CharArraySet DEFAULT_SET;
    
    static {
      try {
        DEFAULT_SET = loadStopwordSet(false, GreekAnalyzer.class, DEFAULT_STOPWORD_FILE, "#");
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
  public GreekAnalyzer() {
    this(DefaultSetHolder.DEFAULT_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words. 
   * <p>
   * <b>NOTE:</b> The stopwords set should be pre-processed with the logic of 
   * {@link GreekLowerCaseFilter} for best results.
   *  
   * @param stopwords a stopword set
   */
  public GreekAnalyzer(CharArraySet stopwords) {
    super(stopwords);
  }
  
  /**
   * Creates
   * {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.Analyzer.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link GreekLowerCaseFilter}, {@link StandardFilter},
   *         {@link StopFilter}, and {@link GreekStemFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new GreekLowerCaseFilter(source);
    result = new StandardFilter(result);
    result = new StopFilter(result, stopwords);
    result = new GreekStemFilter(result);
    return new TokenStreamComponents(source, result);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = new StandardFilter(in);
    result = new GreekLowerCaseFilter(result);
    return result;
  }
}
