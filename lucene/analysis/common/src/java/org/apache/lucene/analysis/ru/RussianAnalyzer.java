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
package org.apache.lucene.analysis.ru;


import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.std40.StandardTokenizer40;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Russian language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 */
public final class RussianAnalyzer extends StopwordAnalyzerBase {
    
    /** File containing default Russian stopwords. */
    public final static String DEFAULT_STOPWORD_FILE = "russian_stop.txt";
    
    private static class DefaultSetHolder {
      static final CharArraySet DEFAULT_STOP_SET;
      
      static {
        try {
          DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, 
              DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
        } catch (IOException ex) {
          // default set should always be present as it is part of the
          // distribution (JAR)
          throw new RuntimeException("Unable to load default stopword set", ex);
        }
      }
    }
    
    private final CharArraySet stemExclusionSet;
    
    /**
     * Returns an unmodifiable instance of the default stop-words set.
     * 
     * @return an unmodifiable instance of the default stop-words set.
     */
    public static CharArraySet getDefaultStopSet() {
      return DefaultSetHolder.DEFAULT_STOP_SET;
    }

    public RussianAnalyzer() {
      this(DefaultSetHolder.DEFAULT_STOP_SET);
    }
  
    /**
     * Builds an analyzer with the given stop words
     * 
     * @param stopwords
     *          a stopword set
     */
    public RussianAnalyzer(CharArraySet stopwords) {
      this(stopwords, CharArraySet.EMPTY_SET);
    }
    
    /**
     * Builds an analyzer with the given stop words
     * 
     * @param stopwords
     *          a stopword set
     * @param stemExclusionSet a set of words not to be stemmed
     */
    public RussianAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionSet) {
      super(stopwords);
      this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
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
   *         provided, and {@link SnowballFilter}
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
      result = new LowerCaseFilter(result);
      result = new StopFilter(result, stopwords);
      if (!stemExclusionSet.isEmpty()) 
        result = new SetKeywordMarkerFilter(result, stemExclusionSet);
      result = new SnowballFilter(result, new org.tartarus.snowball.ext.RussianStemmer());
      return new TokenStreamComponents(source, result);
    }
}
