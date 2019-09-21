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
package org.apache.lucene.analysis.nl;


import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.miscellaneous.SetKeywordMarkerFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.IOUtils;

/**
 * {@link Analyzer} for Dutch language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all), an external list of exclusions (word that will
 * not be stemmed, but indexed) and an external list of word-stem pairs that overrule
 * the algorithm (dictionary stemming).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 *
 * @since 3.1
 */
// TODO: extend StopwordAnalyzerBase
public final class DutchAnalyzer extends Analyzer {
  
  /** File containing default Dutch stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "dutch_stop.txt";

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static CharArraySet getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final CharArraySet DEFAULT_STOP_SET;
    static final CharArrayMap<String> DEFAULT_STEM_DICT;
    static {
      try {
        DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, 
            DEFAULT_STOPWORD_FILE, StandardCharsets.UTF_8));
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
      
      DEFAULT_STEM_DICT = new CharArrayMap<>(4, false);
      DEFAULT_STEM_DICT.put("fiets", "fiets"); //otherwise fiet
      DEFAULT_STEM_DICT.put("bromfiets", "bromfiets"); //otherwise bromfiet
      DEFAULT_STEM_DICT.put("ei", "eier");
      DEFAULT_STEM_DICT.put("kind", "kinder");
    }
  }


  /**
   * Contains the stopwords used with the StopFilter.
   */
  private final CharArraySet stoptable;

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private CharArraySet excltable = CharArraySet.EMPTY_SET;

  private final StemmerOverrideMap stemdict;

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}) 
   * and a few default entries for the stem exclusion table.
   * 
   */
  public DutchAnalyzer() {
    this(DefaultSetHolder.DEFAULT_STOP_SET, CharArraySet.EMPTY_SET, DefaultSetHolder.DEFAULT_STEM_DICT);
  }
  
  public DutchAnalyzer(CharArraySet stopwords){
    this(stopwords, CharArraySet.EMPTY_SET, DefaultSetHolder.DEFAULT_STEM_DICT);
  }
  
  public DutchAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionTable){
    this(stopwords, stemExclusionTable, DefaultSetHolder.DEFAULT_STEM_DICT);
  }
  
  public DutchAnalyzer(CharArraySet stopwords, CharArraySet stemExclusionTable, CharArrayMap<String> stemOverrideDict) {
    this.stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    this.excltable = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionTable));
    if (stemOverrideDict.isEmpty()) {
      this.stemdict = null;
    } else {
      // we don't need to ignore case here since we lowercase in this analyzer anyway
      StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(false);
      CharArrayMap<String>.EntryIterator iter = stemOverrideDict.entrySet().iterator();
      CharsRefBuilder spare = new CharsRefBuilder();
      while (iter.hasNext()) {
        char[] nextKey = iter.nextKey();
        spare.copyChars(nextKey, 0, nextKey.length);
        builder.add(spare.get(), iter.currentValue());
      }
      try {
        this.stemdict = builder.build();
      } catch (IOException ex) {
        throw new RuntimeException("can not build stem dict", ex);
      }
    }
  }
  
  /**
   * Returns a (possibly reused) {@link TokenStream} which tokenizes all the 
   * text in the provided {@link Reader}.
   *
   * @return A {@link TokenStream} built from a {@link StandardTokenizer}
   *   filtered with {@link LowerCaseFilter},
   *   {@link StopFilter}, {@link SetKeywordMarkerFilter} if a stem exclusion set is provided,
   *   {@link StemmerOverrideFilter}, and {@link SnowballFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer source = new StandardTokenizer();
    TokenStream result = new LowerCaseFilter(source);
    result = new StopFilter(result, stoptable);
    if (!excltable.isEmpty())
      result = new SetKeywordMarkerFilter(result, excltable);
    if (stemdict != null)
      result = new StemmerOverrideFilter(result, stemdict);
    result = new SnowballFilter(result, new org.tartarus.snowball.ext.DutchStemmer());
    return new TokenStreamComponents(source, result);
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    return new LowerCaseFilter(in);
  }
}
