package org.apache.lucene.analysis.nl;

/**
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
import org.apache.lucene.analysis.miscellaneous.KeywordMarkerFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.WordlistLoader;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;

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
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating DutchAnalyzer:
 * <ul>
 *   <li> As of 3.1, Snowball stemming is done with SnowballFilter, 
 *        LowerCaseFilter is used prior to StopFilter, and Snowball 
 *        stopwords are used by default.
 *   <li> As of 2.9, StopFilter preserves position
 *        increments
 * </ul>
 * 
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public final class DutchAnalyzer extends Analyzer {
  
  /** File containing default Dutch stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "dutch_stop.txt";

  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(SnowballFilter.class, 
            DEFAULT_STOPWORD_FILE);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }


  /**
   * Contains the stopwords used with the StopFilter.
   */
  private final Set<?> stoptable;

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private Set<?> excltable = Collections.emptySet();

  private final Map<String, String> stemdict = new HashMap<String, String>();
  private final Version matchVersion;

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}) 
   * and a few default entries for the stem exclusion table.
   * 
   */
  public DutchAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
    stemdict.put("fiets", "fiets"); //otherwise fiet
    stemdict.put("bromfiets", "bromfiets"); //otherwise bromfiet
    stemdict.put("ei", "eier");
    stemdict.put("kind", "kinder");
  }
  
  public DutchAnalyzer(Version matchVersion, Set<?> stopwords){
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }
  
  public DutchAnalyzer(Version matchVersion, Set<?> stopwords, Set<?> stemExclusionTable){
    stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stopwords));
    excltable = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionTable));
    this.matchVersion = matchVersion;
  }
  
  /**
   * Returns a (possibly reused) {@link TokenStream} which tokenizes all the 
   * text in the provided {@link Reader}.
   *
   * @return A {@link TokenStream} built from a {@link StandardTokenizer}
   *   filtered with {@link StandardFilter}, {@link LowerCaseFilter}, 
   *   {@link StopFilter}, {@link KeywordMarkerFilter} if a stem exclusion set is provided,
   *   {@link StemmerOverrideFilter}, and {@link SnowballFilter}
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader aReader) {
    if (matchVersion.onOrAfter(Version.LUCENE_31)) {
      final Tokenizer source = new StandardTokenizer(matchVersion, aReader);
      TokenStream result = new StandardFilter(matchVersion, source);
      result = new LowerCaseFilter(matchVersion, result);
      result = new StopFilter(matchVersion, result, stoptable);
      if (!excltable.isEmpty())
        result = new KeywordMarkerFilter(result, excltable);
      if (!stemdict.isEmpty())
        result = new StemmerOverrideFilter(matchVersion, result, stemdict);
      result = new SnowballFilter(result, new org.tartarus.snowball.ext.DutchStemmer());
      return new TokenStreamComponents(source, result);
    } else {
      final Tokenizer source = new StandardTokenizer(matchVersion, aReader);
      TokenStream result = new StandardFilter(matchVersion, source);
      result = new StopFilter(matchVersion, result, stoptable);
      if (!excltable.isEmpty())
        result = new KeywordMarkerFilter(result, excltable);
      result = new DutchStemFilter(result, stemdict);
      return new TokenStreamComponents(source, result);
    }
  }
}
