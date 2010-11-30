package org.apache.lucene.analysis.ru;

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

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Russian language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 * <a name="version"/>
 * <p>You must specify the required {@link Version}
 * compatibility when creating RussianAnalyzer:
 * <ul>
 *   <li> As of 3.1, StandardTokenizer is used, Snowball stemming is done with
 *        SnowballFilter, and Snowball stopwords are used by default.
 * </ul>
 */
public final class RussianAnalyzer extends StopwordAnalyzerBase
{
    /**
     * List of typical Russian stopwords. (for backwards compatibility)
     * @deprecated Remove this for LUCENE 5.0
     */
    @Deprecated
    private static final String[] RUSSIAN_STOP_WORDS_30 = {
      "а", "без", "более", "бы", "был", "была", "были", "было", "быть", "в",
      "вам", "вас", "весь", "во", "вот", "все", "всего", "всех", "вы", "где", 
      "да", "даже", "для", "до", "его", "ее", "ей", "ею", "если", "есть", 
      "еще", "же", "за", "здесь", "и", "из", "или", "им", "их", "к", "как",
      "ко", "когда", "кто", "ли", "либо", "мне", "может", "мы", "на", "надо", 
      "наш", "не", "него", "нее", "нет", "ни", "них", "но", "ну", "о", "об", 
      "однако", "он", "она", "они", "оно", "от", "очень", "по", "под", "при", 
      "с", "со", "так", "также", "такой", "там", "те", "тем", "то", "того", 
      "тоже", "той", "только", "том", "ты", "у", "уже", "хотя", "чего", "чей", 
      "чем", "что", "чтобы", "чье", "чья", "эта", "эти", "это", "я"
    };
    
    /** File containing default Russian stopwords. */
    public final static String DEFAULT_STOPWORD_FILE = "russian_stop.txt";
    
    private static class DefaultSetHolder {
      /** @deprecated remove this for Lucene 5.0 */
      @Deprecated
      static final Set<?> DEFAULT_STOP_SET_30 = CharArraySet
          .unmodifiableSet(new CharArraySet(Version.LUCENE_CURRENT, 
              Arrays.asList(RUSSIAN_STOP_WORDS_30), false));
      static final Set<?> DEFAULT_STOP_SET;
      
      static {
        try {
          DEFAULT_STOP_SET = 
            WordlistLoader.getSnowballWordSet(SnowballFilter.class, DEFAULT_STOPWORD_FILE);
        } catch (IOException ex) {
          // default set should always be present as it is part of the
          // distribution (JAR)
          throw new RuntimeException("Unable to load default stopword set");
        }
      }
    }
    
    private final Set<?> stemExclusionSet;
    
    /**
     * Returns an unmodifiable instance of the default stop-words set.
     * 
     * @return an unmodifiable instance of the default stop-words set.
     */
    public static Set<?> getDefaultStopSet() {
      return DefaultSetHolder.DEFAULT_STOP_SET;
    }

    public RussianAnalyzer(Version matchVersion) {
      this(matchVersion,
        matchVersion.onOrAfter(Version.LUCENE_31) ? DefaultSetHolder.DEFAULT_STOP_SET
            : DefaultSetHolder.DEFAULT_STOP_SET_30);
    }
  
    /**
     * Builds an analyzer with the given stop words.
     * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
     */
    @Deprecated
    public RussianAnalyzer(Version matchVersion, String... stopwords) {
      this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords));
    }
    
    /**
     * Builds an analyzer with the given stop words
     * 
     * @param matchVersion
     *          lucene compatibility version
     * @param stopwords
     *          a stopword set
     */
    public RussianAnalyzer(Version matchVersion, Set<?> stopwords){
      this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
    }
    
    /**
     * Builds an analyzer with the given stop words
     * 
     * @param matchVersion
     *          lucene compatibility version
     * @param stopwords
     *          a stopword set
     * @param stemExclusionSet a set of words not to be stemmed
     */
    public RussianAnalyzer(Version matchVersion, Set<?> stopwords, Set<?> stemExclusionSet){
      super(matchVersion, stopwords);
      this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(matchVersion, stemExclusionSet));
    }
   
   
    /**
     * Builds an analyzer with the given stop words.
     * TODO: create a Set version of this ctor
     * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
     */
    @Deprecated
    public RussianAnalyzer(Version matchVersion, Map<?,?> stopwords)
    {
      this(matchVersion, stopwords.keySet());
    }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   *         built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}
   *         , {@link KeywordMarkerFilter} if a stem exclusion set is
   *         provided, and {@link SnowballFilter}
   */
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      if (matchVersion.onOrAfter(Version.LUCENE_31)) {
        final Tokenizer source = new StandardTokenizer(matchVersion, reader);
        TokenStream result = new StandardFilter(matchVersion, source);
        result = new LowerCaseFilter(matchVersion, result);
        result = new StopFilter(matchVersion, result, stopwords);
        if (!stemExclusionSet.isEmpty()) result = new KeywordMarkerFilter(
            result, stemExclusionSet);
        result = new SnowballFilter(result, new org.tartarus.snowball.ext.RussianStemmer());
        return new TokenStreamComponents(source, result);
      } else {
        final Tokenizer source = new RussianLetterTokenizer(matchVersion, reader);
        TokenStream result = new LowerCaseFilter(matchVersion, source);
        result = new StopFilter(matchVersion, result, stopwords);
        if (!stemExclusionSet.isEmpty()) result = new KeywordMarkerFilter(
          result, stemExclusionSet);
        return new TokenStreamComponents(source, new RussianStemFilter(result));
      }
    }
}
