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

import java.io.Reader;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents; // javadoc @link
import org.apache.lucene.analysis.KeywordMarkerTokenFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Russian language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all).
 * A default set of stopwords is used unless an alternative list is specified.
 * </p>
 */
public final class RussianAnalyzer extends StopwordAnalyzerBase
{
    /**
     * List of typical Russian stopwords.
     */
    private static final String[] RUSSIAN_STOP_WORDS = {
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
    
    private static class DefaultSetHolder {
      static final Set<?> DEFAULT_STOP_SET = CharArraySet
          .unmodifiableSet(new CharArraySet(Version.LUCENE_CURRENT, 
              Arrays.asList(RUSSIAN_STOP_WORDS), false));
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
      this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
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
     * Creates {@link TokenStreamComponents} used to tokenize all the text in the 
     * provided {@link Reader}.
     *
     * @return {@link TokenStreamComponents} built from a 
     *   {@link RussianLetterTokenizer} filtered with 
     *   {@link LowerCaseFilter}, {@link StopFilter}, 
     *   and {@link RussianStemFilter}
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName,
        Reader reader) {
      final Tokenizer source = new RussianLetterTokenizer(matchVersion, reader);
      TokenStream result = new LowerCaseFilter(matchVersion, source);
      result = new StopFilter(matchVersion, result, stopwords);
      if(!stemExclusionSet.isEmpty())
        result = new KeywordMarkerTokenFilter(result, stemExclusionSet);
      return new TokenStreamComponents(source, new RussianStemFilter(result));
      
    }
}
