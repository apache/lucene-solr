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
import org.apache.lucene.analysis.StopFilter;
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
public final class RussianAnalyzer extends Analyzer
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
          .unmodifiableSet(new CharArraySet(Arrays.asList(RUSSIAN_STOP_WORDS),
              false));
    }

    /**
     * Contains the stopwords used with the StopFilter.
     */
    private final Set<?> stopSet;

    private final Version matchVersion;

    public RussianAnalyzer(Version matchVersion) {
      this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
    }
  
    /**
     * Builds an analyzer with the given stop words.
     * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
     */
    public RussianAnalyzer(Version matchVersion, String... stopwords) {
      this(matchVersion, StopFilter.makeStopSet(stopwords));
    }
    
    /**
     * Builds an analyzer with the given stop words
     * 
     * @param matchversion
     *          lucene compatibility version
     * @param stopwords
     *          a stopword set
     */
    public RussianAnalyzer(Version matchVersion, Set<?> stopwords){
      stopSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
      this.matchVersion = matchVersion;
    }
   
    /**
     * Builds an analyzer with the given stop words.
     * TODO: create a Set version of this ctor
     * @deprecated use {@link #RussianAnalyzer(Version, Set)} instead
     */
    public RussianAnalyzer(Version matchVersion, Map<?,?> stopwords)
    {
      this(matchVersion, stopwords.keySet());
    }

    /**
     * Creates a {@link TokenStream} which tokenizes all the text in the 
     * provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a 
     *   {@link RussianLetterTokenizer} filtered with 
     *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
     *   and {@link RussianStemFilter}
     */
    @Override
    public TokenStream tokenStream(String fieldName, Reader reader)
    {
        TokenStream result = new RussianLetterTokenizer(reader);
        result = new LowerCaseFilter(result);
        result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                result, stopSet);
        result = new RussianStemFilter(result);
        return result;
    }
    
    private class SavedStreams {
      Tokenizer source;
      TokenStream result;
    };
    
    /**
     * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
     * in the provided {@link Reader}.
     *
     * @return  A {@link TokenStream} built from a 
     *   {@link RussianLetterTokenizer} filtered with 
     *   {@link RussianLowerCaseFilter}, {@link StopFilter}, 
     *   and {@link RussianStemFilter}
     */
    @Override
    public TokenStream reusableTokenStream(String fieldName, Reader reader) 
      throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new RussianLetterTokenizer(reader);
      streams.result = new LowerCaseFilter(streams.source);
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.result, stopSet);
      streams.result = new RussianStemFilter(streams.result);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
