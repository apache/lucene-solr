package org.apache.lucene.analysis.ar;

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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Hashtable;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Arabic. 
 * <p>
 * This analyzer implements light-stemming as specified by:
 * <i>
 * Light Stemming for Arabic Information Retrieval
 * </i>    
 * http://www.mtholyoke.edu/~lballest/Pubs/arab_stem05.pdf
 * <p>
 * The analysis package contains three primary components:
 * <ul>
 *  <li>{@link ArabicNormalizationFilter}: Arabic orthographic normalization.
 *  <li>{@link ArabicStemFilter}: Arabic light stemming
 *  <li>Arabic stop words file: a set of default Arabic stop words.
 * </ul>
 * 
 */
public final class ArabicAnalyzer extends StopwordAnalyzerBase {

  /**
   * File containing default Arabic stopwords.
   * 
   * Default stopword list is from http://members.unine.ch/jacques.savoy/clef/index.html
   * The stopword list is BSD-Licensed.
   */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /**
   * The comment character in the stopwords file.  All lines prefixed with this will be ignored
   * @deprecated use {@link WordlistLoader#getWordSet(File, String)} directly  
   */
  // TODO make this private 
  @Deprecated
  public static final String STOPWORDS_COMMENT = "#";
  
  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
   * accesses the static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET = loadStopwordSet(false, ArabicAnalyzer.class, DEFAULT_STOPWORD_FILE, STOPWORDS_COMMENT);
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }
  }
  
  private final Set<?> stemExclusionSet;

  /**
   * Builds an analyzer with the default stop words: {@link #DEFAULT_STOPWORD_FILE}.
   */
  public ArabicAnalyzer(Version matchVersion) {
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
  public ArabicAnalyzer(Version matchVersion, Set<?> stopwords){
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }

  /**
   * Builds an analyzer with the given stop word. If a none-empty stem exclusion set is
   * provided this analyzer will add a {@link KeywordMarkerFilter} before
   * {@link ArabicStemFilter}.
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   * @param stemExclusionSet
   *          a set of terms not to be stemmed
   */
  public ArabicAnalyzer(Version matchVersion, Set<?> stopwords, Set<?> stemExclusionSet){
    super(matchVersion, stopwords);
    this.stemExclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(
        matchVersion, stemExclusionSet));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public ArabicAnalyzer( Version matchVersion, String... stopwords ) {
    this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords ));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public ArabicAnalyzer( Version matchVersion, Hashtable<?,?> stopwords ) {
    this(matchVersion, stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words.  Lines can be commented out using {@link #STOPWORDS_COMMENT}
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public ArabicAnalyzer( Version matchVersion, File stopwords ) throws IOException {
    this(matchVersion, WordlistLoader.getWordSet( stopwords, STOPWORDS_COMMENT));
  }

  /**
   * Creates
   * {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   * used to tokenize all the text in the provided {@link Reader}.
   * 
   * @return {@link org.apache.lucene.analysis.ReusableAnalyzerBase.TokenStreamComponents}
   *         built from an {@link StandardTokenizer} filtered with
   *         {@link LowerCaseFilter}, {@link StopFilter},
   *         {@link ArabicNormalizationFilter}, {@link KeywordMarkerFilter}
   *         if a stem exclusion set is provided and {@link ArabicStemFilter}.
   */
  @Override
  protected TokenStreamComponents createComponents(String fieldName,
      Reader reader) {
    final Tokenizer source = matchVersion.onOrAfter(Version.LUCENE_31) ? 
        new StandardTokenizer(matchVersion, reader) : new ArabicLetterTokenizer(matchVersion, reader);
    TokenStream result = new LowerCaseFilter(matchVersion, source);
    // the order here is important: the stopword list is not normalized!
    result = new StopFilter( matchVersion, result, stopwords);
    // TODO maybe we should make ArabicNormalization filter also KeywordAttribute aware?!
    result = new ArabicNormalizationFilter(result);
    if(!stemExclusionSet.isEmpty()) {
      result = new KeywordMarkerFilter(result, stemExclusionSet);
    }
    return new TokenStreamComponents(source, new ArabicStemFilter(result));
  }
}

