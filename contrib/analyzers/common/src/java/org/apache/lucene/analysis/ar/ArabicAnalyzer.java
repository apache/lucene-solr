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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
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
public final class ArabicAnalyzer extends Analyzer {

  /**
   * File containing default Arabic stopwords.
   * 
   * Default stopword list is from http://members.unine.ch/jacques.savoy/clef/index.html
   * The stopword list is BSD-Licensed.
   */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /**
   * Contains the stopwords used with the StopFilter.
   */
  private final Set<?> stoptable;
  /**
   * The comment character in the stopwords file.  All lines prefixed with this will be ignored
   * @deprecated use {@link WordlistLoader#getWordSet(File, String)} directly  
   */
  public static final String STOPWORDS_COMMENT = "#";
  
  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<String> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  /**
   * Atomically loads the DEFAULT_STOP_SET in a lazy fashion once the outer class 
   * accesses the static final set the first time.;
   */
  private static class DefaultSetHolder {
    static final Set<String> DEFAULT_STOP_SET;

    static {
      try {
        DEFAULT_STOP_SET = loadDefaultStopWordSet();
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }

    static Set<String> loadDefaultStopWordSet() throws IOException {
      InputStream stream = ArabicAnalyzer.class
          .getResourceAsStream(DEFAULT_STOPWORD_FILE);
      try {
        InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
        // make sure it is unmodifiable as we expose it in the outer class
        return Collections.unmodifiableSet(WordlistLoader.getWordSet(reader,
            STOPWORDS_COMMENT));
      } finally {
        stream.close();
      }
    }
  }

  private final Version matchVersion;

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
    stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  public ArabicAnalyzer( Version matchVersion, String... stopwords ) {
    this(matchVersion, StopFilter.makeStopSet( stopwords ));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  public ArabicAnalyzer( Version matchVersion, Hashtable<?,?> stopwords ) {
    this(matchVersion, stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words.  Lines can be commented out using {@link #STOPWORDS_COMMENT}
   * @deprecated use {@link #ArabicAnalyzer(Version, Set)} instead
   */
  public ArabicAnalyzer( Version matchVersion, File stopwords ) throws IOException {
    this(matchVersion, WordlistLoader.getWordSet( stopwords, STOPWORDS_COMMENT));
  }


  /**
   * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
   *
   * @return  A {@link TokenStream} built from an {@link ArabicLetterTokenizer} filtered with
   * 			{@link LowerCaseFilter}, {@link StopFilter}, {@link ArabicNormalizationFilter}
   *            and {@link ArabicStemFilter}.
   */
  @Override
  public final TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new ArabicLetterTokenizer( reader );
    result = new LowerCaseFilter(matchVersion, result);
    // the order here is important: the stopword list is not normalized!
    result = new StopFilter( StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                             result, stoptable );
    result = new ArabicNormalizationFilter( result );
    result = new ArabicStemFilter( result );

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
   * @return  A {@link TokenStream} built from an {@link ArabicLetterTokenizer} filtered with
   *            {@link LowerCaseFilter}, {@link StopFilter}, {@link ArabicNormalizationFilter}
   *            and {@link ArabicStemFilter}.
   */
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new ArabicLetterTokenizer(reader);
      streams.result = new LowerCaseFilter(matchVersion, streams.source);
      // the order here is important: the stopword list is not normalized!
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.result, stoptable);
      streams.result = new ArabicNormalizationFilter(streams.result);
      streams.result = new ArabicStemFilter(streams.result);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}

