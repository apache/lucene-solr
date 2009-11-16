package org.apache.lucene.analysis.fa;

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
import org.apache.lucene.analysis.ar.ArabicLetterTokenizer;
import org.apache.lucene.analysis.ar.ArabicNormalizationFilter;
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for Persian.
 * <p>
 * This Analyzer uses {@link ArabicLetterTokenizer} which implies tokenizing around
 * zero-width non-joiner in addition to whitespace. Some persian-specific variant forms (such as farsi
 * yeh and keheh) are standardized. "Stemming" is accomplished via stopwords.
 * </p>
 */
public final class PersianAnalyzer extends Analyzer {

  /**
   * File containing default Persian stopwords.
   * 
   * Default stopword list is from
   * http://members.unine.ch/jacques.savoy/clef/index.html The stopword list is
   * BSD-Licensed.
   * 
   */
  public final static String DEFAULT_STOPWORD_FILE = "stopwords.txt";

  /**
   * Contains the stopwords used with the StopFilter.
   */
  private final Set<?> stoptable;

  /**
   * The comment character in the stopwords file. All lines prefixed with this
   * will be ignored
   */
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
        DEFAULT_STOP_SET = loadDefaultStopWordSet();
      } catch (IOException ex) {
        // default set should always be present as it is part of the
        // distribution (JAR)
        throw new RuntimeException("Unable to load default stopword set");
      }
    }

    static Set<String> loadDefaultStopWordSet() throws IOException {
      InputStream stream = PersianAnalyzer.class
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
   * Builds an analyzer with the default stop words:
   * {@link #DEFAULT_STOPWORD_FILE}.
   */
  public PersianAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words 
   * 
   * @param matchversion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public PersianAnalyzer(Version matchVersion, Set<?> stopwords){
    stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
   */
  public PersianAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(stopwords));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
   */
  public PersianAnalyzer(Version matchVersion, Hashtable<?, ?> stopwords) {
    this(matchVersion, stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words. Lines can be commented out
   * using {@link #STOPWORDS_COMMENT}
   * @deprecated use {@link #PersianAnalyzer(Version, Set)} instead
   */
  public PersianAnalyzer(Version matchVersion, File stopwords) throws IOException {
    this(matchVersion, WordlistLoader.getWordSet(stopwords, STOPWORDS_COMMENT));
  }

  /**
   * Creates a {@link TokenStream} which tokenizes all the text in the provided
   * {@link Reader}.
   * 
   * @return A {@link TokenStream} built from a {@link ArabicLetterTokenizer}
   *         filtered with {@link LowerCaseFilter}, 
   *         {@link ArabicNormalizationFilter},
   *         {@link PersianNormalizationFilter} and Persian Stop words
   */
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new ArabicLetterTokenizer(reader);
    result = new LowerCaseFilter(result);
    result = new ArabicNormalizationFilter(result);
    /* additional persian-specific normalization */
    result = new PersianNormalizationFilter(result);
    /*
     * the order here is important: the stopword list is normalized with the
     * above!
     */
    result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                            result, stoptable);
    return result;
  }
  
  private class SavedStreams {
    Tokenizer source;
    TokenStream result;
  }

  /**
   * Returns a (possibly reused) {@link TokenStream} which tokenizes all the text 
   * in the provided {@link Reader}.
   * 
   * @return A {@link TokenStream} built from a {@link ArabicLetterTokenizer}
   *         filtered with {@link LowerCaseFilter}, 
   *         {@link ArabicNormalizationFilter},
   *         {@link PersianNormalizationFilter} and Persian Stop words
   */
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new ArabicLetterTokenizer(reader);
      streams.result = new LowerCaseFilter(streams.source);
      streams.result = new ArabicNormalizationFilter(streams.result);
      /* additional persian-specific normalization */
      streams.result = new PersianNormalizationFilter(streams.result);
      /*
       * the order here is important: the stopword list is normalized with the
       * above!
       */
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.result, stoptable);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
