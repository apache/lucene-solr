package org.apache.lucene.analysis.de;
// This file is encoded in UTF-8

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.util.Version;

/**
 * {@link Analyzer} for German language. 
 * <p>
 * Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, but the
 * exclusion list is empty by default.
 * </p>
 * 
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public class GermanAnalyzer extends Analyzer {
  
  /**
   * List of typical german stopwords.
   * @deprecated use {@link #getDefaultStopSet()} instead
   */
  //TODO make this private in 3.1
  public final static String[] GERMAN_STOP_WORDS = {
    "einer", "eine", "eines", "einem", "einen",
    "der", "die", "das", "dass", "daß",
    "du", "er", "sie", "es",
    "was", "wer", "wie", "wir",
    "und", "oder", "ohne", "mit",
    "am", "im", "in", "aus", "auf",
    "ist", "sein", "war", "wird",
    "ihr", "ihre", "ihres",
    "als", "für", "von", "mit",
    "dich", "dir", "mich", "mir",
    "mein", "sein", "kein",
    "durch", "wegen", "wird"
  };
  
  /**
   * Returns a set of default German-stopwords 
   * @return a set of default German-stopwords 
   */
  public static final Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_SET;
  }
  
  private static class DefaultSetHolder {
    private static final Set<?> DEFAULT_SET = CharArraySet.unmodifiableSet(new CharArraySet(
        Arrays.asList(GERMAN_STOP_WORDS), false));
  }

  /**
   * Contains the stopwords used with the {@link StopFilter}.
   */
  //TODO make this final in 3.1
  private Set<?> stopSet;

  /**
   * Contains words that should be indexed but not stemmed.
   */
  // TODO make this final in 3.1
  private Set<?> exclusionSet;

  private final Version matchVersion;

  /**
   * Builds an analyzer with the default stop words:
   * {@link #getDefaultStopSet()}.
   */
  public GermanAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words 
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   */
  public GermanAnalyzer(Version matchVersion, Set<?> stopwords) {
    this(matchVersion, stopwords, CharArraySet.EMPTY_SET);
  }
  
  /**
   * Builds an analyzer with the given stop words
   * 
   * @param matchVersion
   *          lucene compatibility version
   * @param stopwords
   *          a stopword set
   * @param stemExclusionSet
   *          a stemming exclusion set
   */
  public GermanAnalyzer(Version matchVersion, Set<?> stopwords, Set<?> stemExclusionSet) {
    stopSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    exclusionSet = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionSet));
    setOverridesTokenStreamMethod(GermanAnalyzer.class);
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #GermanAnalyzer(Version, Set)}
   */
  public GermanAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(stopwords));
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #GermanAnalyzer(Version, Set)}
   */
  public GermanAnalyzer(Version matchVersion, Map<?,?> stopwords) {
    this(matchVersion, stopwords.keySet());
    
  }

  /**
   * Builds an analyzer with the given stop words.
   * @deprecated use {@link #GermanAnalyzer(Version, Set)}
   */
  public GermanAnalyzer(Version matchVersion, File stopwords) throws IOException {
    this(matchVersion, WordlistLoader.getWordSet(stopwords));
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   * @deprecated use {@link #GermanAnalyzer(Version, Set, Set)} instead
   */
  public void setStemExclusionTable(String[] exclusionlist) {
    exclusionSet = StopFilter.makeStopSet(exclusionlist);
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from a {@link Map}
   * @deprecated use {@link #GermanAnalyzer(Version, Set, Set)} instead
   */
  public void setStemExclusionTable(Map exclusionlist) {
    exclusionSet = new HashSet(exclusionlist.keySet());
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   * @deprecated use {@link #GermanAnalyzer(Version, Set, Set)} instead
   */
  public void setStemExclusionTable(File exclusionlist) throws IOException {
    exclusionSet = WordlistLoader.getWordSet(exclusionlist);
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Creates a {@link TokenStream} which tokenizes all the text in the provided {@link Reader}.
   *
   * @return A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}, and
   *         {@link GermanStemFilter}
   */
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new StandardTokenizer(matchVersion, reader);
    result = new StandardFilter(result);
    result = new LowerCaseFilter(matchVersion, result);
    result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                            result, stopSet);
    result = new GermanStemFilter(result, exclusionSet);
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
   * @return A {@link TokenStream} built from a {@link StandardTokenizer} filtered with
   *         {@link StandardFilter}, {@link LowerCaseFilter}, {@link StopFilter}, and
   *         {@link GermanStemFilter}
   */
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader) throws IOException {
    if (overridesTokenStreamMethod) {
      // LUCENE-1678: force fallback to tokenStream() if we
      // have been subclassed and that subclass overrides
      // tokenStream but not reusableTokenStream
      return tokenStream(fieldName, reader);
    }
    
    SavedStreams streams = (SavedStreams) getPreviousTokenStream();
    if (streams == null) {
      streams = new SavedStreams();
      streams.source = new StandardTokenizer(matchVersion, reader);
      streams.result = new StandardFilter(streams.source);
      streams.result = new LowerCaseFilter(matchVersion, streams.result);
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.result, stopSet);
      streams.result = new GermanStemFilter(streams.result, exclusionSet);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
