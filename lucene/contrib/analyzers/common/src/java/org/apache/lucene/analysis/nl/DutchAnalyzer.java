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
import org.apache.lucene.analysis.CharArrayMap;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.KeywordMarkerFilter;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.ReusableAnalyzerBase;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
public final class DutchAnalyzer extends ReusableAnalyzerBase {
  /**
   * List of typical Dutch stopwords.
   * @deprecated use {@link #getDefaultStopSet()} instead
   */
  @Deprecated
  public final static String[] DUTCH_STOP_WORDS;
  
  /** File containing default Dutch stopwords. */
  public final static String DEFAULT_STOPWORD_FILE = "dutch_stop.txt";
  
  static {
    Set<?> defaultStopSet =  getDefaultStopSet();
    DUTCH_STOP_WORDS = new String[defaultStopSet.size()];
    int i = 0;
    for (Object object: defaultStopSet) {
      DUTCH_STOP_WORDS[i++] = new String((char[])object);
    } // what a hack!
  }

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
        DEFAULT_STOP_SET = WordlistLoader.getSnowballWordSet(IOUtils.getDecodingReader(SnowballFilter.class, 
            DEFAULT_STOPWORD_FILE, IOUtils.CHARSET_UTF_8), Version.LUCENE_CURRENT);
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

  private Map<Object,String>  stemdict = CharArrayMap.emptyMap();
  private final Version matchVersion;

  /**
   * Builds an analyzer with the default stop words ({@link #getDefaultStopSet()}) 
   * and a few default entries for the stem exclusion table.
   * 
   */
  public DutchAnalyzer(Version matchVersion) {
    this(matchVersion, DefaultSetHolder.DEFAULT_STOP_SET);
    stemdict = new CharArrayMap<String>(matchVersion, 16, false);
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
   * Builds an analyzer with the given stop words.
   *
   * @param matchVersion
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public DutchAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(matchVersion, stopwords));
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public DutchAnalyzer(Version matchVersion, HashSet<?> stopwords) {
    this(matchVersion, (Set<?>)stopwords);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  @Deprecated
  public DutchAnalyzer(Version matchVersion, File stopwords) {
    // this is completely broken!
    try {
      stoptable = WordlistLoader.getWordSet(IOUtils.getDecodingReader(stopwords,
          IOUtils.CHARSET_UTF_8), matchVersion);
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
    this.matchVersion = matchVersion;
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   *
   * @param exclusionlist
   * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(String... exclusionlist) {
    excltable = StopFilter.makeStopSet(matchVersion, exclusionlist);
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from a Hashtable.
   * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(HashSet<?> exclusionlist) {
    excltable = exclusionlist;
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
   */
  @Deprecated
  public void setStemExclusionTable(File exclusionlist) {
    try {
      
      excltable = WordlistLoader.getWordSet(IOUtils.getDecodingReader(exclusionlist,
          IOUtils.CHARSET_UTF_8), matchVersion);
      setPreviousTokenStream(null); // force a new stemmer to be created
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads a stemdictionary file , that overrules the stemming algorithm
   * This is a textfile that contains per line
   * <tt>word<b>\t</b>stem</tt>, i.e: two tab seperated words
   * @deprecated This prevents reuse of TokenStreams.  If you wish to use a custom
   * stem dictionary, create your own Analyzer with {@link StemmerOverrideFilter}
   */
  @Deprecated
  public void setStemDictionary(File stemdictFile) {
    try {
      stemdict = WordlistLoader.getStemDict(IOUtils.getDecodingReader(stemdictFile,
          IOUtils.CHARSET_UTF_8), new CharArrayMap<String>(matchVersion, 16, false));
      setPreviousTokenStream(null); // force a new stemmer to be created
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
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
