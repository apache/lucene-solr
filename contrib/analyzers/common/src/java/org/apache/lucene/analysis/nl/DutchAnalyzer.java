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
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;  // for javadoc
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
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
 * <p><b>NOTE</b>: This class uses the same {@link Version}
 * dependent settings as {@link StandardAnalyzer}.</p>
 */
public class DutchAnalyzer extends Analyzer {
  /**
   * List of typical Dutch stopwords.
   * @deprecated use {@link #getDefaultStopSet()} instead
   */
  public final static String[] DUTCH_STOP_WORDS =
      {
        "de", "en", "van", "ik", "te", "dat", "die", "in", "een",
        "hij", "het", "niet", "zijn", "is", "was", "op", "aan", "met", "als", "voor", "had",
        "er", "maar", "om", "hem", "dan", "zou", "of", "wat", "mijn", "men", "dit", "zo",
        "door", "over", "ze", "zich", "bij", "ook", "tot", "je", "mij", "uit", "der", "daar",
        "haar", "naar", "heb", "hoe", "heeft", "hebben", "deze", "u", "want", "nog", "zal",
        "me", "zij", "nu", "ge", "geen", "omdat", "iets", "worden", "toch", "al", "waren",
        "veel", "meer", "doen", "toen", "moet", "ben", "zonder", "kan", "hun", "dus",
        "alles", "onder", "ja", "eens", "hier", "wie", "werd", "altijd", "doch", "wordt",
        "wezen", "kunnen", "ons", "zelf", "tegen", "na", "reeds", "wil", "kon", "niets",
        "uw", "iemand", "geweest", "andere"
      };
  /**
   * Returns an unmodifiable instance of the default stop-words set.
   * @return an unmodifiable instance of the default stop-words set.
   */
  public static Set<?> getDefaultStopSet(){
    return DefaultSetHolder.DEFAULT_STOP_SET;
  }
  
  private static class DefaultSetHolder {
    static final Set<?> DEFAULT_STOP_SET = CharArraySet
        .unmodifiableSet(new CharArraySet(Arrays.asList(DUTCH_STOP_WORDS),
            false));
  }


  /**
   * Contains the stopwords used with the StopFilter.
   */
  private final Set<?> stoptable;

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private Set<?> excltable = Collections.emptySet();

  private Map<String, String> stemdict = new HashMap<String, String>();
  private final Version matchVersion;

  /**
   * Builds an analyzer with the default stop words ({@link #DUTCH_STOP_WORDS}) 
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
    stoptable = CharArraySet.unmodifiableSet(CharArraySet.copy(stopwords));
    excltable = CharArraySet.unmodifiableSet(CharArraySet.copy(stemExclusionTable));
    this.matchVersion = matchVersion;
    setOverridesTokenStreamMethod(DutchAnalyzer.class);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param matchVersion
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  public DutchAnalyzer(Version matchVersion, String... stopwords) {
    this(matchVersion, StopFilter.makeStopSet(stopwords));
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  public DutchAnalyzer(Version matchVersion, HashSet<?> stopwords) {
    this(matchVersion, (Set<?>)stopwords);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   * @deprecated use {@link #DutchAnalyzer(Version, Set)} instead
   */
  public DutchAnalyzer(Version matchVersion, File stopwords) {
    // this is completely broken!
    setOverridesTokenStreamMethod(DutchAnalyzer.class);
    try {
      stoptable = org.apache.lucene.analysis.WordlistLoader.getWordSet(stopwords);
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
  public void setStemExclusionTable(String... exclusionlist) {
    excltable = StopFilter.makeStopSet(exclusionlist);
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from a Hashtable.
   * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
   */
  public void setStemExclusionTable(HashSet<?> exclusionlist) {
    excltable = exclusionlist;
    setPreviousTokenStream(null); // force a new stemmer to be created
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   * @deprecated use {@link #DutchAnalyzer(Version, Set, Set)} instead
   */
  public void setStemExclusionTable(File exclusionlist) {
    try {
      excltable = org.apache.lucene.analysis.WordlistLoader.getWordSet(exclusionlist);
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
   */
  public void setStemDictionary(File stemdictFile) {
    try {
      stemdict = WordlistLoader.getStemDict(stemdictFile);
      setPreviousTokenStream(null); // force a new stemmer to be created
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a {@link TokenStream} which tokenizes all the text in the 
   * provided {@link Reader}.
   *
   * @return A {@link TokenStream} built from a {@link StandardTokenizer}
   *   filtered with {@link StandardFilter}, {@link StopFilter}, 
   *   and {@link DutchStemFilter}
   */
  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new StandardTokenizer(matchVersion, reader);
    result = new StandardFilter(result);
    result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                            result, stoptable);
    result = new DutchStemFilter(result, excltable, stemdict);
    return result;
  }
  
  private class SavedStreams {
    Tokenizer source;
    TokenStream result;
  };
  
  /**
   * Returns a (possibly reused) {@link TokenStream} which tokenizes all the 
   * text in the provided {@link Reader}.
   *
   * @return A {@link TokenStream} built from a {@link StandardTokenizer}
   *   filtered with {@link StandardFilter}, {@link StopFilter}, 
   *   and {@link DutchStemFilter}
   */
  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
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
      streams.result = new StopFilter(StopFilter.getEnablePositionIncrementsVersionDefault(matchVersion),
                                      streams.result, stoptable);
      streams.result = new DutchStemFilter(streams.result, excltable, stemdict);
      setPreviousTokenStream(streams);
    } else {
      streams.source.reset(reader);
    }
    return streams.result;
  }
}
