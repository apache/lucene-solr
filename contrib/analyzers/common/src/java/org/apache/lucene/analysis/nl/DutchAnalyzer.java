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
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;

/**
 * Analyzer for Dutch language. Supports an external list of stopwords (words that
 * will not be indexed at all), an external list of exclusions (word that will
 * not be stemmed, but indexed) and an external list of word-stem pairs that overrule
 * the algorithm (dictionary stemming).
 * A default set of stopwords is used unless an alternative list is specified, the
 * exclusion list is empty by default.
 *
 */
public class DutchAnalyzer extends Analyzer {
  /**
   * List of typical Dutch stopwords.
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
   * Contains the stopwords used with the StopFilter.
   */
  private Set stoptable = new HashSet();

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private Set excltable = new HashSet();

  private Map stemdict = new HashMap();


  /**
   * Builds an analyzer with the default stop words ({@link #DUTCH_STOP_WORDS}) 
   * and a few default entries for the stem exclusion table.
   * 
   */
  public DutchAnalyzer() {
    stoptable = StopFilter.makeStopSet(DUTCH_STOP_WORDS);
    stemdict.put("fiets", "fiets"); //otherwise fiet
    stemdict.put("bromfiets", "bromfiets"); //otherwise bromfiet
    stemdict.put("ei", "eier");
    stemdict.put("kind", "kinder");
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   */
  public DutchAnalyzer(String[] stopwords) {
    stoptable = StopFilter.makeStopSet(stopwords);
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   */
  public DutchAnalyzer(HashSet stopwords) {
    stoptable = stopwords;
  }

  /**
   * Builds an analyzer with the given stop words.
   *
   * @param stopwords
   */
  public DutchAnalyzer(File stopwords) {
    try {
      stoptable = org.apache.lucene.analysis.WordlistLoader.getWordSet(stopwords);
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   *
   * @param exclusionlist
   */
  public void setStemExclusionTable(String[] exclusionlist) {
    excltable = StopFilter.makeStopSet(exclusionlist);
  }

  /**
   * Builds an exclusionlist from a Hashtable.
   */
  public void setStemExclusionTable(HashSet exclusionlist) {
    excltable = exclusionlist;
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   */
  public void setStemExclusionTable(File exclusionlist) {
    try {
      excltable = org.apache.lucene.analysis.WordlistLoader.getWordSet(exclusionlist);
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
      stemdict = org.apache.lucene.analysis.WordlistLoader.getStemDict(stemdictFile);
    } catch (IOException e) {
      // TODO: throw IOException
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates a TokenStream which tokenizes all the text in the provided TextReader.
   *
   * @return A TokenStream build from a StandardTokenizer filtered with StandardFilter,
   * StopFilter, DutchStemFilter
   */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new StandardTokenizer(reader);
    result = new StandardFilter(result);
    result = new StopFilter(result, stoptable);
    result = new DutchStemFilter(result, excltable, stemdict);
    return result;
  }
}
