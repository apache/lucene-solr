package org.apache.lucene.analysis.de;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.io.Reader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Analyzer for German language. Supports an external list of stopwords (words that
 * will not be indexed at all) and an external list of exclusions (word that will
 * not be stemmed, but indexed).
 * A default set of stopwords is used unless an alternative list is specified, the
 * exclusion list is empty by default.
 *
 * @author Gerhard Schwarz
 * @version $Id$
 */
public class GermanAnalyzer extends Analyzer {
  /**
   * List of typical german stopwords.
   */
  private String[] GERMAN_STOP_WORDS = {
    "einer", "eine", "eines", "einem", "einen",
    "der", "die", "das", "dass", "da�",
    "du", "er", "sie", "es",
    "was", "wer", "wie", "wir",
    "und", "oder", "ohne", "mit",
    "am", "im", "in", "aus", "auf",
    "ist", "sein", "war", "wird",
    "ihr", "ihre", "ihres",
    "als", "f�r", "von", "mit",
    "dich", "dir", "mich", "mir",
    "mein", "sein", "kein",
    "durch", "wegen", "wird"
  };

  /**
   * Contains the stopwords used with the StopFilter.
   */
  private Set stopSet = new HashSet();

  /**
   * Contains words that should be indexed but not stemmed.
   */
  private Set exclusionSet = new HashSet();

  /**
   * Builds an analyzer.
   */
  public GermanAnalyzer() {
    stopSet = StopFilter.makeStopSet(GERMAN_STOP_WORDS);
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public GermanAnalyzer(String[] stopwords) {
    stopSet = StopFilter.makeStopSet(stopwords);
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public GermanAnalyzer(Hashtable stopwords) {
    stopSet = new HashSet(stopwords.keySet());
  }

  /**
   * Builds an analyzer with the given stop words.
   */
  public GermanAnalyzer(File stopwords) throws IOException {
    stopSet = new HashSet(WordlistLoader.getWordtable(stopwords).keySet());
  }

  /**
   * Builds an exclusionlist from an array of Strings.
   */
  public void setStemExclusionTable(String[] exclusionlist) {
    exclusionSet = StopFilter.makeStopSet(exclusionlist);
  }

  /**
   * Builds an exclusionlist from a Hashtable.
   */
  public void setStemExclusionTable(Hashtable exclusionlist) {
    exclusionSet = new HashSet(exclusionlist.keySet());
  }

  /**
   * Builds an exclusionlist from the words contained in the given file.
   */
  public void setStemExclusionTable(File exclusionlist) throws IOException {
    exclusionSet = new HashSet(WordlistLoader.getWordtable(exclusionlist).keySet());
  }

  /**
   * Creates a TokenStream which tokenizes all the text in the provided Reader.
   *
   * @return A TokenStream build from a StandardTokenizer filtered with
   *         StandardFilter, StopFilter, GermanStemFilter
   */
  public TokenStream tokenStream(String fieldName, Reader reader) {
    TokenStream result = new StandardTokenizer(reader);
    result = new StandardFilter(result);
// shouldn't there be a lowercaser before stop word filtering?
    result = new StopFilter(result, stopSet);
    result = new GermanStemFilter(result, exclusionSet);
    return result;
  }
}
