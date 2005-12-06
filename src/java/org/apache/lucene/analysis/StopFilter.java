package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;

/**
 * Removes stop words from a token stream.
 */

public final class StopFilter extends TokenFilter {

  private final Set stopWords;
  private final boolean ignoreCase;

    /**
     * Construct a token stream filtering the given input.
     */
    public StopFilter(TokenStream input, String [] stopWords)
    {
        this(input, stopWords, false);
    }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the array of words.
   */
  public StopFilter(TokenStream in, String[] stopWords, boolean ignoreCase) {
    super(in);
    this.ignoreCase = ignoreCase;
    this.stopWords = makeStopSet(stopWords, ignoreCase);
  }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Hashtable.
   *
   * @deprecated Use {@link #StopFilter(TokenStream, Set)} instead
   */ 
  public StopFilter(TokenStream in, Hashtable stopTable) {
    this(in, stopTable, false);
  }
  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Hashtable.
   * If ignoreCase is true, all keys in the stopTable should already
   * be lowercased.
   * @deprecated Use {@link #StopFilter(TokenStream, Set)} instead
   */
  public StopFilter(TokenStream in, Hashtable stopTable, boolean ignoreCase) {
    this(in, stopTable.keySet(), ignoreCase);
  }

    /**
     * Construct a token stream filtering the given input.
     * @param input
     * @param stopWords The set of Stop Words, as Strings.  If ignoreCase is true, all strings should be lower cased
     * @param ignoreCase -Ignore case when stopping.  The stopWords set must be setup to contain only lower case words 
     */
    public StopFilter(TokenStream input, Set stopWords, boolean ignoreCase)
    {
        super(input);
        this.ignoreCase = ignoreCase;
        this.stopWords = stopWords;
    }

  /**
   * Constructs a filter which removes words from the input
   * TokenStream that are named in the Set.
   * It is crucial that an efficient Set implementation is used
   * for maximum performance.
   *
   * @see #makeStopSet(java.lang.String[])
   */
  public StopFilter(TokenStream in, Set stopWords) {
    this(in, stopWords, false);
  }
  /**
   * Builds a Hashtable from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this table construction to be cached once when
   * an Analyzer is constructed.
   *
   * @deprecated Use {@link #makeStopSet(String[])} instead.
   */
  public static final Hashtable makeStopTable(String[] stopWords) {
      return makeStopTable(stopWords, false);
  }
    
    /**
    * Builds a Hashtable from an array of stop words,
    * appropriate for passing into the StopFilter constructor.
    * This permits this table construction to be cached once when
    * an Analyzer is constructed.
  * @deprecated Use {@link #makeStopSet(java.lang.String[], boolean)}  instead.
 */ 
  public static final Hashtable makeStopTable(String [] stopWords, boolean ignoreCase) {
      Hashtable stopTable = new Hashtable(stopWords.length);                                
      for (int i = 0; i < stopWords.length; i++)
      {
          String stopWord = ignoreCase ? stopWords[i].toLowerCase() : stopWords[i];
          stopTable.put(stopWord, stopWord);
      }
    return stopTable;
  }

  /**
   * Builds a Set from an array of stop words,
   * appropriate for passing into the StopFilter constructor.
   * This permits this stopWords construction to be cached once when
   * an Analyzer is constructed.
   * 
   * @see #makeStopSet(java.lang.String[], boolean) passing false to ignoreCase
   */
  public static final Set makeStopSet(String[] stopWords) {
    return makeStopSet(stopWords, false);
  }
    
  /**
   * 
    * @param stopWords
   * @param ignoreCase If true, all words are lower cased first.  
   * @return a Set containing the words
   */    
  public static final Set makeStopSet(String[] stopWords, boolean ignoreCase) {
    HashSet stopTable = new HashSet(stopWords.length);
    for (int i = 0; i < stopWords.length; i++)
      stopTable.add(ignoreCase ? stopWords[i].toLowerCase() : stopWords[i]);
    return stopTable;
  }    

  /**
   * Returns the next input Token whose termText() is not a stop word.
   */
  public final Token next() throws IOException {
    // return the first non-stop word found
    for (Token token = input.next(); token != null; token = input.next())
    {
        String termText = ignoreCase ? token.termText.toLowerCase() : token.termText;
        if (!stopWords.contains(termText))
          return token;
    }
    // reached EOS -- return null
    return null;
  }
}
