package org.apache.lucene.analysis;

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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Removes stop words from a token stream.
 */

public final class StopFilter extends TokenFilter {

  private final CharArraySet stopWords;
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
    this.stopWords = makeStopCharArraySet(stopWords, ignoreCase);
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
    this.stopWords = new CharArraySet(stopWords.size(), ignoreCase);
    Iterator it = stopWords.iterator();
    while(it.hasNext())
      this.stopWords.add((String) it.next());
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

  private static final CharArraySet makeStopCharArraySet(String[] stopWords, boolean ignoreCase) {
    CharArraySet stopSet = new CharArraySet(stopWords.length, ignoreCase);
    for (int i = 0; i < stopWords.length; i++)
      stopSet.add(ignoreCase ? stopWords[i].toLowerCase() : stopWords[i]);
    return stopSet;
  }

  /**
   * Returns the next input Token whose termText() is not a stop word.
   */
  public final Token next(Token result) throws IOException {
    // return the first non-stop word found
    while((result = input.next(result)) != null) {
      if (!stopWords.contains(result.termBuffer(), result.termLength))
        return result;
    }
    // reached EOS -- return null
    return null;
  }
}
