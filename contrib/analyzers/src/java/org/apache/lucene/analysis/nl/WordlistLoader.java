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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.HashMap;

/**
 *         <p/>
 *         Loads a text file and adds every line as an entry to a Hashtable. Every line
 *         should contain only one word. If the file is not found or on any error, an
 *         empty table is returned.
 *
 * @deprecated use {@link org.apache.lucene.analysis.WordlistLoader} instead
 */
public class WordlistLoader {
  /**
   * @param path     Path to the wordlist
   * @param wordfile Name of the wordlist
   * @deprecated use {@link org.apache.lucene.analysis.WordlistLoader#getWordSet(File)} instead
   */
  public static HashMap getWordtable(String path, String wordfile) {
    if (path == null || wordfile == null) {
      return new HashMap();
    }
    return getWordtable(new File(path, wordfile));
  }

  /**
   * @param wordfile Complete path to the wordlist
   * @deprecated use {@link org.apache.lucene.analysis.WordlistLoader#getWordSet(File)} instead
   */
  public static HashMap getWordtable(String wordfile) {
    if (wordfile == null) {
      return new HashMap();
    }
    return getWordtable(new File(wordfile));
  }

  /**
   * Reads a stemsdictionary. Each line contains:
   * word \t stem
   * i.e. tab seperated)
   *
   * @return Stem dictionary that overrules, the stemming algorithm
   * @deprecated use {@link org.apache.lucene.analysis.WordlistLoader#getStemDict(File)} instead
   */
  public static HashMap getStemDict(File wordstemfile) {
    if (wordstemfile == null) {
      return new HashMap();
    }
    HashMap result = new HashMap();
    try {
      LineNumberReader lnr = new LineNumberReader(new FileReader(wordstemfile));
      String line;
      String[] wordstem;
      while ((line = lnr.readLine()) != null) {
        wordstem = line.split("\t", 2);
        result.put(wordstem[0], wordstem[1]);
      }
    } catch (IOException e) {
    }
    return result;
  }

  /**
   * @param wordfile File containing the wordlist
   * @deprecated use {@link org.apache.lucene.analysis.WordlistLoader#getWordSet(File)} instead
   */
  public static HashMap getWordtable(File wordfile) {
    if (wordfile == null) {
      return new HashMap();
    }
    HashMap result = null;
    try {
      LineNumberReader lnr = new LineNumberReader(new FileReader(wordfile));
      String word = null;
      String[] stopwords = new String[100];
      int wordcount = 0;
      while ((word = lnr.readLine()) != null) {
        wordcount++;
        if (wordcount == stopwords.length) {
          String[] tmp = new String[stopwords.length + 50];
          System.arraycopy(stopwords, 0, tmp, 0, wordcount);
          stopwords = tmp;
        }
        stopwords[wordcount - 1] = word;
      }
      result = makeWordTable(stopwords, wordcount);
    }
        // On error, use an empty table
    catch (IOException e) {
      result = new HashMap();
    }
    return result;
  }

  /**
   * Builds the wordlist table.
   *
   * @param words  Word that where read
   * @param length Amount of words that where read into <tt>words</tt>
   */
  private static HashMap makeWordTable(String[] words, int length) {
    HashMap table = new HashMap(length);
    for (int i = 0; i < length; i++) {
      table.put(words[i], words[i]);
    }
    return table;
  }
}