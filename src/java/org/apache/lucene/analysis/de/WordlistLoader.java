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

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Hashtable;

/**
 * Loads a text file and adds every line as an entry to a Hashtable. Every line
 * should contain only one word.
 *
 * @author Gerhard Schwarz
 * @version $Id$
 * @todo refactor to convert to Sets instead of Hashtable
 */
public class WordlistLoader {
  /**
   * @param path     Path to the wordlist
   * @param wordfile Name of the wordlist
   */
  public static Hashtable getWordtable(String path, String wordfile) throws IOException {
    if (path == null || wordfile == null) {
      return new Hashtable();
    }
    return getWordtable(new File(path, wordfile));
  }

  /**
   * @param wordfile Complete path to the wordlist
   */
  public static Hashtable getWordtable(String wordfile) throws IOException {
    if (wordfile == null) {
      return new Hashtable();
    }
    return getWordtable(new File(wordfile));
  }

  /**
   * @param wordfile File containing the wordlist
   * @todo Create a Set version of this method
   */
  public static Hashtable getWordtable(File wordfile) throws IOException {
    if (wordfile == null) {
      return new Hashtable();
    }
    Hashtable result = null;
    FileReader freader = null;
    LineNumberReader lnr = null;
    try {
      freader = new FileReader(wordfile);
      lnr = new LineNumberReader(freader);
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
    } finally {
      if (lnr != null)
        lnr.close();
      if (freader != null)
        freader.close();
    }
    return result;
  }

  /**
   * Builds the wordlist table.
   *
   * @param words  Word that where read
   * @param length Amount of words that where read into <tt>words</tt>
   */
  private static Hashtable makeWordTable(String[] words, int length) {
    Hashtable table = new Hashtable(length);
    for (int i = 0; i < length; i++) {
      table.put(words[i], words[i]);
    }
    return table;
  }
}
