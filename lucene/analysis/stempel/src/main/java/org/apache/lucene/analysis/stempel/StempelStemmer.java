/*
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
package org.apache.lucene.analysis.stempel;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import org.egothor.stemmer.Diff;
import org.egothor.stemmer.Trie;

/**
 * <p>
 * Stemmer class is a convenient facade for other stemmer-related classes. The
 * core stemming algorithm and its implementation is taken verbatim from the
 * Egothor project ( <a href="http://www.egothor.org">www.egothor.org </a>).
 * </p>
 * <p>
 * Even though the stemmer tables supplied in the distribution package are built
 * for Polish language, there is nothing language-specific here.
 * </p>
 */
public class StempelStemmer {
  private Trie stemmer = null;
  private StringBuilder buffer = new StringBuilder();

  /**
   * Create a Stemmer using selected stemmer table
   * 
   * @param stemmerTable stemmer table.
   */
  public StempelStemmer(InputStream stemmerTable) throws IOException {
    this(load(stemmerTable));
  }

  /**
   * Create a Stemmer using pre-loaded stemmer table
   * 
   * @param stemmer pre-loaded stemmer table
   */
  public StempelStemmer(Trie stemmer) {
    this.stemmer = stemmer;
  }
  
  /**
   * Load a stemmer table from an inputstream.
   */
  public static Trie load(InputStream stemmerTable) throws IOException {
    DataInputStream in = null;
    try {
      in = new DataInputStream(new BufferedInputStream(stemmerTable));
      String method = in.readUTF().toUpperCase(Locale.ROOT);
      if (method.indexOf('M') < 0) {
        return new org.egothor.stemmer.Trie(in);
      } else {
        return new org.egothor.stemmer.MultiTrie2(in);
      }
    } finally {
      in.close();
    }
  }

  /**
   * Stem a word. 
   * 
   * @param word input word to be stemmed.
   * @return stemmed word, or null if the stem could not be generated.
   */
  public StringBuilder stem(CharSequence word) {
    CharSequence cmd = stemmer.getLastOnPath(word);
    
    if (cmd == null)
        return null;
    
    buffer.setLength(0);
    buffer.append(word);

    Diff.apply(buffer, cmd);
    
    if (buffer.length() > 0)
      return buffer;
    else
      return null;
  }
}
