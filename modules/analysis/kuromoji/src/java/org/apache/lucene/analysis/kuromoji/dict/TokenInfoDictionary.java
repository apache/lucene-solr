package org.apache.lucene.analysis.kuromoji.dict;

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

import java.io.InputStream;
import java.io.IOException;
import java.io.FileNotFoundException;

import org.apache.lucene.analysis.kuromoji.trie.DoubleArrayTrie;

import org.apache.lucene.util.IOUtils;

public final class TokenInfoDictionary extends BinaryDictionary {

  public static final String TRIE_FILENAME_SUFFIX = "$trie.dat";

  private final DoubleArrayTrie trie;
  
  private TokenInfoDictionary() throws IOException {
    super();
    InputStream is = null;
    DoubleArrayTrie trie = null;
    try {
      is = getClass().getResourceAsStream(getClass().getSimpleName() + TRIE_FILENAME_SUFFIX);
      if (is == null)
        throw new FileNotFoundException("Not in classpath: " + getClass().getName().replace('.','/') + TRIE_FILENAME_SUFFIX);
      trie = new DoubleArrayTrie(is);
    } catch (IOException ioe) {
      throw new RuntimeException("Cannot load DoubleArrayTrie.", ioe);
    } finally {
      IOUtils.closeWhileHandlingException(is);
    }
    this.trie = trie;
  }
  
  public DoubleArrayTrie getTrie() {
    return trie;
  }
  
  public synchronized static TokenInfoDictionary getInstance() {
    if (singleton == null) try {
      singleton = new TokenInfoDictionary();
    } catch (IOException ioe) {
      throw new RuntimeException("Cannot load TokenInfoDictionary.", ioe);
    }
    return singleton;
  }
  
  private static TokenInfoDictionary singleton;
  
}
