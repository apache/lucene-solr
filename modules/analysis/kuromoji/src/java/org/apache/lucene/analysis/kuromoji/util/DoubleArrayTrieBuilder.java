package org.apache.lucene.analysis.kuromoji.util;

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

import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.kuromoji.trie.DoubleArrayTrie;
import org.apache.lucene.analysis.kuromoji.trie.Trie;

public class DoubleArrayTrieBuilder {
  
  
  public DoubleArrayTrieBuilder() {
    
  }
  
  public static DoubleArrayTrie build(Set<Entry<Integer, String>> entries) {
    Trie tempTrie = buildTrie(entries);
    DoubleArrayTrie daTrie = new DoubleArrayTrie();
    daTrie.build(tempTrie);
    return daTrie;
  }
  
  public static Trie buildTrie(Set<Entry<Integer, String>> entries) {
    Trie trie = new Trie();
    for (Entry<Integer, String> entry : entries) {
      String surfaceForm = entry.getValue();
      trie.add(surfaceForm);
    }
    return trie;
  }
}
