package org.apache.lucene.index;

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.WordlistLoader;

public class TestWordlistLoader extends LuceneTestCase {

  public void testWordlistLoading() throws IOException {
    String s = "ONE\n  two \nthree";
    HashSet wordSet1 = WordlistLoader.getWordSet(new StringReader(s));
    checkSet(wordSet1);
    HashSet wordSet2 = WordlistLoader.getWordSet(new BufferedReader(new StringReader(s)));
    checkSet(wordSet2);
  }
  
  private void checkSet(HashSet wordset) {
    assertEquals(3, wordset.size());
    assertTrue(wordset.contains("ONE"));		// case is not modified
    assertTrue(wordset.contains("two"));		// surrounding whitespace is removed
    assertTrue(wordset.contains("three"));
    assertFalse(wordset.contains("four"));
  }

}
