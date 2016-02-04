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
package org.apache.lucene.analysis.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

import org.apache.lucene.util.LuceneTestCase;

import org.apache.lucene.analysis.util.WordlistLoader;

public class TestWordlistLoader extends LuceneTestCase {

  public void testWordlistLoading() throws IOException {
    String s = "ONE\n  two \nthree";
    CharArraySet wordSet1 = WordlistLoader.getWordSet(new StringReader(s));
    checkSet(wordSet1);
    CharArraySet wordSet2 = WordlistLoader.getWordSet(new BufferedReader(new StringReader(s)));
    checkSet(wordSet2);
  }

  public void testComments() throws Exception {
    String s = "ONE\n  two \nthree\n#comment";
    CharArraySet wordSet1 = WordlistLoader.getWordSet(new StringReader(s), "#");
    checkSet(wordSet1);
    assertFalse(wordSet1.contains("#comment"));
    assertFalse(wordSet1.contains("comment"));
  }


  private void checkSet(CharArraySet wordset) {
    assertEquals(3, wordset.size());
    assertTrue(wordset.contains("ONE"));  // case is not modified
    assertTrue(wordset.contains("two"));  // surrounding whitespace is removed
    assertTrue(wordset.contains("three"));
    assertFalse(wordset.contains("four"));
  }

  /**
   * Test stopwords in snowball format
   */
  public void testSnowballListLoading() throws IOException {
    String s = 
      "|comment\n" + // commented line
      " |comment\n" + // commented line with leading whitespace
      "\n" + // blank line
      "  \t\n" + // line with only whitespace
      " |comment | comment\n" + // commented line with comment
      "ONE\n" + // stopword, in uppercase
      "   two   \n" + // stopword with leading/trailing space
      " three   four five \n" + // multiple stopwords
      "six seven | comment\n"; //multiple stopwords + comment
    CharArraySet wordset = WordlistLoader.getSnowballWordSet(new StringReader(s));
    assertEquals(7, wordset.size());
    assertTrue(wordset.contains("ONE"));
    assertTrue(wordset.contains("two"));
    assertTrue(wordset.contains("three"));
    assertTrue(wordset.contains("four"));
    assertTrue(wordset.contains("five"));
    assertTrue(wordset.contains("six"));
    assertTrue(wordset.contains("seven"));
  }
}
