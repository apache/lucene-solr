package org.apache.lucene.analysis.hunspell2;

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

import org.apache.lucene.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

public class TestStemmer extends LuceneTestCase {
  private static Stemmer stemmer;

  @BeforeClass
  public static void beforeClass() throws Exception {
    try (InputStream affixStream = TestStemmer.class.getResourceAsStream("simple.aff");
        InputStream dictStream = TestStemmer.class.getResourceAsStream("simple.dic")) {
     Dictionary dictionary = new Dictionary(affixStream, dictStream);
     stemmer = new Stemmer(dictionary);
   }
  }
  
  @AfterClass
  public static void afterClass() {
    stemmer = null;
  }

  public void testSimpleSuffix() {
    assertStemsTo("lucene", "lucene", "lucen");
    assertStemsTo("mahoute", "mahout");
  }

  public void testSimplePrefix() {
    assertStemsTo("solr", "olr");
  }

  public void testRecursiveSuffix() {
    assertStemsTo("abcd", "ab");
  }

  // all forms unmunched from dictionary
  public void testAllStems() {
    assertStemsTo("ab", "ab");
    assertStemsTo("abc", "ab");
    assertStemsTo("apach", "apach");
    assertStemsTo("apache", "apach");
    assertStemsTo("foo", "foo");
    assertStemsTo("food", "foo");
    assertStemsTo("foos", "foo");
    assertStemsTo("lucen", "lucen");
    assertStemsTo("lucene", "lucen", "lucene");
    assertStemsTo("mahout", "mahout");
    assertStemsTo("mahoute", "mahout");
    assertStemsTo("moo", "moo");
    assertStemsTo("mood", "moo");
    assertStemsTo("olr", "olr");
    assertStemsTo("solr", "olr");
  }
  
  // some bogus stuff that should not stem (empty lists)!
  public void testBogusStems() {    
    assertStemsTo("abs");
    assertStemsTo("abe");
    assertStemsTo("sab");
    assertStemsTo("sapach");
    assertStemsTo("sapache");
    assertStemsTo("apachee");
    assertStemsTo("sfoo");
    assertStemsTo("sfoos");
    assertStemsTo("fooss");
    assertStemsTo("lucenee");
    assertStemsTo("solre");
  }
  
  private void assertStemsTo(String s, String... expected) {
    Arrays.sort(expected);
    
    List<Stem> stems = stemmer.stem(s);
    String actual[] = new String[stems.size()];
    for (int i = 0; i < actual.length; i++) {
      actual[i] = stems.get(i).getStemString();
    }
    Arrays.sort(actual);
    
    assertArrayEquals(expected, actual);
  }
}
