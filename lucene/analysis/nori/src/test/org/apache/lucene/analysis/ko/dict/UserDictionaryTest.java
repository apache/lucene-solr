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
package org.apache.lucene.analysis.ko.dict;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.ko.POS;
import org.apache.lucene.analysis.ko.TestKoreanTokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class UserDictionaryTest extends LuceneTestCase {
  @Test
  public void testLookup() throws IOException {
    UserDictionary dictionary = TestKoreanTokenizer.readDict();
    String s = "세종";
    char[] sArray = s.toCharArray();
    List<Integer> wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(1, wordIds.size());
    assertNull(dictionary.getMorphemes(wordIds.get(0), sArray, 0, s.length()));

    s = "세종시";
    sArray = s.toCharArray();
    wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(2, wordIds.size());
    assertNull(dictionary.getMorphemes(wordIds.get(0), sArray, 0, s.length()));

    Dictionary.Morpheme[] decompound = dictionary.getMorphemes(wordIds.get(1), sArray, 0, s.length());
    assertNotNull(decompound);
    assertEquals(2, decompound.length);
    assertEquals(decompound[0].posTag, POS.Tag.NNG);
    assertEquals(decompound[0].surfaceForm, "세종");
    assertEquals(decompound[1].posTag, POS.Tag.NNG);
    assertEquals(decompound[1].surfaceForm, "시");

    s = "c++";
    sArray = s.toCharArray();
    wordIds = dictionary.lookup(sArray, 0, s.length());
    assertEquals(1, wordIds.size());
    assertNull(dictionary.getMorphemes(wordIds.get(0), sArray, 0, s.length()));
  }
  
  @Test
  public void testRead() {
    UserDictionary dictionary = TestKoreanTokenizer.readDict();
    assertNotNull(dictionary);
  }
}
