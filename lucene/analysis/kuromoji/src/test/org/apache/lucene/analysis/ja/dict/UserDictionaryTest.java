package org.apache.lucene.analysis.ja.dict;

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

import java.io.IOException;

import org.apache.lucene.analysis.ja.TestJapaneseTokenizer;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class UserDictionaryTest extends LuceneTestCase {

  @Test
  public void testLookup() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    String s = "関西国際空港に行った";
    int[][] dictionaryEntryResult = dictionary.lookup(s.toCharArray(), 0, s.length());
    // Length should be three 関西, 国際, 空港
    assertEquals(3, dictionaryEntryResult.length);
    
    // Test positions
    assertEquals(0, dictionaryEntryResult[0][1]); // index of 関西
    assertEquals(2, dictionaryEntryResult[1][1]); // index of 国際
    assertEquals(4, dictionaryEntryResult[2][1]); // index of 空港
    
    // Test lengths
    assertEquals(2, dictionaryEntryResult[0][2]); // length of 関西
    assertEquals(2, dictionaryEntryResult[1][2]); // length of 国際
    assertEquals(2, dictionaryEntryResult[2][2]); // length of 空港
    
    s = "関西国際空港と関西国際空港に行った";
    int[][] dictionaryEntryResult2 = dictionary.lookup(s.toCharArray(), 0, s.length());
    // Length should be six 
    assertEquals(6, dictionaryEntryResult2.length);
  }
  
  @Test
  public void testReadings() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    int[][] result = dictionary.lookup("日本経済新聞".toCharArray(), 0, 6);
    assertEquals(3, result.length);
    int wordIdNihon = result[0][0]; // wordId of 日本 in 日本経済新聞
    assertEquals("ニホン", dictionary.getReading(wordIdNihon, "日本".toCharArray(), 0, 2));
    
    result = dictionary.lookup("朝青龍".toCharArray(), 0, 3);
    assertEquals(1, result.length);
    int wordIdAsashoryu = result[0][0]; // wordId for 朝青龍
    assertEquals("アサショウリュウ", dictionary.getReading(wordIdAsashoryu, "朝青龍".toCharArray(), 0, 3));
  }
  
  @Test
  public void testPartOfSpeech() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    int[][] result = dictionary.lookup("日本経済新聞".toCharArray(), 0, 6);
    assertEquals(3, result.length);
    int wordIdKeizai = result[1][0]; // wordId of 経済 in 日本経済新聞
    assertEquals("カスタム名詞", dictionary.getPartOfSpeech(wordIdKeizai));
  }
  
  @Test
  public void testRead() throws IOException {
    UserDictionary dictionary = TestJapaneseTokenizer.readDict();
    assertNotNull(dictionary);
  }
}
