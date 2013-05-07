package org.apache.lucene.analysis.miscellaneous;
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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.StemmerOverrideFilter.StemmerOverrideMap;
import org.apache.lucene.util._TestUtil;

/**
 * 
 */
public class TestStemmerOverrideFilter extends BaseTokenStreamTestCase {
  public void testOverride() throws IOException {
    // lets make booked stem to books
    // the override filter will convert "booked" to "books",
    // but also mark it with KeywordAttribute so Porter will not change it.
    StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder();
    builder.add("booked", "books");
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader("booked"));
    TokenStream stream = new PorterStemFilter(new StemmerOverrideFilter(
        tokenizer, builder.build()));
    assertTokenStreamContents(stream, new String[] {"books"});
  }
  
  public void testIgnoreCase() throws IOException {
    // lets make booked stem to books
    // the override filter will convert "booked" to "books",
    // but also mark it with KeywordAttribute so Porter will not change it.
    StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(true);
    builder.add("boOkEd", "books");
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader("BooKeD"));
    TokenStream stream = new PorterStemFilter(new StemmerOverrideFilter(
        tokenizer, builder.build()));
    assertTokenStreamContents(stream, new String[] {"books"});
  }

  public void testNoOverrides() throws IOException {
    StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(true);
    Tokenizer tokenizer = new KeywordTokenizer(new StringReader("book"));
    TokenStream stream = new PorterStemFilter(new StemmerOverrideFilter(
        tokenizer, builder.build()));
    assertTokenStreamContents(stream, new String[] {"book"});
  }
  
  public void testRandomRealisticWhiteSpace() throws IOException {
    Map<String,String> map = new HashMap<String,String>();
    int numTerms = atLeast(50);
    for (int i = 0; i < numTerms; i++) {
      String randomRealisticUnicodeString = _TestUtil
          .randomRealisticUnicodeString(random());
      char[] charArray = randomRealisticUnicodeString.toCharArray();
      StringBuilder builder = new StringBuilder();
      for (int j = 0; j < charArray.length;) {
        int cp = Character.codePointAt(charArray, j);
        if (!Character.isWhitespace(cp)) {
          builder.appendCodePoint(cp);
        }
        j += Character.charCount(cp);
      }
      if (builder.length() > 0) {
        String value = _TestUtil.randomSimpleString(random());
        map.put(builder.toString(),
            value.isEmpty() ? "a" : value);
        
      }
    }
    if (map.isEmpty()) {
      map.put("booked", "books");
    }
    StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(random().nextBoolean());
    Set<Entry<String,String>> entrySet = map.entrySet();
    StringBuilder input = new StringBuilder();
    List<String> output = new ArrayList<String>();
    for (Entry<String,String> entry : entrySet) {
      builder.add(entry.getKey(), entry.getValue());
      if (random().nextBoolean() || output.isEmpty()) {
        input.append(entry.getKey()).append(" ");
        output.add(entry.getValue());
      }
    }
    Tokenizer tokenizer = new WhitespaceTokenizer(TEST_VERSION_CURRENT,
        new StringReader(input.toString()));
    TokenStream stream = new PorterStemFilter(new StemmerOverrideFilter(
        tokenizer, builder.build()));
    assertTokenStreamContents(stream, output.toArray(new String[0]));
  }
  
  public void testRandomRealisticKeyword() throws IOException {
    Map<String,String> map = new HashMap<String,String>();
    int numTerms = atLeast(50);
    for (int i = 0; i < numTerms; i++) {
      String randomRealisticUnicodeString = _TestUtil
          .randomRealisticUnicodeString(random());
      if (randomRealisticUnicodeString.length() > 0) {
        String value = _TestUtil.randomSimpleString(random());
        map.put(randomRealisticUnicodeString,
            value.isEmpty() ? "a" : value);
      }
    }
    if (map.isEmpty()) {
      map.put("booked", "books");
    }
    StemmerOverrideFilter.Builder builder = new StemmerOverrideFilter.Builder(random().nextBoolean());
    Set<Entry<String,String>> entrySet = map.entrySet();
    for (Entry<String,String> entry : entrySet) {
      builder.add(entry.getKey(), entry.getValue());
    }
    StemmerOverrideMap build = builder.build();
    for (Entry<String,String> entry : entrySet) {
      if (random().nextBoolean()) {
        Tokenizer tokenizer = new KeywordTokenizer(new StringReader(
            entry.getKey()));
        TokenStream stream = new PorterStemFilter(new StemmerOverrideFilter(
            tokenizer, build));
        assertTokenStreamContents(stream, new String[] {entry.getValue()});
      }
    }
  }
}
