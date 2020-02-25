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
package org.apache.lucene.analysis.ko;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests for {@link KoreanTokenizerFactory}
 */
public class TestKoreanTokenizerFactory extends BaseTokenStreamTestCase {
  public void testSimple() throws IOException {
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(Collections.emptyMap());
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("안녕하세요"));
    assertTokenStreamContents(ts,
        new String[] { "안녕", "하", "시", "어요" },
        new int[] { 0, 2, 3, 3 },
        new int[] { 2, 3, 5, 5 }
    );
  }

  /**
   * Test decompoundMode
   */
  public void testDiscardDecompound() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("decompoundMode", "discard");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("갠지스강"));
    assertTokenStreamContents(ts,
        new String[] { "갠지스", "강" }
    );
  }

  public void testNoDecompound() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("decompoundMode", "none");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("갠지스강"));
    assertTokenStreamContents(ts,
        new String[] { "갠지스강" }
    );
  }

  public void testMixedDecompound() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("decompoundMode", "mixed");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("갠지스강"));
    assertTokenStreamContents(ts,
        new String[] { "갠지스강", "갠지스", "강" }
    );
  }

  /**
   * Test user dictionary
   */
  public void testUserDict() throws IOException {
    String userDict =
        "# Additional nouns\n" +
        "세종시 세종 시\n" +
        "# \n" +
        "c++\n";
    Map<String,String> args = new HashMap<>();
    args.put("userDictionary", "userdict.txt");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(userDict));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("세종시"));
    assertTokenStreamContents(ts,
        new String[] { "세종", "시" }
    );
  }

  /**
   * Test discardPunctuation True
   */
  public void testDiscardPunctuation_true() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("discardPunctuation", "true");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("10.1 인치 모니터"));
    assertTokenStreamContents(ts,
        new String[] { "10", "1", "인치", "모니터" }
    );
  }

  /**
   * Test discardPunctuation False
   */
  public void testDiscardPunctuation_false() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("discardPunctuation", "false");
    KoreanTokenizerFactory factory = new KoreanTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer ts = factory.create(newAttributeFactory());
    ts.setReader(new StringReader("10.1 인치 모니터"));
    assertTokenStreamContents(ts,
        new String[] { "10", ".", "1", " ", "인치", " ", "모니터" }
    );
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () ->
        new KoreanTokenizerFactory(new HashMap<String, String>() {{
          put("bogusArg", "bogusValue");
        }})
    );
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
