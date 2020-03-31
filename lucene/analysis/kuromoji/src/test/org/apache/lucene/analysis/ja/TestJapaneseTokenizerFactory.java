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
package org.apache.lucene.analysis.ja;


import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests for {@link JapaneseTokenizerFactory}
 */
public class TestJapaneseTokenizerFactory extends BaseTokenStreamTestCase {
  public void testSimple() throws IOException {
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    factory.inform(new StringMockResourceLoader(""));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("これは本ではない"));
    assertTokenStreamContents(ts,
        new String[] { "これ", "は", "本", "で", "は", "ない" },
        new int[] { 0, 2, 3, 4, 5, 6 },
        new int[] { 2, 3, 4, 5, 6, 8 }
    );
  }

  /**
   * Test that search mode is enabled and working by default
   */
  public void testDefaults() throws IOException {
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    factory.inform(new StringMockResourceLoader(""));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("シニアソフトウェアエンジニア"));
    assertTokenStreamContents(ts,
                              new String[] { "シニア", "ソフトウェア", "エンジニア" }
    );
  }

  /**
   * Test mode parameter: specifying normal mode
   */
  public void testMode() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("mode", "normal");
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("シニアソフトウェアエンジニア"));
    assertTokenStreamContents(ts,
        new String[] { "シニアソフトウェアエンジニア" }
    );
  }

  /**
   * Test user dictionary
   */
  public void testUserDict() throws IOException {
    String userDict =
        "# Custom segmentation for long entries\n" +
        "日本経済新聞,日本 経済 新聞,ニホン ケイザイ シンブン,カスタム名詞\n" +
        "関西国際空港,関西 国際 空港,カンサイ コクサイ クウコウ,テスト名詞\n" +
        "# Custom reading for sumo wrestler\n" +
        "朝青龍,朝青龍,アサショウリュウ,カスタム人名\n";
    Map<String,String> args = new HashMap<>();
    args.put("userDictionary", "userdict.txt");
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(userDict));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("関西国際空港に行った"));
    assertTokenStreamContents(ts,
        new String[] { "関西", "国際", "空港", "に",  "行っ",  "た" }
    );
  }

  /**
   * Test preserving punctuation
   */
  public void testPreservePunctuation() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("discardPunctuation", "false");
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("今ノルウェーにいますが、来週の頭日本に戻ります。楽しみにしています！お寿司が食べたいな。。。"));
    assertTokenStreamContents(ts,
        new String[] { "今", "ノルウェー", "に", "い", "ます", "が", "、",
            "来週", "の", "頭", "日本", "に", "戻り", "ます", "。",
            "楽しみ", "に", "し", "て", "い", "ます", "！",
            "お", "寿司", "が", "食べ", "たい", "な", "。", "。", "。"}
    );
  }

  /**
   * Test discarding compound (original) token
   */
  public void testPreserveCompoundToken() throws IOException {
    Map<String,String> args = new HashMap<>();
    args.put("discardCompoundToken", "false");
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    TokenStream ts = factory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("シニアソフトウェアエンジニア"));
    assertTokenStreamContents(ts,
        new String[] { "シニア", "シニアソフトウェアエンジニア", "ソフトウェア", "エンジニア" }
    );
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new JapaneseTokenizerFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }

  private TokenStream makeTokenStream(HashMap<String, String> args, String in) throws IOException {
    JapaneseTokenizerFactory factory = new JapaneseTokenizerFactory(args);
    factory.inform(new StringMockResourceLoader(""));
    Tokenizer t = factory.create(newAttributeFactory());
    t.setReader(new StringReader(in));
    return t;
  }

  /**
   * Test nbestCost parameter
   */
  public void testNbestCost() throws IOException {
    assertTokenStreamContents(makeTokenStream(new HashMap<String, String>() {{put("nBestCost", "2000");}},
                                              "鳩山積み"),
                              new String[] {"鳩", "鳩山", "山積み", "積み"});
  }

  /**
   * Test nbestExamples parameter
   */
  public void testNbestExample() throws IOException {
    assertTokenStreamContents(makeTokenStream(new HashMap<String, String>()
                                              {{put("nBestExamples", "/鳩山積み-鳩山/鳩山積み-鳩/");}},
                                              "鳩山積み"),
                              new String[] {"鳩", "鳩山", "山積み", "積み"});
  }
}
