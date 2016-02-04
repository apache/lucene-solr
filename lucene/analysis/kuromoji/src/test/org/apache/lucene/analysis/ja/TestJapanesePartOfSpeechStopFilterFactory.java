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


import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

/**
 * Simple tests for {@link JapanesePartOfSpeechStopFilterFactory}
 */
public class TestJapanesePartOfSpeechStopFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws Exception {
    String tags = 
        "#  verb-main:\n" +
        "動詞-自立\n";
    
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream ts = tokenizerFactory.create();
    ((Tokenizer)ts).setReader(new StringReader("私は制限スピードを超える。"));
    Map<String,String> args = new HashMap<>();
    args.put("luceneMatchVersion", Version.LATEST.toString());
    args.put("tags", "stoptags.txt");
    JapanesePartOfSpeechStopFilterFactory factory = new JapanesePartOfSpeechStopFilterFactory(args);
    factory.inform(new StringMockResourceLoader(tags));
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "私", "は", "制限", "スピード", "を" }
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new JapanesePartOfSpeechStopFilterFactory(new HashMap<String,String>() {{
        put("luceneMatchVersion", Version.LATEST.toString());
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }

  public void test43Backcompat() throws Exception {
    String tags = "#  particle-case-misc: Case particles.\n"
                + "#  e.g. から, が, で, と, に, へ, より, を, の, にて\n"
                + "助詞-格助詞-一般";

    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    Tokenizer tokenizer = tokenizerFactory.create();
    tokenizer.setReader(new StringReader("私は制限スピードを超える。"));
    Map<String,String> args = new HashMap<>();
    args.put("luceneMatchVersion", Version.LUCENE_4_3_1.toString());
    args.put("enablePositionIncrements", "true");
    args.put("tags", "stoptags.txt");
    JapanesePartOfSpeechStopFilterFactory factory = new JapanesePartOfSpeechStopFilterFactory(args);
    factory.inform(new StringMockResourceLoader(tags));
    TokenStream stream = factory.create(tokenizer);
    assertTrue(stream instanceof Lucene43JapanesePartOfSpeechStopFilter);
    assertTokenStreamContents(stream, new String[] { "私", "は", "制限", "スピード", "超える" }, 
        new int[] {1, 1, 1, 1, 2});

    tokenizer = tokenizerFactory.create();
    tokenizer.setReader(new StringReader("私は制限スピードを超える。"));
    args = new HashMap<>();
    args.put("luceneMatchVersion", Version.LUCENE_4_3_1.toString());
    args.put("enablePositionIncrements", "false");
    args.put("tags", "stoptags.txt");
    factory = new JapanesePartOfSpeechStopFilterFactory(args);
    factory.inform(new StringMockResourceLoader(tags));
    stream = factory.create(tokenizer);
    assertTrue(stream instanceof Lucene43JapanesePartOfSpeechStopFilter);
    assertTokenStreamContents(stream, new String[]{"私", "は", "制限", "スピード", "超える"},
        new int[] {1, 1, 1, 1, 1});
    
    try {
      args = new HashMap<>();
      args.put("luceneMatchVersion", Version.LUCENE_4_4_0.toString());
      args.put("enablePositionIncrements", "false");
      args.put("tags", "stoptags.txt");
      factory = new JapanesePartOfSpeechStopFilterFactory(args);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("enablePositionIncrements=false is not supported"));
    }
    args = new HashMap<>();
    args.put("luceneMatchVersion", Version.LUCENE_4_4_0.toString());
    args.put("enablePositionIncrements", "true");
    args.put("tags", "stoptags.txt");
    factory = new JapanesePartOfSpeechStopFilterFactory(args);

    try {
      args = new HashMap<>();
      args.put("luceneMatchVersion", Version.LATEST.toString());
      args.put("enablePositionIncrements", "false");
      args.put("tags", "stoptags.txt");
      factory = new JapanesePartOfSpeechStopFilterFactory(args);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("not a valid option"));
    }
  }

}
