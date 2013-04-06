package org.apache.lucene.analysis.ja;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

/**
 * Simple tests for {@link JapanesePartOfSpeechStopFilterFactory}
 */
public class TestJapanesePartOfSpeechStopFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {
    String tags = 
        "#  verb-main:\n" +
        "動詞-自立\n";
    
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream ts = tokenizerFactory.create(new StringReader("私は制限スピードを超える。"));
    Map<String,String> args = new HashMap<String,String>();
    args.put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
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
        put("luceneMatchVersion", TEST_VERSION_CURRENT.toString());
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
