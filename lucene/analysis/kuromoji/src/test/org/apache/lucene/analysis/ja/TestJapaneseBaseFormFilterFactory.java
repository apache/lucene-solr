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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Simple tests for {@link JapaneseBaseFormFilterFactory}
 */
public class TestJapaneseBaseFormFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream ts = tokenizerFactory.create(newAttributeFactory());
    ((Tokenizer)ts).setReader(new StringReader("それはまだ実験段階にあります"));
    JapaneseBaseFormFilterFactory factory = new JapaneseBaseFormFilterFactory(new HashMap<String,String>());
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "それ", "は", "まだ", "実験", "段階", "に", "ある", "ます"  }
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new JapaneseBaseFormFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
