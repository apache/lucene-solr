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
 * Simple tests for {@link org.apache.lucene.analysis.ja.JapaneseNumberFilterFactory}
 */
public class TestJapaneseNumberFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws IOException {

    Map<String, String> args = new HashMap<>();
    args.put("discardPunctuation", "false");

    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(args);

    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream tokenStream = tokenizerFactory.create(newAttributeFactory());
    ((Tokenizer)tokenStream).setReader(new StringReader("昨日のお寿司は1０万円でした。"));

    JapaneseNumberFilterFactory factory = new JapaneseNumberFilterFactory(new HashMap<>());
    tokenStream = factory.create(tokenStream);
    assertTokenStreamContents(tokenStream,
        new String[] { "昨日", "の", "お", "寿司", "は", "100000", "円", "でし", "た", "。" }
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      new JapaneseNumberFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
