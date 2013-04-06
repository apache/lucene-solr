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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

/**
 * Simple tests for {@link JapaneseKatakanaStemFilterFactory}
 */
public class TestJapaneseKatakanaStemFilterFactory extends BaseTokenStreamTestCase {
  public void testKatakanaStemming() throws IOException {
    JapaneseTokenizerFactory tokenizerFactory = new JapaneseTokenizerFactory(new HashMap<String,String>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream tokenStream = tokenizerFactory.create(
        new StringReader("明後日パーティーに行く予定がある。図書館で資料をコピーしました。")
    );
    JapaneseKatakanaStemFilterFactory filterFactory = new JapaneseKatakanaStemFilterFactory(new HashMap<String,String>());;
    assertTokenStreamContents(filterFactory.create(tokenStream),
        new String[]{ "明後日", "パーティ", "に", "行く", "予定", "が", "ある",   // パーティー should be stemmed
                      "図書館", "で", "資料", "を", "コピー", "し", "まし", "た"} // コピー should not be stemmed
    );
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new JapaneseKatakanaStemFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
