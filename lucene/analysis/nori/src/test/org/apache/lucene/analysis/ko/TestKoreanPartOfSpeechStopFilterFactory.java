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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.Version;

/**
 * Simple tests for {@link KoreanPartOfSpeechStopFilterFactory}
 */
public class TestKoreanPartOfSpeechStopFilterFactory extends BaseTokenStreamTestCase {
  public void testStopTags() throws IOException {
    KoreanTokenizerFactory tokenizerFactory = new KoreanTokenizerFactory(new HashMap<>());
    tokenizerFactory.inform(new StringMockResourceLoader(""));
    TokenStream ts = tokenizerFactory.create();
    ((Tokenizer)ts).setReader(new StringReader(" 한국은 대단한 나라입니다."));
    Map<String,String> args = new HashMap<>();
    args.put("luceneMatchVersion", Version.LATEST.toString());
    args.put("tags", "E, J");
    KoreanPartOfSpeechStopFilterFactory factory = new KoreanPartOfSpeechStopFilterFactory(args);
    ts = factory.create(ts);
    assertTokenStreamContents(ts,
        new String[] { "한국", "대단", "하", "나라", "이" }
    );
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () ->
        new KoreanPartOfSpeechStopFilterFactory(new HashMap<String, String>() {{
          put("luceneMatchVersion", Version.LATEST.toString());
          put("bogusArg", "bogusValue");
        }})
    );
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
