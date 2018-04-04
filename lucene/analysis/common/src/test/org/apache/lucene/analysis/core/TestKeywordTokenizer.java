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
package org.apache.lucene.analysis.core;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.AttributeFactory;

public class TestKeywordTokenizer extends BaseTokenStreamTestCase {

  public void testSimple() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    KeywordTokenizer tokenizer = new KeywordTokenizer();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[]{"Tokenizer \ud801\udc1ctest"});
  }

  public void testFactory() {
    Map<String, String> args = new HashMap<>();
    KeywordTokenizerFactory factory = new KeywordTokenizerFactory(args);
    AttributeFactory attributeFactory = newAttributeFactory();
    Tokenizer tokenizer = factory.create(attributeFactory);
    assertEquals(KeywordTokenizer.class, tokenizer.getClass());
  }

  private Map<String, String> makeArgs(String... args) {
    Map<String, String> ret = new HashMap<>();
    for (int idx = 0; idx < args.length; idx += 2) {
      ret.put(args[idx], args[idx + 1]);
    }
    return ret;
  }

  public void testParamsFactory() throws IOException {
    // negative maxTokenLen
    IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () ->
        new KeywordTokenizerFactory(makeArgs("maxTokenLen", "-1")));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: -1", iae.getMessage());

    // zero maxTokenLen
    iae = expectThrows(IllegalArgumentException.class, () ->
        new KeywordTokenizerFactory(makeArgs("maxTokenLen", "0")));
    assertEquals("maxTokenLen must be greater than 0 and less than 1048576 passed: 0", iae.getMessage());

    // Added random param, should throw illegal error
    iae = expectThrows(IllegalArgumentException.class, () ->
        new KeywordTokenizerFactory(makeArgs("maxTokenLen", "255", "randomParam", "rValue")));
    assertEquals("Unknown parameters: {randomParam=rValue}", iae.getMessage());

    // tokeniser will never split, no matter what is passed, 
    // but the buffer will not be more than length of the token

    KeywordTokenizerFactory factory = new KeywordTokenizerFactory(makeArgs("maxTokenLen", "5"));
    AttributeFactory attributeFactory = newAttributeFactory();
    Tokenizer tokenizer = factory.create(attributeFactory);
    StringReader reader = new StringReader("Tokenizertest");
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[]{"Tokenizertest"});

    // tokeniser will never split, no matter what is passed, 
    // but the buffer will not be more than length of the token
    factory = new KeywordTokenizerFactory(makeArgs("maxTokenLen", "2"));
    attributeFactory = newAttributeFactory();
    tokenizer = factory.create(attributeFactory);
    reader = new StringReader("Tokenizer\u00A0test");
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[]{"Tokenizer\u00A0test"});
  }
}
