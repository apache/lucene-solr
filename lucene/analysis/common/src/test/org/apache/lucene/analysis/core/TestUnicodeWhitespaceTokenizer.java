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

public class TestUnicodeWhitespaceTokenizer extends BaseTokenStreamTestCase {
  
  // clone of test from WhitespaceTokenizer
  public void testSimple() throws IOException {
    StringReader reader = new StringReader("Tokenizer \ud801\udc1ctest");
    UnicodeWhitespaceTokenizer tokenizer = new UnicodeWhitespaceTokenizer();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "\ud801\udc1ctest" });
  }
  
  public void testNBSP() throws IOException {
    StringReader reader = new StringReader("Tokenizer\u00A0test");
    UnicodeWhitespaceTokenizer tokenizer = new UnicodeWhitespaceTokenizer();
    tokenizer.setReader(reader);
    assertTokenStreamContents(tokenizer, new String[] { "Tokenizer",
        "test" });
  }

  public void testFactory() {
    Map<String, String> args = new HashMap<>();
    args.put("rule", "unicode");
    WhitespaceTokenizerFactory factory = new WhitespaceTokenizerFactory(args);
    AttributeFactory attributeFactory = newAttributeFactory();
    Tokenizer tokenizer = factory.create(attributeFactory);
    assertEquals(UnicodeWhitespaceTokenizer.class, tokenizer.getClass());
  }

}
