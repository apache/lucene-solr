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
package org.apache.lucene.analysis.th;


import java.io.StringReader;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the Thai word filter factory is working.
 */
public class TestThaiTokenizerFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Ensure the filter actually decomposes text.
   */
  public void testWordBreak() throws Exception {
    assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
    Tokenizer tokenizer = tokenizerFactory("Thai").create(newAttributeFactory());
    tokenizer.setReader(new StringReader("การที่ได้ต้องแสดงว่างานดี"));
    assertTokenStreamContents(tokenizer, new String[] {"การ", "ที่", "ได้",
        "ต้อง", "แสดง", "ว่า", "งาน", "ดี"});
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    assumeTrue("JRE does not support Thai dictionary-based BreakIterator", ThaiTokenizer.DBBI_AVAILABLE);
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenizerFactory("Thai", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
