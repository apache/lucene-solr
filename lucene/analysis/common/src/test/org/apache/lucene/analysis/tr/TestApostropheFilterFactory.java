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
package org.apache.lucene.analysis.tr;



import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

/**
 * Simple tests to ensure the apostrophe filter factory is working.
 */
public class TestApostropheFilterFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Ensure the filter actually removes characters after an apostrophe.
   */
  public void testApostrophes() throws Exception {
    Reader reader = new StringReader("Türkiye'de 2003'te Van Gölü'nü gördüm");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer) stream).setReader(reader);
    stream = tokenFilterFactory("Apostrophe").create(stream);
    assertTokenStreamContents(stream, new String[]{"Türkiye", "2003", "Van", "Gölü", "gördüm"});
  }

  /**
   * Test that bogus arguments result in exception
   */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Apostrophe", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameter(s):"));
    }
  }
}
