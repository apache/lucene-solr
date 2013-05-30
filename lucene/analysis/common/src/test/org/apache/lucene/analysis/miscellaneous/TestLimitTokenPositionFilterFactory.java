package org.apache.lucene.analysis.miscellaneous;
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

import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

public class TestLimitTokenPositionFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testMaxPosition1() throws Exception {
    Reader reader = new StringReader("A1 B2 C3 D4 E5 F6");
    MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    // LimitTokenPositionFilter doesn't consume the entire stream that it wraps
    tokenizer.setEnableChecks(false);
    TokenStream stream = tokenizer;
    stream = tokenFilterFactory("LimitTokenPosition",
        "maxTokenPosition", "1").create(stream);
    assertTokenStreamContents(stream, new String[] { "A1" });
  }
  
  public void testMissingParam() throws Exception {
    try {
      tokenFilterFactory("LimitTokenPosition");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue("exception doesn't mention param: " + e.getMessage(),
          0 < e.getMessage().indexOf(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY));
    }
  }

  public void testMaxPosition1WithShingles() throws Exception {
    Reader reader = new StringReader("one two three four five");
    MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    // LimitTokenPositionFilter doesn't consume the entire stream that it wraps
    tokenizer.setEnableChecks(false);
    TokenStream stream = tokenizer;
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "2",
        "maxShingleSize", "3",
        "outputUnigrams", "true").create(stream);
    stream = tokenFilterFactory("LimitTokenPosition",
        "maxTokenPosition", "1").create(stream);
    assertTokenStreamContents(stream, new String[] { "one", "one two", "one two three" });
  }
  
  public void testConsumeAllTokens() throws Exception {
    Reader reader = new StringReader("A1 B2 C3 D4 E5 F6");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("LimitTokenPosition",
        "maxTokenPosition", "3",
        "consumeAllTokens", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "A1", "B2", "C3" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("LimitTokenPosition", 
          "maxTokenPosition", "3", 
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
