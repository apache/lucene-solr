package org.apache.lucene.analysis.miscellaneous;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

public class TestLimitTokenCountFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void test() throws Exception {
    Reader reader = new StringReader("A1 B2 C3 D4 E5 F6");
    MockTokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    // LimitTokenCountFilter doesn't consume the entire stream that it wraps
    tokenizer.setEnableChecks(false);
    TokenStream stream = tokenizer;
    stream = tokenFilterFactory("LimitTokenCount",
        "maxTokenCount", "3").create(stream);
    assertTokenStreamContents(stream, new String[] { "A1", "B2", "C3" });
  }

  public void testRequired() throws Exception {
    // param is required
    try {
      tokenFilterFactory("LimitTokenCount");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue("exception doesn't mention param: " + e.getMessage(),
                 0 < e.getMessage().indexOf(LimitTokenCountFilterFactory.MAX_TOKEN_COUNT_KEY));
    }
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("LimitTokenCount", 
          "maxTokenCount", "3", 
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
