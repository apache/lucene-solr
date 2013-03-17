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

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;

public class TestLimitTokenPositionFilterFactory extends BaseTokenStreamTestCase {

  public void testMaxPosition1() throws IOException {
    LimitTokenPositionFilterFactory factory = new LimitTokenPositionFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY, "1");
    factory.init(args);
    String test = "A1 B2 C3 D4 E5 F6";
    MockTokenizer tok = new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false);
    // LimitTokenPositionFilter doesn't consume the entire stream that it wraps
    tok.setEnableChecks(false);
    TokenStream stream = factory.create(tok);
    assertTokenStreamContents(stream, new String[] { "A1" });
  }
  
  public void testMissingParam() {
    LimitTokenPositionFilterFactory factory = new LimitTokenPositionFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    IllegalArgumentException iae = null;
    try {
      factory.init(args);
    } catch (IllegalArgumentException e) {
      assertTrue("exception doesn't mention param: " + e.getMessage(),
          0 < e.getMessage().indexOf(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY));
      iae = e;
    }
    assertNotNull("no exception thrown", iae);
  }

  public void testMaxPosition1WithShingles() throws IOException {
    LimitTokenPositionFilterFactory factory = new LimitTokenPositionFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY, "1");
    factory.init(args);
    String input = "one two three four five";
    MockTokenizer tok = new MockTokenizer(new StringReader(input), MockTokenizer.WHITESPACE, false);
    // LimitTokenPositionFilter doesn't consume the entire stream that it wraps
    tok.setEnableChecks(false);
    ShingleFilter shingleFilter = new ShingleFilter(tok, 2, 3);
    shingleFilter.setOutputUnigrams(true);
    TokenStream stream = factory.create(shingleFilter);
    assertTokenStreamContents(stream, new String[] { "one", "one two", "one two three" });
  }
  
  public void testConsumeAllTokens() throws IOException {
    LimitTokenPositionFilterFactory factory = new LimitTokenPositionFilterFactory();
    Map<String, String> args = new HashMap<String, String>();
    args.put(LimitTokenPositionFilterFactory.MAX_TOKEN_POSITION_KEY, "3");
    args.put(LimitTokenPositionFilterFactory.CONSUME_ALL_TOKENS_KEY, "true");
    factory.init(args);
    String test = "A1 B2 C3 D4 E5 F6";
    MockTokenizer tok = new MockTokenizer(new StringReader(test), MockTokenizer.WHITESPACE, false);
    TokenStream stream = factory.create(tok);
    assertTokenStreamContents(stream, new String[] { "A1", "B2", "C3" });
  }
}
