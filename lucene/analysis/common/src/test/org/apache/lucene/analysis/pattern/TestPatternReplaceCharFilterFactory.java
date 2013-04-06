package org.apache.lucene.analysis.pattern;

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

/**
 * Simple tests to ensure this factory is working
 */
public class TestPatternReplaceCharFilterFactory extends BaseTokenStreamFactoryTestCase {
  
  //           1111
  // 01234567890123
  // this is test.
  public void testNothingChange() throws Exception {
    Reader reader = new StringReader("this is test.");
    reader = charFilterFactory("PatternReplace",
        "pattern", "(aa)\\s+(bb)\\s+(cc)",
        "replacement", "$1$2$3").create(reader);
    TokenStream ts = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    assertTokenStreamContents(ts,
        new String[] { "this", "is", "test." },
        new int[] { 0, 5, 8 },
        new int[] { 4, 7, 13 });
  }
  
  // 012345678
  // aa bb cc
  public void testReplaceByEmpty() throws Exception {
    Reader reader = new StringReader("aa bb cc");
    reader = charFilterFactory("PatternReplace",
        "pattern", "(aa)\\s+(bb)\\s+(cc)").create(reader);
    TokenStream ts = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    assertTokenStreamContents(ts, new String[] {});
  }
  
  // 012345678
  // aa bb cc
  // aa#bb#cc
  public void test1block1matchSameLength() throws Exception {
    Reader reader = new StringReader("aa bb cc");
    reader = charFilterFactory("PatternReplace",
        "pattern", "(aa)\\s+(bb)\\s+(cc)",
        "replacement", "$1#$2#$3").create(reader);
    TokenStream ts = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    assertTokenStreamContents(ts,
        new String[] { "aa#bb#cc" },
        new int[] { 0 },
        new int[] { 8 });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      charFilterFactory("PatternReplace",
          "pattern", "something",
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
