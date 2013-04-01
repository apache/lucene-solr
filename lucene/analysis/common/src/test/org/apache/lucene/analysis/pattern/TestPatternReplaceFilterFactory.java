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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

/**
 * Simple tests to ensure this factory is working
 */
public class TestPatternReplaceFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testReplaceAll() throws Exception {
    Reader reader = new StringReader("aabfooaabfooabfoob ab caaaaaaaaab");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("PatternReplace",
        "pattern", "a*b",
        "replacement", "-").create(stream);
                   
    assertTokenStreamContents(stream, 
        new String[] { "-foo-foo-foo-", "-", "c-" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("PatternReplace",
          "pattern", "something",
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
