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
import org.apache.lucene.analysis.util.ClasspathResourceLoader;
import org.apache.lucene.util.Version;

public class TestLengthFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void test() throws Exception {
    Reader reader = new StringReader("foo foobar super-duper-trooper");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Length",
        Version.LUCENE_4_3, new ClasspathResourceLoader(getClass()),
        "min", "4",
        "max", "10",
        "enablePositionIncrements", "false").create(stream);
    assertTokenStreamContents(stream, new String[] { "foobar" }, new int[] { 1 });
  }

  public void testPositionIncrements() throws Exception {
    Reader reader = new StringReader("foo foobar super-duper-trooper");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Length",
        LengthFilterFactory.MIN_KEY, "4",
        LengthFilterFactory.MAX_KEY, "10").create(stream);
    assertTokenStreamContents(stream, new String[] { "foobar" }, new int[] { 2 });
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Length",
          LengthFilterFactory.MIN_KEY, "4",
          LengthFilterFactory.MAX_KEY, "5",
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }

  /** Test that invalid arguments result in exception */
  public void testInvalidArguments() throws Exception {
    try {
      Reader reader = new StringReader("foo foobar super-duper-trooper");
      TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
      tokenFilterFactory("Length",
          LengthFilterFactory.MIN_KEY, "5",
          LengthFilterFactory.MAX_KEY, "4").create(stream);
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("maximum length must not be greater than minimum length"));
    }
  }
}