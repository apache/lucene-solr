package org.apache.lucene.analysis.shingle;

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
 * Simple tests to ensure the Shingle filter factory works.
 */
public class TestShingleFilterFactory extends BaseTokenStreamFactoryTestCase {
  /**
   * Test the defaults
   */
  public void testDefaults() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "this is", "is", "is a", "a", "a test", "test" }
    );
  }
  
  /**
   * Test with unigrams disabled
   */
  public void testNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "outputUnigrams", "false").create(stream);
    assertTokenStreamContents(stream,
        new String[] {"this is", "is a", "a test"});
  }
  
  /**
   * Test with a higher max shingle size
   */
  public void testMaxShingleSize() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "maxShingleSize", "3").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "this is", "this is a", "is", 
                       "is a", "is a test", "a", "a test", "test" }
    );
  }

  /**
   * Test with higher min (and max) shingle size
   */
  public void testMinShingleSize() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "4").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "this is a", "this is a test",
                       "is", "is a test", "a", "test" }
    );
  }

  /**
   * Test with higher min (and max) shingle size and with unigrams disabled
   */
  public void testMinShingleSizeNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "4",
        "outputUnigrams", "false").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this is a", "this is a test", "is a test" });
  }

  /**
   * Test with higher same min and max shingle size
   */
  public void testEqualMinAndMaxShingleSize() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "3").create(stream);
    assertTokenStreamContents(stream, 
         new String[] { "this", "this is a", "is", "is a test", "a", "test" });
  }

  /**
   * Test with higher same min and max shingle size and with unigrams disabled
   */
  public void testEqualMinAndMaxShingleSizeNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "3",
        "outputUnigrams", "false").create(stream);
    assertTokenStreamContents(stream,
        new String[] { "this is a", "is a test" });
  }
    
  /**
   * Test with a non-default token separator
   */
  public void testTokenSeparator() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "tokenSeparator", "=BLAH=").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "this=BLAH=is", "is", "is=BLAH=a", 
                       "a", "a=BLAH=test", "test" }
    );
  }

  /**
   * Test with a non-default token separator and with unigrams disabled
   */
  public void testTokenSeparatorNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "tokenSeparator", "=BLAH=",
        "outputUnigrams", "false").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this=BLAH=is", "is=BLAH=a", "a=BLAH=test" });
  }

  /**
   * Test with an empty token separator
   */
  public void testEmptyTokenSeparator() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "tokenSeparator", "").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "thisis", "is", "isa", "a", "atest", "test" });
  }
    
  /**
   * Test with higher min (and max) shingle size 
   * and with a non-default token separator
   */
  public void testMinShingleSizeAndTokenSeparator() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "4",
        "tokenSeparator", "=BLAH=").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this", "this=BLAH=is=BLAH=a", 
                       "this=BLAH=is=BLAH=a=BLAH=test", "is", 
                       "is=BLAH=a=BLAH=test", "a", "test" }
    );
  }

  /**
   * Test with higher min (and max) shingle size 
   * and with a non-default token separator
   * and with unigrams disabled
   */
  public void testMinShingleSizeAndTokenSeparatorNoUnigrams() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "minShingleSize", "3",
        "maxShingleSize", "4",
        "tokenSeparator", "=BLAH=",
        "outputUnigrams", "false").create(stream);
    assertTokenStreamContents(stream, 
        new String[] { "this=BLAH=is=BLAH=a", "this=BLAH=is=BLAH=a=BLAH=test", 
                       "is=BLAH=a=BLAH=test", }
    );
  }

  /**
   * Test with unigrams disabled except when there are no shingles, with
   * a single input token. Using default min/max shingle sizes: 2/2.  No
   * shingles will be created, since there are fewer input tokens than
   * min shingle size.  However, because outputUnigramsIfNoShingles is
   * set to true, even though outputUnigrams is set to false, one
   * unigram should be output.
   */
  public void testOutputUnigramsIfNoShingles() throws Exception {
    Reader reader = new StringReader("test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Shingle",
        "outputUnigrams", "false",
        "outputUnigramsIfNoShingles", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "test" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Shingle", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
