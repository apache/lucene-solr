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

public class TestCapitalizationFilterFactory extends BaseTokenStreamFactoryTestCase {
  
  public void testCapitalization() throws Exception {
    Reader reader = new StringReader("kiTTEN");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Kitten" });
  }
  
  public void testCapitalization2() throws Exception {
    Reader reader = new StringReader("and");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "And" });
  }
  
  /** first is forced, but it's not a keep word, either */
  public void testCapitalization3() throws Exception {
    Reader reader = new StringReader("AnD");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "And" });
  }
  
  public void testCapitalization4() throws Exception {
    Reader reader = new StringReader("AnD");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "false").create(stream);
    assertTokenStreamContents(stream, new String[] { "And" });
  }
  
  public void testCapitalization5() throws Exception {
    Reader reader = new StringReader("big");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Big" });
  }
  
  public void testCapitalization6() throws Exception {
    Reader reader = new StringReader("BIG");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "BIG" });
  }
  
  public void testCapitalization7() throws Exception {
    Reader reader = new StringReader("Hello thEre my Name is Ryan");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Hello there my name is ryan" });
  }
  
  public void testCapitalization8() throws Exception {
    Reader reader = new StringReader("Hello thEre my Name is Ryan");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Hello", "There", "My", "Name", "Is", "Ryan" });
  }
  
  public void testCapitalization9() throws Exception {
    Reader reader = new StringReader("Hello thEre my Name is Ryan");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "minWordLength", "3",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Hello", "There", "my", "Name", "is", "Ryan" });
  }
  
  public void testCapitalization10() throws Exception {
    Reader reader = new StringReader("McKinley");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "minWordLength", "3",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Mckinley" });
  }
  
  /** using "McK" as okPrefix */
  public void testCapitalization11() throws Exception {
    Reader reader = new StringReader("McKinley");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "minWordLength", "3",
        "okPrefix", "McK",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "McKinley" });
  }
  
  /** test with numbers */
  public void testCapitalization12() throws Exception {
    Reader reader = new StringReader("1st 2nd third");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "minWordLength", "3",
        "okPrefix", "McK",
        "forceFirstLetter", "false").create(stream);
    assertTokenStreamContents(stream, new String[] { "1st", "2nd", "Third" });
  }
  
  public void testCapitalization13() throws Exception {
    Reader reader = new StringReader("the The the");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "and the it BIG",
        "onlyFirstWord", "false",
        "minWordLength", "3",
        "okPrefix", "McK",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "The The the" });
  }

  public void testKeepIgnoreCase() throws Exception {
    Reader reader = new StringReader("kiTTEN");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "kitten",
        "keepIgnoreCase", "true",
        "onlyFirstWord", "true",
        "forceFirstLetter", "true").create(stream);

    assertTokenStreamContents(stream, new String[] { "KiTTEN" });
  }
  
  public void testKeepIgnoreCase2() throws Exception {
    Reader reader = new StringReader("kiTTEN");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "kitten",
        "keepIgnoreCase", "true",
        "onlyFirstWord", "true",
        "forceFirstLetter", "false").create(stream);

    assertTokenStreamContents(stream, new String[] { "kiTTEN" });
  }
  
  public void testKeepIgnoreCase3() throws Exception {
    Reader reader = new StringReader("kiTTEN");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "keepIgnoreCase", "true",
        "onlyFirstWord", "true",
        "forceFirstLetter", "false").create(stream);

    assertTokenStreamContents(stream, new String[] { "Kitten" });
  }
  
  /**
   * Test CapitalizationFilterFactory's minWordLength option.
   * 
   * This is very weird when combined with ONLY_FIRST_WORD!!!
   */
  public void testMinWordLength() throws Exception {
    Reader reader = new StringReader("helo testing");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "onlyFirstWord", "true",
        "minWordLength", "5").create(stream);
    assertTokenStreamContents(stream, new String[] { "helo", "Testing" });
  }
  
  /**
   * Test CapitalizationFilterFactory's maxWordCount option with only words of 1
   * in each token (it should do nothing)
   */
  public void testMaxWordCount() throws Exception {
    Reader reader = new StringReader("one two three four");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "maxWordCount", "2").create(stream);
    assertTokenStreamContents(stream, new String[] { "One", "Two", "Three", "Four" });
  }
  
  /**
   * Test CapitalizationFilterFactory's maxWordCount option when exceeded
   */
  public void testMaxWordCount2() throws Exception {
    Reader reader = new StringReader("one two three four");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.KEYWORD, false);
    stream = tokenFilterFactory("Capitalization",
        "maxWordCount", "2").create(stream);
    assertTokenStreamContents(stream, new String[] { "one two three four" });
  }
  
  /**
   * Test CapitalizationFilterFactory's maxTokenLength option when exceeded
   * 
   * This is weird, it is not really a max, but inclusive (look at 'is')
   */
  public void testMaxTokenLength() throws Exception {
    Reader reader = new StringReader("this is a test");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "maxTokenLength", "2").create(stream);
    assertTokenStreamContents(stream, new String[] { "this", "is", "A", "test" });
  }
  
  /**
   * Test CapitalizationFilterFactory's forceFirstLetter option
   */
  public void testForceFirstLetterWithKeep() throws Exception {
    Reader reader = new StringReader("kitten");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Capitalization",
        "keep", "kitten",
        "forceFirstLetter", "true").create(stream);
    assertTokenStreamContents(stream, new String[] { "Kitten" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Capitalization", "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
