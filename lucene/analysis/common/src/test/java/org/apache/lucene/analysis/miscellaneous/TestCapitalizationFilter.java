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
package org.apache.lucene.analysis.miscellaneous;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.junit.Test;

import static org.apache.lucene.analysis.miscellaneous.CapitalizationFilter.*;

/** Tests {@link CapitalizationFilter} */
public class TestCapitalizationFilter extends BaseTokenStreamTestCase {  
  public void testCapitalization() throws Exception {
    CharArraySet keep = new CharArraySet(
        Arrays.asList("and", "the", "it", "BIG"), false);
    
    assertCapitalizesTo("kiTTEN", new String[] { "Kitten" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    assertCapitalizesTo("and", new String[] { "And" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    assertCapitalizesTo("AnD", new String[] { "And" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);

    //first is not forced, but it's not a keep word, either
    assertCapitalizesTo("AnD", new String[] { "And" }, 
        true, keep, false, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);

    assertCapitalizesTo("big", new String[] { "Big" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);

    assertCapitalizesTo("BIG", new String[] { "BIG" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    assertCapitalizesToKeyword("Hello thEre my Name is Ryan", "Hello there my name is ryan", 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    // now each token
    assertCapitalizesTo("Hello thEre my Name is Ryan", 
        new String[] { "Hello", "There", "My", "Name", "Is", "Ryan" }, 
        false, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
           
    // now only the long words
    assertCapitalizesTo("Hello thEre my Name is Ryan", 
        new String[] { "Hello", "There", "my", "Name", "is", "Ryan" }, 
        false, keep, true, null, 3, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    // without prefix
    assertCapitalizesTo("McKinley", 
        new String[] { "Mckinley" }, 
        true, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    // Now try some prefixes
    List<char[]> okPrefix = new ArrayList<>();
    okPrefix.add("McK".toCharArray());
    
    assertCapitalizesTo("McKinley", 
        new String[] { "McKinley" }, 
        true, keep, true, okPrefix, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
    
    // now try some stuff with numbers
    assertCapitalizesTo("1st 2nd third", 
        new String[] { "1st", "2nd", "Third" }, 
        false, keep, false, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);    
    
    assertCapitalizesToKeyword("the The the", "The The the", 
        false, keep, true, null, 0, DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);    
  }
  
  static void assertCapitalizesTo(Tokenizer tokenizer, String expected[],
      boolean onlyFirstWord, CharArraySet keep, boolean forceFirstLetter,
      Collection<char[]> okPrefix, int minWordLength, int maxWordCount,
      int maxTokenLength) throws IOException {
    CapitalizationFilter filter = new CapitalizationFilter(tokenizer, onlyFirstWord, keep, 
        forceFirstLetter, okPrefix, minWordLength, maxWordCount, maxTokenLength);
    assertTokenStreamContents(filter, expected);    
  }
  
  static void assertCapitalizesTo(String input, String expected[],
      boolean onlyFirstWord, CharArraySet keep, boolean forceFirstLetter,
      Collection<char[]> okPrefix, int minWordLength, int maxWordCount,
      int maxTokenLength) throws IOException {
    final MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    tokenizer.setReader(new StringReader(input));
    assertCapitalizesTo(tokenizer,
        expected, onlyFirstWord, keep, forceFirstLetter, okPrefix, minWordLength,
        maxWordCount, maxTokenLength);
  }
  
  static void assertCapitalizesToKeyword(String input, String expected,
      boolean onlyFirstWord, CharArraySet keep, boolean forceFirstLetter,
      Collection<char[]> okPrefix, int minWordLength, int maxWordCount,
      int maxTokenLength) throws IOException {
    final MockTokenizer tokenizer = new MockTokenizer(MockTokenizer.KEYWORD, false);
    tokenizer.setReader(new StringReader(input));
    assertCapitalizesTo(tokenizer,
        new String[] { expected }, onlyFirstWord, keep, forceFirstLetter, okPrefix,
        minWordLength, maxWordCount, maxTokenLength);    
  }
  
  /** blast some random strings through the analyzer */
  public void testRandomString() throws Exception {
    Analyzer a = new Analyzer() {

      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new MockTokenizer(MockTokenizer.WHITESPACE, false);
        return new TokenStreamComponents(tokenizer, new CapitalizationFilter(tokenizer));
      }
    };
    
    checkRandomData(random(), a, 1000*RANDOM_MULTIPLIER);
    a.close();
  }
  
  public void testEmptyTerm() throws IOException {
    Analyzer a = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new CapitalizationFilter(tokenizer));
      }
    };
    checkOneTerm(a, "", "");
    a.close();
  }

  /**
   * checking the validity of constructor arguments
   */
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments() throws Exception {
    new CapitalizationFilter(whitespaceMockTokenizer("accept only valid arguments"),true, null, true, null, -1 , DEFAULT_MAX_WORD_COUNT, DEFAULT_MAX_TOKEN_LENGTH);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments1() throws Exception {
    new CapitalizationFilter(whitespaceMockTokenizer("accept only valid arguments"),true, null, true, null, 0 , -10, DEFAULT_MAX_TOKEN_LENGTH);
  }
  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArguments2() throws Exception {
    new CapitalizationFilter(whitespaceMockTokenizer("accept only valid arguments"),true, null, true, null, 0 , DEFAULT_MAX_WORD_COUNT, -50);
  }
}
