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

package org.apache.solr.spelling;

import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Test for SpellingQueryConverter
 *
 *
 * @since solr 1.3
 */
public class SpellingQueryConverterTest extends LuceneTestCase {

  @Test
  public void test() throws Exception {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    Collection<Token> tokens = converter.convert("field:foo");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not: " + 1, tokens.size() == 1);
  }

  @Test
  public void testSpecialChars()  {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    String original = "field_with_underscore:value_with_underscore";
    Collection<Token> tokens = converter.convert(original);
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "field_with_digits123:value_with_digits123";
    tokens = converter.convert(original);
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "field-with-hyphens:value-with-hyphens";
    tokens = converter.convert(original);
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    // mix 'em up and add some to the value
//    original = "field_with-123s:value_,.|with-hyphens";
//    tokens = converter.convert(original);
//    assertTrue("tokens is null and it shouldn't be", tokens != null);
//    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
//    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));

    original = "foo:bar^5.0";
    tokens = converter.convert(original);
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
    assertTrue("Token offsets do not match", isOffsetCorrect(original, tokens));
  }

  private boolean isOffsetCorrect(String s, Collection<Token> tokens) {
    for (Token token : tokens) {
      int start = token.startOffset();
      int end = token.endOffset();
      if (!s.substring(start, end).equals(token.toString()))  return false;
    }
    return true;
  }

  @Test
  public void testUnicode() {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT));
    
    // chinese text value
    Collection<Token> tokens = converter.convert("text_field:我购买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("text_购field:我购买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("text_field:我购xyz买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
  }

  @Test
  public void testMultipleClauses() {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT));

    // two field:value pairs should give two tokens
    Collection<Token> tokens = converter.convert("买text_field:我购买了道具和服装。 field2:bar");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());

    // a field:value pair and a search term should give two tokens
    tokens = converter.convert("text_field:我购买了道具和服装。 bar");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());
  }
  
  @Test
  public void testRequiredOrProhibitedFlags() {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer(TEST_VERSION_CURRENT));

    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa bbb ccc"));
      assertTrue("Should have 3 tokens",          tokens != null && tokens.size()==3);
      assertTrue("token 1 should be optional",    !hasRequiredFlag(tokens.get(0)) && !hasProhibitedFlag(tokens.get(0)));
      assertTrue("token 2 should be optional",    !hasRequiredFlag(tokens.get(1)) && !hasProhibitedFlag(tokens.get(1)));
      assertTrue("token 3 should be optional",    !hasRequiredFlag(tokens.get(2)) && !hasProhibitedFlag(tokens.get(2)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("+aaa bbb -ccc"));
      assertTrue("Should have 3 tokens",          tokens != null && tokens.size()==3);
      assertTrue("token 1 should be required",     hasRequiredFlag(tokens.get(0)) && !hasProhibitedFlag(tokens.get(0)));
      assertTrue("token 2 should be optional",    !hasRequiredFlag(tokens.get(1)) && !hasProhibitedFlag(tokens.get(1)));
      assertTrue("token 3 should be prohibited",  !hasRequiredFlag(tokens.get(2)) &&  hasProhibitedFlag(tokens.get(2)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa AND bbb ccc"));
      assertTrue("Should have 3 tokens",           tokens != null && tokens.size()==3);
      assertTrue("token 1 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 2 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 3 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa OR bbb OR ccc"));
      assertTrue("Should have 3 tokens",           tokens != null && tokens.size()==3);
      assertTrue("token 1 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 2 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 3 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa AND bbb NOT ccc"));
      assertTrue("Should have 3 tokens",            tokens != null && tokens.size()==3);
      assertTrue("token 1 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 2 precedes n.b.o.",          hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 3 doesn't precede n.b.o.",  !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa NOT bbb AND ccc"));
      assertTrue("Should have 3 tokens",           tokens != null && tokens.size()==3);
      assertTrue("token 1 precedes n.b.o.",          hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 2 precedes n.b.o.",          hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 3 doesn't precedes n.b.o.", !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    {
      List<Token> tokens = new ArrayList<Token>(converter.convert("aaa AND NOT bbb AND ccc"));
      assertTrue("Should have 3 tokens",           tokens != null && tokens.size()==3);
      assertTrue("token 1 precedes n.b.o.",          hasNBOFlag(tokens.get(0)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 2 precedes n.b.o.",          hasNBOFlag(tokens.get(1)) && hasInBooleanFlag(tokens.get(0)));
      assertTrue("token 3 doesn't precedes n.b.o.", !hasNBOFlag(tokens.get(2)) && hasInBooleanFlag(tokens.get(0)));
    }
    
  }
  
  private boolean hasRequiredFlag(Token t) {
    return (t.getFlags() & QueryConverter.REQUIRED_TERM_FLAG) == QueryConverter.REQUIRED_TERM_FLAG;
  }
  private boolean hasProhibitedFlag(Token t) {
    return (t.getFlags() & QueryConverter.PROHIBITED_TERM_FLAG) == QueryConverter.PROHIBITED_TERM_FLAG;
  }
  private boolean hasNBOFlag(Token t) {
    return (t.getFlags() & QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG) == QueryConverter.TERM_PRECEDES_NEW_BOOLEAN_OPERATOR_FLAG;
  }
  private boolean hasInBooleanFlag(Token t) {
    return (t.getFlags() & QueryConverter.TERM_IN_BOOLEAN_QUERY_FLAG) == QueryConverter.TERM_IN_BOOLEAN_QUERY_FLAG;
  }
}
