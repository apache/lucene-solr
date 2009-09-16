/**
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
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.solr.common.util.NamedList;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.Assert;

import java.util.Collection;


/**
 * Test for SpellingQueryConverter
 *
 * @version $Id$
 * @since solr 1.3
 */
public class SpellingQueryConverterTest {

  @Test
  public void test() throws Exception {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    Collection<Token> tokens = converter.convert("field:foo");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    assertTrue("tokens Size: " + tokens.size() + " is not: " + 1, tokens.size() == 1);
  }

  @Test
  public void testSpecialChars()  {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    Collection<Token> tokens = converter.convert("field_with_underscore:value_with_underscore");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("field_with_digits123:value_with_digits123");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("field-with-hyphens:value-with-hyphens");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    // mix 'em up and add some to the value
    tokens = converter.convert("field_with-123s:value_,.|with-hyphens");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
  }

  @Test
  public void testUnicode() {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer());
    
    // chinese text value
    Collection<Token> tokens = converter.convert("text_field:我购买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("text_购field:我购买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());

    tokens = converter.convert("text_field:我购xyz买了道具和服装。");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 1", 1, tokens.size());
  }

  @Test
  public void testMultipleClauses() {
    SpellingQueryConverter converter = new SpellingQueryConverter();
    converter.init(new NamedList());
    converter.setAnalyzer(new WhitespaceAnalyzer());

    // two field:value pairs should give two tokens
    Collection<Token> tokens = converter.convert("买text_field:我购买了道具和服装。 field2:bar");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());

    // a field:value pair and a search term should give two tokens
    tokens = converter.convert("text_field:我购买了道具和服装。 bar");
    assertTrue("tokens is null and it shouldn't be", tokens != null);
    Assert.assertEquals("tokens Size: " + tokens.size() + " is not 2", 2, tokens.size());
  }
}
