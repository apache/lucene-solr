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


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;
import org.apache.lucene.util.Version;

/**
 * Simple tests to ensure the keyword marker filter factory is working.
 */
public class TestKeywordMarkerFilterFactory extends BaseTokenStreamFactoryTestCase {
  
  public void testKeywords() throws Exception {
    Reader reader = new StringReader("dogs cats");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker", Version.LATEST,
        new StringMockResourceLoader("cats"),
        "protected", "protwords.txt").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "dog", "cats" });
  }
  
  public void testKeywords2() throws Exception {
    Reader reader = new StringReader("dogs cats");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker",
        "pattern", "cats|Dogs").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "dog", "cats" });
  }
  
  public void testKeywordsMixed() throws Exception {
    Reader reader = new StringReader("dogs cats birds");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker", Version.LATEST, new StringMockResourceLoader("cats"),
        "protected", "protwords.txt",
        "pattern", "birds|Dogs").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "dog", "cats", "birds" });
  }
  
  public void testKeywordsCaseInsensitive() throws Exception {
    Reader reader = new StringReader("dogs cats Cats");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker", Version.LATEST, new StringMockResourceLoader("cats"),
        "protected", "protwords.txt",
        "ignoreCase", "true").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "dog", "cats", "Cats" });
  }
  
  public void testKeywordsCaseInsensitive2() throws Exception {
    Reader reader = new StringReader("dogs cats Cats");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker",
        "pattern", "Cats",
        "ignoreCase", "true").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);;
    assertTokenStreamContents(stream, new String[] { "dog", "cats", "Cats" });
  }
  
  public void testKeywordsCaseInsensitiveMixed() throws Exception {
    Reader reader = new StringReader("dogs cats Cats Birds birds");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("KeywordMarker", Version.LATEST,
        new StringMockResourceLoader("cats"),
        "protected", "protwords.txt",
        "pattern", "birds",
        "ignoreCase", "true").create(stream);
    stream = tokenFilterFactory("PorterStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "dog", "cats", "Cats", "Birds", "birds" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("KeywordMarker", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
