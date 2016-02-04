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
package org.apache.lucene.analysis.core;


import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.util.Version;

import java.io.Reader;
import java.io.StringReader;
import java.util.Set;

/**
 * Testcase for {@link TypeTokenFilterFactory}
 */
public class TestTypeTokenFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testInform() throws Exception {
    TypeTokenFilterFactory factory = (TypeTokenFilterFactory) tokenFilterFactory("Type",
        "types", "stoptypes-1.txt");
    Set<String> types = factory.getStopTypes();
    assertTrue("types is null and it shouldn't be", types != null);
    assertTrue("types Size: " + types.size() + " is not: " + 2, types.size() == 2);

    factory = (TypeTokenFilterFactory) tokenFilterFactory("Type",
        "types", "stoptypes-1.txt, stoptypes-2.txt",
        "useWhitelist", "true");
    types = factory.getStopTypes();
    assertTrue("types is null and it shouldn't be", types != null);
    assertTrue("types Size: " + types.size() + " is not: " + 4, types.size() == 4);
  }

  public void testCreationWithBlackList() throws Exception {
    TokenFilterFactory factory = tokenFilterFactory("Type",
        "types", "stoptypes-1.txt, stoptypes-2.txt");
    NumericTokenStream input = new NumericTokenStream();
    input.setIntValue(123);
    factory.create(input);
  }
  
  public void testCreationWithWhiteList() throws Exception {
    TokenFilterFactory factory = tokenFilterFactory("Type",
        "types", "stoptypes-1.txt, stoptypes-2.txt",
        "useWhitelist", "true");
    NumericTokenStream input = new NumericTokenStream();
    input.setIntValue(123);
    factory.create(input);
  }

  public void testMissingTypesParameter() throws Exception {
    try {
      tokenFilterFactory("Type");
      fail("not supplying 'types' parameter should cause an IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // everything ok
    }
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("Type", 
          "types", "stoptypes-1.txt", 
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }

  public void test43Backcompat() throws Exception {
    Reader reader = new StringReader("foo bar");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("Type", Version.LUCENE_4_3_1,
        "enablePositionIncrements", "false",
        "types", "stoptypes-1.txt").create(stream);
    assertTrue(stream instanceof Lucene43TypeTokenFilter);
    assertTokenStreamContents(stream, new String[] {"foo", "bar"}, new int[] {0, 4}, new int[] {3, 7}, new int[] {1, 1});

    try {
      tokenFilterFactory("Type", Version.LUCENE_4_4_0, "enablePositionIncrements", "false", "types", "stoptypes-1.txt");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("enablePositionIncrements=false is not supported"));
    }
    tokenFilterFactory("Type", Version.LUCENE_4_4_0, "enablePositionIncrements", "true", "types", "stoptypes-1.txt");

    try {
      tokenFilterFactory("Type", "enablePositionIncrements", "true", "types", "stoptypes-1.txt");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("not a valid option"));
    }
  }
}
