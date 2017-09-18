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
package org.apache.lucene.analysis.bn;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Test Bengali Filter Factory
 */
public class TestBengaliFilters extends BaseTokenStreamFactoryTestCase {
  /**
   * Test IndicNormalizationFilterFactory
   */
  public void testIndicNormalizer() throws Exception {
    Reader reader = new StringReader("ত্‍ আমি");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] { "ৎ", "আমি" });
  }
  
  /**
   * Test BengaliNormalizationFilterFactory
   */
  public void testBengaliNormalizer() throws Exception {
    Reader reader = new StringReader("বাড়ী");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    stream = tokenFilterFactory("BengaliNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] {"বারি"});
  }
  
  /**
   * Test BengaliStemFilterFactory
   */
  public void testStemmer() throws Exception {
    Reader reader = new StringReader("বাড়ী");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    stream = tokenFilterFactory("BengaliNormalization").create(stream);
    stream = tokenFilterFactory("BengaliStem").create(stream);
    assertTokenStreamContents(stream, new String[] {"বার"});
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("IndicNormalization", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("BengaliNormalization", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
    
    expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("BengaliStem", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
