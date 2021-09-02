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
package org.apache.lucene.analysis.te;

import java.io.Reader;
import java.io.StringReader;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.TokenStream;

/** Simple tests to ensure the Telugu filter Factories are working. */
public class TestTeluguFilters extends BaseTokenStreamFactoryTestCase {
  /** Test IndicNormalizationFilterFactory */
  public void testIndicNormalizer() throws Exception {
    Reader reader = new StringReader("ప్  अाैर");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] {"ప్", "और"});
  }

  /** Test TeluguNormalizationFilterFactory */
  public void testTeluguNormalizer() throws Exception {
    Reader reader = new StringReader("వస్తువులు");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    stream = tokenFilterFactory("TeluguNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] {"వస్తుమలు"});
  }

  /** Test TeluguStemFilterFactory */
  public void testStemmer() throws Exception {
    Reader reader = new StringReader("వస్తువులు");
    TokenStream stream = whitespaceMockTokenizer(reader);
    stream = tokenFilterFactory("IndicNormalization").create(stream);
    stream = tokenFilterFactory("TeluguNormalization").create(stream);
    stream = tokenFilterFactory("TeluguStem").create(stream);
    assertTokenStreamContents(stream, new String[] {"వస్తుమ"});
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("IndicNormalization", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("TeluguNormalization", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));

    expected =
        expectThrows(
            IllegalArgumentException.class,
            () -> {
              tokenFilterFactory("TeluguStem", "bogusArg", "bogusValue");
            });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
