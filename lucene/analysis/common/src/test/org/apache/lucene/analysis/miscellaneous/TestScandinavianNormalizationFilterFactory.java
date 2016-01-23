package org.apache.lucene.analysis.miscellaneous;

/*
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

import java.io.Reader;
import java.io.StringReader;

public class TestScandinavianNormalizationFilterFactory extends BaseTokenStreamFactoryTestCase {

  public void testStemming() throws Exception {
    TokenStream stream = whitespaceMockTokenizer("räksmörgås");
    stream = tokenFilterFactory("ScandinavianNormalization").create(stream);
    assertTokenStreamContents(stream, new String[] { "ræksmørgås" });
  }

  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      tokenFilterFactory("ScandinavianNormalization",
          "bogusArg", "bogusValue");
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}