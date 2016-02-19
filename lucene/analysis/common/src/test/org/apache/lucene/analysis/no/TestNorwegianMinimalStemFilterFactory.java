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
package org.apache.lucene.analysis.no;


import java.io.Reader;
import java.io.StringReader;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;

/**
 * Simple tests to ensure the Norwegian Minimal stem factory is working.
 */
public class TestNorwegianMinimalStemFilterFactory extends BaseTokenStreamFactoryTestCase {
  public void testStemming() throws Exception {
    Reader reader = new StringReader("eple eplet epler eplene eplets eplenes");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("NorwegianMinimalStem").create(stream);
    assertTokenStreamContents(stream, new String[] { "epl", "epl", "epl", "epl", "epl", "epl" });
  }
  
  /** Test stemming with variant set explicitly to Bokmål */
  public void testBokmaalStemming() throws Exception {
    Reader reader = new StringReader("eple eplet epler eplene eplets eplenes");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("NorwegianMinimalStem", "variant", "nb").create(stream);
    assertTokenStreamContents(stream, new String[] { "epl", "epl", "epl", "epl", "epl", "epl" });
  }
  
  /** Test stemming with variant set explicitly to Nynorsk */
  public void testNynorskStemming() throws Exception {
    Reader reader = new StringReader("gut guten gutar gutane gutens gutanes");
    TokenStream stream = new MockTokenizer(MockTokenizer.WHITESPACE, false);
    ((Tokenizer)stream).setReader(reader);
    stream = tokenFilterFactory("NorwegianMinimalStem", "variant", "nn").create(stream);
    assertTokenStreamContents(stream, new String[] { "gut", "gut", "gut", "gut", "gut", "gut" });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
      tokenFilterFactory("NorwegianMinimalStem", "bogusArg", "bogusValue");
    });
    assertTrue(expected.getMessage().contains("Unknown parameters"));
  }
}
