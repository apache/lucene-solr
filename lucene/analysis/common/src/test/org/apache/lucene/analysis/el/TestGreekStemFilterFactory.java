package org.apache.lucene.analysis.el;

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

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.el.GreekLowerCaseFilter;

/**
 * Simple tests to ensure the Greek stem filter factory is working.
 */
public class TestGreekStemFilterFactory extends BaseTokenStreamTestCase {
  public void testStemming() throws Exception {
    Reader reader = new StringReader("άνθρωπος");
    Tokenizer tokenizer = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    TokenStream normalized = new GreekLowerCaseFilter(TEST_VERSION_CURRENT, tokenizer);
    GreekStemFilterFactory factory = new GreekStemFilterFactory();
    TokenStream stream = factory.create(normalized);
    assertTokenStreamContents(stream, new String[] { "ανθρωπ" });
  }
}
