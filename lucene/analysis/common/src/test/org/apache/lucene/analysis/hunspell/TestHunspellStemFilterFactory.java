package org.apache.lucene.analysis.hunspell;

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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.ResourceAsStreamResourceLoader;

/**
 * Simple tests to ensure the Hunspell stemmer loads from factory
 */
public class TestHunspellStemFilterFactory extends BaseTokenStreamTestCase {
  public void testStemming() throws Exception {
    HunspellStemFilterFactory factory = new HunspellStemFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("dictionary", "test.dic");
    args.put("affix", "test.aff");
    factory.setLuceneMatchVersion(TEST_VERSION_CURRENT);
    factory.init(args);
    factory.inform(new ResourceAsStreamResourceLoader(getClass()));
    
    Reader reader = new StringReader("abc");
    TokenStream stream = factory.create(new MockTokenizer(reader, MockTokenizer.WHITESPACE, false));
    assertTokenStreamContents(stream, new String[] { "ab" });
  }
}
