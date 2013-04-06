package org.apache.lucene.analysis.synonym;

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

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.BaseTokenStreamFactoryTestCase;
import org.apache.lucene.analysis.util.StringMockResourceLoader;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @since solr 1.4
 */
public class TestMultiWordSynonyms extends BaseTokenStreamFactoryTestCase {

  /**
   * @deprecated Remove this test in 5.0
   */
  @Deprecated
  public void testMultiWordSynonymsOld() throws IOException {
    List<String> rules = new ArrayList<String>();
    rules.add("a b c,d");
    SlowSynonymMap synMap = new SlowSynonymMap(true);
    SlowSynonymFilterFactory.parseRules(rules, synMap, "=>", ",", true, null);

    SlowSynonymFilter ts = new SlowSynonymFilter(new MockTokenizer(new StringReader("a e"), MockTokenizer.WHITESPACE, false), synMap);
    // This fails because ["e","e"] is the value of the token stream
    assertTokenStreamContents(ts, new String[] { "a", "e" });
  }
  
  public void testMultiWordSynonyms() throws Exception {
    Reader reader = new StringReader("a e");
    TokenStream stream = new MockTokenizer(reader, MockTokenizer.WHITESPACE, false);
    stream = tokenFilterFactory("Synonym", TEST_VERSION_CURRENT,
        new StringMockResourceLoader("a b c,d"),
        "synonyms", "synonyms.txt").create(stream);
    // This fails because ["e","e"] is the value of the token stream
    assertTokenStreamContents(stream, new String[] { "a", "e" });
  }
}
