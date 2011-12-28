package org.apache.solr.analysis;

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

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.TokenStream;

/** Simple tests for {@link BeiderMorseFilterFactory} */
public class TestBeiderMorseFilterFactory extends BaseTokenTestCase {
  public void testBasics() throws Exception {
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory();
    factory.init(DEFAULT_VERSION_PARAM);
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("Weinberg"), MockTokenizer.WHITESPACE, false));
    assertTokenStreamContents(ts,
        new String[] { "vDnbirk", "vanbirk", "vinbirk", "wDnbirk", "wanbirk", "winbirk" },
        new int[] { 0, 0, 0, 0, 0, 0 },
        new int[] { 8, 8, 8, 8, 8, 8 },
        new int[] { 1, 0, 0, 0, 0, 0 });
  }
  
  public void testLanguageSet() throws Exception {
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("languageSet", "polish");
    factory.init(args);
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("Weinberg"), MockTokenizer.WHITESPACE, false));
    assertTokenStreamContents(ts,
        new String[] { "vDmbYrk", "vDmbirk", "vambYrk", "vambirk", "vimbYrk", "vimbirk" },
        new int[] { 0, 0, 0, 0, 0, 0 },
        new int[] { 8, 8, 8, 8, 8, 8 },
        new int[] { 1, 0, 0, 0, 0, 0 });
  }
  
  public void testOptions() throws Exception {
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory();
    Map<String,String> args = new HashMap<String,String>();
    args.put("nameType", "ASHKENAZI");
    args.put("ruleType", "EXACT");
    factory.init(args);
    TokenStream ts = factory.create(new MockTokenizer(new StringReader("Weinberg"), MockTokenizer.WHITESPACE, false));
    assertTokenStreamContents(ts,
        new String[] { "vajnberk" },
        new int[] { 0 },
        new int[] { 8 },
        new int[] { 1 });
  }
}
