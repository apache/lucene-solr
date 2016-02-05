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
package org.apache.lucene.analysis.phonetic;


import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;

/** Simple tests for {@link BeiderMorseFilterFactory} */
public class TestBeiderMorseFilterFactory extends BaseTokenStreamTestCase {
  public void testBasics() throws Exception {
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory(new HashMap<String,String>());
    TokenStream ts = factory.create(whitespaceMockTokenizer("Weinberg"));
    assertTokenStreamContents(ts,
        new String[] { "vDnbYrk", "vDnbirk", "vanbYrk", "vanbirk", "vinbYrk", "vinbirk", "wDnbirk", "wanbirk", "winbirk" },
        new int[] { 0, 0, 0, 0, 0, 0, 0, 0, 0},
        new int[] { 8, 8, 8, 8, 8, 8, 8, 8, 8},
        new int[] { 1, 0, 0, 0, 0, 0, 0, 0, 0});
  }
  
  public void testLanguageSet() throws Exception {
    Map<String,String> args = new HashMap<>();
    args.put("languageSet", "polish");
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory(args);
    TokenStream ts = factory.create(whitespaceMockTokenizer("Weinberg"));
    assertTokenStreamContents(ts,
        new String[] { "vDmbYrk", "vDmbirk", "vambYrk", "vambirk", "vimbYrk", "vimbirk" },
        new int[] { 0, 0, 0, 0, 0, 0 },
        new int[] { 8, 8, 8, 8, 8, 8 },
        new int[] { 1, 0, 0, 0, 0, 0 });
  }
  
  public void testOptions() throws Exception {
    Map<String,String> args = new HashMap<>();
    args.put("nameType", "ASHKENAZI");
    args.put("ruleType", "EXACT");
    BeiderMorseFilterFactory factory = new BeiderMorseFilterFactory(args);
    TokenStream ts = factory.create(whitespaceMockTokenizer("Weinberg"));
    assertTokenStreamContents(ts,
        new String[] { "vajnberk" },
        new int[] { 0 },
        new int[] { 8 },
        new int[] { 1 });
  }
  
  /** Test that bogus arguments result in exception */
  public void testBogusArguments() throws Exception {
    try {
      new BeiderMorseFilterFactory(new HashMap<String,String>() {{
        put("bogusArg", "bogusValue");
      }});
      fail();
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Unknown parameters"));
    }
  }
}
