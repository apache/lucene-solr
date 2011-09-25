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

import java.io.Reader;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Factory for {@link MockTokenizer} for testing purposes.
 */
public class MockTokenizerFactory extends BaseTokenizerFactory {
  CharacterRunAutomaton pattern;
  boolean enableChecks;
  
  @Override
  public void init(Map<String,String> args) {
    super.init(args);
    String patternArg = args.get("pattern");
    if (patternArg == null) {
      patternArg = "whitespace";
    }
    
    if ("whitespace".equalsIgnoreCase(patternArg)) {
      pattern = MockTokenizer.WHITESPACE;
    } else if ("keyword".equalsIgnoreCase(patternArg)) {
      pattern = MockTokenizer.KEYWORD;
    } else if ("simple".equalsIgnoreCase(patternArg)) {
      pattern = MockTokenizer.SIMPLE;
    } else {
      throw new RuntimeException("invalid pattern!");
    }
    
    enableChecks = getBoolean("enableChecks", true);
  }


  @Override
  public Tokenizer create(Reader input) {
    MockTokenizer t = new MockTokenizer(input, pattern, false);
    t.setEnableChecks(enableChecks);
    return t;
  }
}