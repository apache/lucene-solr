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
package org.apache.solr.analysis;

import java.util.Arrays;
import java.util.Map;

import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Factory for {@link MockTokenizer} for testing purposes.
 */
public class MockTokenizerFactory extends TokenizerFactory {

  /** SPI name */
  public static final String NAME = "mock";

  final CharacterRunAutomaton pattern;
  final boolean enableChecks;
  
  /** Creates a new MockTokenizerFactory */
  public MockTokenizerFactory(Map<String,String> args) {
    super(args);
    String patternArg = get(args, "pattern", Arrays.asList("keyword", "simple", "whitespace"));
    if ("keyword".equalsIgnoreCase(patternArg)) {
      pattern = MockTokenizer.KEYWORD;
    } else if ("simple".equalsIgnoreCase(patternArg)) {
      pattern = MockTokenizer.SIMPLE;
    } else {
      pattern = MockTokenizer.WHITESPACE;
    }
    
    enableChecks = getBoolean(args, "enableChecks", true);
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("Unknown parameters: " + args);
    }
  }

  @Override
  public MockTokenizer create(AttributeFactory factory) {
    MockTokenizer t = new MockTokenizer(factory, pattern, false);
    t.setEnableChecks(enableChecks);
    return t;
  }
}