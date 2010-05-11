package org.apache.lucene.analysis;

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

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer {
  public static final CharacterRunAutomaton WHITESPACE = 
    new CharacterRunAutomaton(new RegExp("[^ \t\r\n]+").toAutomaton());
  public static final CharacterRunAutomaton KEYWORD =
    new CharacterRunAutomaton(new RegExp(".*").toAutomaton());

  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
  }
  
  public MockAnalyzer() {
    this(WHITESPACE, true);
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    return new MockTokenizer(reader, runAutomaton, lowerCase);
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    MockTokenizer t = (MockTokenizer) getPreviousTokenStream();
    if (t == null) {
      t = new MockTokenizer(reader, runAutomaton, lowerCase);
      setPreviousTokenStream(t);
    } else {
      t.reset(reader);
    }
    return t;
  }
}