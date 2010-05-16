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

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer { 
  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final CharacterRunAutomaton filter;
  private final boolean enablePositionIncrements;

  /**
   * Creates a new MockAnalyzer.
   * 
   * @param runAutomaton DFA describing how tokenization should happen (e.g. [a-zA-Z]+)
   * @param lowerCase true if the tokenizer should lowercase terms
   * @param filter DFA describing how terms should be filtered (set of stopwords, etc)
   * @param enablePositionIncrements true if position increments should reflect filtered terms.
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase, CharacterRunAutomaton filter, boolean enablePositionIncrements) {
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.filter = filter;
    this.enablePositionIncrements = enablePositionIncrements;
  }

  /**
   * Creates a new MockAnalyzer, with no filtering.
   * 
   * @param runAutomaton DFA describing how tokenization should happen (e.g. [a-zA-Z]+)
   * @param lowerCase true if the tokenizer should lowercase terms
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false);
  }
  
  /** 
   * Create a Whitespace-lowercasing analyzer with no stopwords removal 
   */
  public MockAnalyzer() {
    this(MockTokenizer.WHITESPACE, true);
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    MockTokenizer tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
    return new MockTokenFilter(tokenizer, filter, enablePositionIncrements);
  }

  private class SavedStreams {
    MockTokenizer tokenizer;
    MockTokenFilter filter;
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    SavedStreams saved = (SavedStreams) getPreviousTokenStream();
    if (saved == null) {
      saved = new SavedStreams();
      saved.tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
      saved.filter = new MockTokenFilter(saved.tokenizer, filter, enablePositionIncrements);
      setPreviousTokenStream(saved);
      return saved.filter;
    } else {
      saved.tokenizer.reset(reader);
      return saved.filter;
    }
  }
}