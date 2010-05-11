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

import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Automaton-based tokenizer for testing. Optionally lowercases.
 */
public class MockTokenizer extends CharTokenizer {
  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private int state;

  public MockTokenizer(Reader input, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    super(Version.LUCENE_CURRENT, input);
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.state = runAutomaton.getInitialState();
  }
  
  @Override
  protected boolean isTokenChar(int c) {
    state = runAutomaton.step(state, c);
    if (state < 0) {
      state = runAutomaton.getInitialState();
      return false;
    } else {
      return true;
    }
  }
  
  @Override
  protected int normalize(int c) {
    return lowerCase ? Character.toLowerCase(c) : c;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    state = runAutomaton.getInitialState();
  }
}
