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

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.index.Payload;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Analyzer for testing
 */
public final class MockAnalyzer extends Analyzer { 
  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final CharacterRunAutomaton filter;
  private final boolean enablePositionIncrements;
  private final boolean payload;
  private int positionIncrementGap;

  /**
   * Calls {@link #MockAnalyzer(CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean, boolean) 
   * MockAnalyzer(runAutomaton, lowerCase, filter, enablePositionIncrements, true}).
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase, CharacterRunAutomaton filter, boolean enablePositionIncrements) {
    this(runAutomaton, lowerCase, filter, enablePositionIncrements, true);    
  }

  /**
   * Creates a new MockAnalyzer.
   * 
   * @param runAutomaton DFA describing how tokenization should happen (e.g. [a-zA-Z]+)
   * @param lowerCase true if the tokenizer should lowercase terms
   * @param filter DFA describing how terms should be filtered (set of stopwords, etc)
   * @param enablePositionIncrements true if position increments should reflect filtered terms.
   * @param payload if payloads should be added containing the positions (for testing)
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase, CharacterRunAutomaton filter, boolean enablePositionIncrements, boolean payload) {
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.filter = filter;
    this.enablePositionIncrements = enablePositionIncrements;
    this.payload = payload;
  }

  /**
   * Calls {@link #MockAnalyzer(CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean, boolean) 
   * MockAnalyzer(runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false, true}).
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false, true);
  }

  /**
   * Calls {@link #MockAnalyzer(CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean, boolean) 
   * MockAnalyzer(runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false, payload}).
   */
  public MockAnalyzer(CharacterRunAutomaton runAutomaton, boolean lowerCase, boolean payload) {
    this(runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false, payload);
  }
  
  /** 
   * Create a Whitespace-lowercasing analyzer with no stopwords removal.
   * <p>
   * Calls {@link #MockAnalyzer(CharacterRunAutomaton, boolean, CharacterRunAutomaton, boolean, boolean) 
   * MockAnalyzer(MockTokenizer.WHITESPACE, true, MockTokenFilter.EMPTY_STOPSET, false, true}).
   */
  public MockAnalyzer() {
    this(MockTokenizer.WHITESPACE, true);
  }

  @Override
  public TokenStream tokenStream(String fieldName, Reader reader) {
    MockTokenizer tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
    TokenFilter filt = new MockTokenFilter(tokenizer, filter, enablePositionIncrements);
    if (payload){
      filt = new SimplePayloadFilter(filt, fieldName);
    }
    return filt;
  }

  private class SavedStreams {
    MockTokenizer tokenizer;
    TokenFilter filter;
  }

  @Override
  public TokenStream reusableTokenStream(String fieldName, Reader reader)
      throws IOException {
    SavedStreams saved = (SavedStreams) getPreviousTokenStream();
    if (saved == null) {
      saved = new SavedStreams();
      saved.tokenizer = new MockTokenizer(reader, runAutomaton, lowerCase);
      saved.filter = new MockTokenFilter(saved.tokenizer, filter, enablePositionIncrements);
      if (payload){
        saved.filter = new SimplePayloadFilter(saved.filter, fieldName);
      }
      setPreviousTokenStream(saved);
      return saved.filter;
    } else {
      saved.tokenizer.reset(reader);
      saved.filter.reset();
      return saved.filter;
    }
  }
  
  public void setPositionIncrementGap(int positionIncrementGap){
    this.positionIncrementGap = positionIncrementGap;
  }
  
  @Override
  public int getPositionIncrementGap(String fieldName){
    return positionIncrementGap;
  }
}

final class SimplePayloadFilter extends TokenFilter {
  String fieldName;
  int pos;
  final PayloadAttribute payloadAttr;
  final CharTermAttribute termAttr;

  public SimplePayloadFilter(TokenStream input, String fieldName) {
    super(input);
    this.fieldName = fieldName;
    pos = 0;
    payloadAttr = input.addAttribute(PayloadAttribute.class);
    termAttr = input.addAttribute(CharTermAttribute.class);
  }

  @Override
  public boolean incrementToken() throws IOException {
    if (input.incrementToken()) {
      payloadAttr.setPayload(new Payload(("pos: " + pos).getBytes()));
      pos++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    pos = 0;
  }
}
