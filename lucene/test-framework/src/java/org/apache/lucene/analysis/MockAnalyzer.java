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
package org.apache.lucene.analysis;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * Analyzer for testing
 * <p>
 * This analyzer is a replacement for Whitespace/Simple/KeywordAnalyzers
 * for unit tests. If you are testing a custom component such as a queryparser
 * or analyzer-wrapper that consumes analysis streams, it's a great idea to test
 * it with this analyzer instead. MockAnalyzer has the following behavior:
 * <ul>
 *   <li>By default, the assertions in {@link MockTokenizer} are turned on for extra
 *       checks that the consumer is consuming properly. These checks can be disabled
 *       with {@link #setEnableChecks(boolean)}.
 *   <li>Payload data is randomly injected into the stream for more thorough testing
 *       of payloads.
 * </ul>
 * @see MockTokenizer
 */
public final class MockAnalyzer extends Analyzer {
  private final CharacterRunAutomaton runAutomaton;
  private final boolean lowerCase;
  private final CharacterRunAutomaton filter;
  private int positionIncrementGap;
  private Integer offsetGap;
  private final Random random;
  private Map<String,Integer> previousMappings = new HashMap<>();
  private boolean enableChecks = true;
  private int maxTokenLength = MockTokenizer.DEFAULT_MAX_TOKEN_LENGTH;

  /**
   * Creates a new MockAnalyzer.
   * 
   * @param random Random for payloads behavior
   * @param runAutomaton DFA describing how tokenization should happen (e.g. [a-zA-Z]+)
   * @param lowerCase true if the tokenizer should lowercase terms
   * @param filter DFA describing how terms should be filtered (set of stopwords, etc)
   */
  public MockAnalyzer(Random random, CharacterRunAutomaton runAutomaton, boolean lowerCase, CharacterRunAutomaton filter) {
    super(PER_FIELD_REUSE_STRATEGY);
    // TODO: this should be solved in a different way; Random should not be shared (!).
    this.random = new Random(random.nextLong());
    this.runAutomaton = runAutomaton;
    this.lowerCase = lowerCase;
    this.filter = filter;
  }

  /**
   * Calls {@link #MockAnalyzer(Random, CharacterRunAutomaton, boolean, CharacterRunAutomaton) 
   * MockAnalyzer(random, runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET, false}).
   */
  public MockAnalyzer(Random random, CharacterRunAutomaton runAutomaton, boolean lowerCase) {
    this(random, runAutomaton, lowerCase, MockTokenFilter.EMPTY_STOPSET);
  }

  /** 
   * Create a Whitespace-lowercasing analyzer with no stopwords removal.
   * <p>
   * Calls {@link #MockAnalyzer(Random, CharacterRunAutomaton, boolean, CharacterRunAutomaton) 
   * MockAnalyzer(random, MockTokenizer.WHITESPACE, true, MockTokenFilter.EMPTY_STOPSET, false}).
   */
  public MockAnalyzer(Random random) {
    this(random, MockTokenizer.WHITESPACE, true);
  }

  @Override
  public TokenStreamComponents createComponents(String fieldName) {
    MockTokenizer tokenizer = new MockTokenizer(runAutomaton, lowerCase, maxTokenLength);
    tokenizer.setEnableChecks(enableChecks);
    MockTokenFilter filt = new MockTokenFilter(tokenizer, filter);
    return new TokenStreamComponents(tokenizer, maybePayload(filt, fieldName));
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    TokenStream result = in;
    if (lowerCase) {
      result = new MockLowerCaseFilter(result);
    }
    return result;
  }

  private synchronized TokenFilter maybePayload(TokenFilter stream, String fieldName) {
    Integer val = previousMappings.get(fieldName);
    if (val == null) {
      val = -1; // no payloads
      if (LuceneTestCase.rarely(random)) {
        switch(random.nextInt(3)) {
          case 0: val = -1; // no payloads
                  break;
          case 1: val = Integer.MAX_VALUE; // variable length payload
                  break;
          case 2: val = random.nextInt(12); // fixed length payload
                  break;
        }
      }
      if (LuceneTestCase.VERBOSE) {
        if (val == Integer.MAX_VALUE) {
          System.out.println("MockAnalyzer: field=" + fieldName + " gets variable length payloads");
        } else if (val != -1) {
          System.out.println("MockAnalyzer: field=" + fieldName + " gets fixed length=" + val + " payloads");
        }
      }
      previousMappings.put(fieldName, val); // save it so we are consistent for this field
    }
    
    if (val == -1)
      return stream;
    else if (val == Integer.MAX_VALUE)
      return new MockVariableLengthPayloadFilter(random, stream);
    else
      return new MockFixedLengthPayloadFilter(random, stream, val);
  }
  
  public void setPositionIncrementGap(int positionIncrementGap){
    this.positionIncrementGap = positionIncrementGap;
  }
  
  @Override
  public int getPositionIncrementGap(String fieldName){
    return positionIncrementGap;
  }

  /**
   * Set a new offset gap which will then be added to the offset when several fields with the same name are indexed
   * @param offsetGap The offset gap that should be used.
   */
  public void setOffsetGap(int offsetGap){
    this.offsetGap = offsetGap;
  }

  /**
   * Get the offset gap between tokens in fields if several fields with the same name were added.
   * @param fieldName Currently not used, the same offset gap is returned for each field.
   */
  @Override
  public int getOffsetGap(String fieldName){
    return offsetGap == null ? super.getOffsetGap(fieldName) : offsetGap;
  }
  
  /** 
   * Toggle consumer workflow checking: if your test consumes tokenstreams normally you
   * should leave this enabled.
   */
  public void setEnableChecks(boolean enableChecks) {
    this.enableChecks = enableChecks;
  }
  
  /** 
   * Toggle maxTokenLength for MockTokenizer
   */
  public void setMaxTokenLength(int length) {
    this.maxTokenLength = length;
  }
}
