package org.apache.lucene.analysis;

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

import static org.apache.lucene.util.automaton.BasicAutomata.makeEmpty;
import static org.apache.lucene.util.automaton.BasicAutomata.makeString;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.util.automaton.BasicOperations;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

/**
 * A tokenfilter for testing that removes terms accepted by a DFA.
 * <ul>
 *  <li>Union a list of singletons to act like a stopfilter.
 *  <li>Use the complement to act like a keepwordfilter
 *  <li>Use a regex like <code>.{12,}</code> to act like a lengthfilter
 * </ul>
 */
public final class MockTokenFilter extends TokenFilter {
  /** Empty set of stopwords */
  public static final CharacterRunAutomaton EMPTY_STOPSET =
    new CharacterRunAutomaton(makeEmpty());
  
  /** Set of common english stopwords */
  public static final CharacterRunAutomaton ENGLISH_STOPSET = 
    new CharacterRunAutomaton(BasicOperations.union(Arrays.asList(
      makeString("a"), makeString("an"), makeString("and"), makeString("are"),
      makeString("as"), makeString("at"), makeString("be"), makeString("but"), 
      makeString("by"), makeString("for"), makeString("if"), makeString("in"), 
      makeString("into"), makeString("is"), makeString("it"), makeString("no"),
      makeString("not"), makeString("of"), makeString("on"), makeString("or"), 
      makeString("such"), makeString("that"), makeString("the"), makeString("their"), 
      makeString("then"), makeString("there"), makeString("these"), makeString("they"), 
      makeString("this"), makeString("to"), makeString("was"), makeString("will"), 
      makeString("with"))));
  
  private final CharacterRunAutomaton filter;
  private boolean enablePositionIncrements = true;

  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final PositionIncrementAttribute posIncrAtt = addAttribute(PositionIncrementAttribute.class);
  
  /**
   * Create a new MockTokenFilter.
   * 
   * @param input TokenStream to filter
   * @param filter DFA representing the terms that should be removed.
   */
  public MockTokenFilter(TokenStream input, CharacterRunAutomaton filter) {
    super(input);
    this.filter = filter;
  }
  
  @Override
  public boolean incrementToken() throws IOException {
    // TODO: fix me when posInc=false, to work like FilteringTokenFilter in that case and not return
    // initial token with posInc=0 ever
    
    // return the first non-stop word found
    int skippedPositions = 0;
    while (input.incrementToken()) {
      if (!filter.run(termAtt.buffer(), 0, termAtt.length())) {
        if (enablePositionIncrements) {
          posIncrAtt.setPositionIncrement(posIncrAtt.getPositionIncrement() + skippedPositions);
        }
        return true;
      }
      skippedPositions += posIncrAtt.getPositionIncrement();
    }
    // reached EOS -- return false
    return false;
  }
  
  /**
   * @see #setEnablePositionIncrements(boolean)
   */
  public boolean getEnablePositionIncrements() {
    return enablePositionIncrements;
  }

  /**
   * If <code>true</code>, this Filter will preserve
   * positions of the incoming tokens (ie, accumulate and
   * set position increments of the removed stop tokens).
   */
  public void setEnablePositionIncrements(boolean enable) {
    this.enablePositionIncrements = enable;
  }
}
