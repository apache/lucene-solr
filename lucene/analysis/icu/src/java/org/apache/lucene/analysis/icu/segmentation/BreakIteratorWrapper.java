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
package org.apache.lucene.analysis.icu.segmentation;


import java.text.CharacterIterator;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.text.UTF16;

/**
 * Contain all the issues surrounding BreakIterators in ICU in one place.
 * Basically this boils down to the fact that they aren't very friendly to any
 * sort of OO design.
 * <p>
 * http://bugs.icu-project.org/trac/ticket/5901: RBBI.getRuleStatus(), hoist to
 * BreakIterator from RuleBasedBreakIterator
 * <p>
 * DictionaryBasedBreakIterator is a subclass of RuleBasedBreakIterator, but
 * doesn't actually behave as a subclass: it always returns 0 for
 * getRuleStatus(): 
 * http://bugs.icu-project.org/trac/ticket/4730: Thai RBBI, no boundary type
 * tags
 * @lucene.experimental
 */
abstract class BreakIteratorWrapper {
  protected final CharArrayIterator textIterator = new CharArrayIterator();
  protected char text[];
  protected int start;
  protected int length;

  abstract int next();
  abstract int current();
  abstract int getRuleStatus();
  abstract void setText(CharacterIterator text);

  void setText(char text[], int start, int length) {
    this.text = text;
    this.start = start;
    this.length = length;
    textIterator.setText(text, start, length);
    setText(textIterator);
  }

  /**
   * If it's a RuleBasedBreakIterator, the rule status can be used for token type. If it's
   * any other BreakIterator, the rulestatus method is not available, so treat
   * it like a generic BreakIterator.
   */
  static BreakIteratorWrapper wrap(BreakIterator breakIterator) {
    if (breakIterator instanceof RuleBasedBreakIterator)
      return new RBBIWrapper((RuleBasedBreakIterator) breakIterator);
    else
      return new BIWrapper(breakIterator);
  }

  /**
   * RuleBasedBreakIterator wrapper: RuleBasedBreakIterator (as long as it's not
   * a DictionaryBasedBreakIterator) behaves correctly.
   */
  static final class RBBIWrapper extends BreakIteratorWrapper {
    private final RuleBasedBreakIterator rbbi;

    RBBIWrapper(RuleBasedBreakIterator rbbi) {
      this.rbbi = rbbi;
    }

    @Override
    int current() {
      return rbbi.current();
    }

    @Override
    int getRuleStatus() {
      return rbbi.getRuleStatus();
    }

    @Override
    int next() {
      return rbbi.next();
    }

    @Override
    void setText(CharacterIterator text) {
      rbbi.setText(text);
    }
  }

  /**
   * Generic BreakIterator wrapper: Either the rulestatus method is not
   * available or always returns 0. Calculate a rulestatus here so it behaves
   * like RuleBasedBreakIterator.
   * 
   * Note: This is slower than RuleBasedBreakIterator.
   */
  static final class BIWrapper extends BreakIteratorWrapper {
    private final BreakIterator bi;
    private int status;

    BIWrapper(BreakIterator bi) {
      this.bi = bi;
    }

    @Override
    int current() {
      return bi.current();
    }

    @Override
    int getRuleStatus() {
      return status;
    }

    @Override
    int next() {
      int current = bi.current();
      int next = bi.next();
      status = calcStatus(current, next);
      return next;
    }

    private int calcStatus(int current, int next) {
      if (current == BreakIterator.DONE || next == BreakIterator.DONE)
        return RuleBasedBreakIterator.WORD_NONE;

      int begin = start + current;
      int end = start + next;

      int codepoint;
      for (int i = begin; i < end; i += UTF16.getCharCount(codepoint)) {
        codepoint = UTF16.charAt(text, 0, end, begin);

        if (UCharacter.isDigit(codepoint))
          return RuleBasedBreakIterator.WORD_NUMBER;
        else if (UCharacter.isLetter(codepoint)) {
          // TODO: try to separately specify ideographic, kana? 
          // [currently all bundled as letter for this case]
          return RuleBasedBreakIterator.WORD_LETTER;
        }
      }

      return RuleBasedBreakIterator.WORD_NONE;
    }

    @Override
    void setText(CharacterIterator text) {
      bi.setText(text);
      status = RuleBasedBreakIterator.WORD_NONE;
    }
  }
}
