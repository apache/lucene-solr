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

import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.text.UTF16;
import com.ibm.icu.text.UnicodeSet;

/**
 * Wraps RuleBasedBreakIterator, making object reuse convenient and 
 * emitting a rule status for emoji sequences.
 * @lucene.experimental
 */
final class BreakIteratorWrapper {
  private final CharArrayIterator textIterator = new CharArrayIterator();
  private final RuleBasedBreakIterator rbbi;
  private char text[];
  private int start;
  private int status;
  
  BreakIteratorWrapper(RuleBasedBreakIterator rbbi) {
    this.rbbi = rbbi;
  }
  
  int current() {
    return rbbi.current();
  }

  int getRuleStatus() {
    return status;
  }

  int next() {
    int current = rbbi.current();
    int next = rbbi.next();
    status = calcStatus(current, next);
    return next;
  }
  
  /** Returns current rule status for the text between breaks. (determines token type) */
  private int calcStatus(int current, int next) {
    // to support presentation selectors, we need to handle alphanum, num, and none at least, so currently not worth optimizing.
    // https://unicode.org/cldr/utility/list-unicodeset.jsp?a=%5B%3AEmoji%3A%5D-%5B%3AEmoji_Presentation%3A%5D&g=Word_Break&i=
    if (next != BreakIterator.DONE && isEmoji(current, next)) {
      return ICUTokenizerConfig.EMOJI_SEQUENCE_STATUS;
    } else {
      return rbbi.getRuleStatus();
    }
  }
  
  // See unicode doc L2/16-315 for rationale.
  // basically for us the ambiguous cases (keycap/etc) as far as types go.
  static final UnicodeSet EMOJI_RK = new UnicodeSet("[\u002a\u00230-9©®™〰〽]").freeze();
  // faster than doing hasBinaryProperty() checks, at the cost of 1KB ram
  static final UnicodeSet EMOJI = new UnicodeSet("[[:Emoji:][:Extended_Pictographic:]]").freeze();

  /** Returns true if the current text represents emoji character or sequence */
  private boolean isEmoji(int current, int next) {
    int begin = start + current;
    int end = start + next;
    int codepoint = UTF16.charAt(text, 0, end, begin);
    if (EMOJI.contains(codepoint)) {
      if (EMOJI_RK.contains(codepoint)) {
        // if its in EmojiRK, we don't treat it as emoji unless there is evidence it forms emoji sequence,
        // an emoji presentation selector or keycap follows.
        int trailer = begin + Character.charCount(codepoint);
        return trailer < end && (text[trailer] == 0xFE0F || text[trailer] == 0x20E3);
      } else {
        return true;
      }
    }
    return false;
  }

  void setText(char text[], int start, int length) {
    this.text = text;
    this.start = start;
    textIterator.setText(text, start, length);
    rbbi.setText(textIterator);
    status = RuleBasedBreakIterator.WORD_NONE;
  }
}
