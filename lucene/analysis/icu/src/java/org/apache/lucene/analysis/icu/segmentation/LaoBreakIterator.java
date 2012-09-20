package org.apache.lucene.analysis.icu.segmentation;

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

import java.text.CharacterIterator;

import com.ibm.icu.lang.UCharacter;
import com.ibm.icu.text.BreakIterator;
import com.ibm.icu.text.RuleBasedBreakIterator;
import com.ibm.icu.text.UnicodeSet;

/**
 * Syllable iterator for Lao text.
 * <p>
 * This breaks Lao text into syllables according to:
 * <i>Syllabification of Lao Script for Line Breaking</i>
 * Phonpasit Phissamay, Valaxay Dalolay, Chitaphone Chanhsililath, Oulaiphone Silimasak, 
 * Sarmad Hussain, Nadir Durrani, Science Technology and Environment Agency, CRULP.
 * <ul>
 *  <li>http://www.panl10n.net/english/final%20reports/pdf%20files/Laos/LAO06.pdf
 *  <li>http://www.panl10n.net/Presentations/Cambodia/Phonpassit/LineBreakingAlgo.pdf
 * </ul>
 * <p>
 * Most work is accomplished with RBBI rules, however some additional special logic is needed
 * that cannot be coded in a grammar, and this is implemented here.
 * <p>
 * For example, what appears to be a final consonant might instead be part of the next syllable.
 * Rules match in a greedy fashion, leaving an illegal sequence that matches no rules.
 * <p>
 * Take for instance the text ກວ່າດອກ
 * The first rule greedily matches ກວ່າດ, but then ອກ is encountered, which is illegal.
 * What LaoBreakIterator does, according to the paper:
 * <ol>
 *  <li>backtrack and remove the ດ from the last syllable, placing it on the current syllable.
 *  <li>verify the modified previous syllable (ກວ່າ ) is still legal.
 *  <li>verify the modified current syllable (ດອກ) is now legal.
 *  <li>If 2 or 3 fails, then restore the ດ to the last syllable and skip the current character.
 * </ol>
 * <p>
 * Finally, LaoBreakIterator also takes care of the second concern mentioned in the paper.
 * This is the issue of combining marks being in the wrong order (typos).
 * @lucene.experimental
 */
public class LaoBreakIterator extends BreakIterator {
  RuleBasedBreakIterator rules;
  CharArrayIterator text;
  
  CharArrayIterator working = new CharArrayIterator();
  int workingOffset = 0;
  
  CharArrayIterator verifyText = new CharArrayIterator();
  RuleBasedBreakIterator verify;
  
  private static final UnicodeSet laoSet;
  static {
    laoSet = new UnicodeSet("[:Lao:]");
    laoSet.compact();
    laoSet.freeze();
  }
  
  /** 
   * Creates a new iterator, performing the backtracking verification
   * across the provided <code>rules</code>.
   */
  public LaoBreakIterator(RuleBasedBreakIterator rules) {
    this.rules = (RuleBasedBreakIterator) rules.clone();
    this.verify = (RuleBasedBreakIterator) rules.clone();
  }

  @Override
  public int current() {
    int current = rules.current();
    return current == BreakIterator.DONE ? BreakIterator.DONE : workingOffset + current;
  }

  @Override
  public int first() {
    working.setText(this.text.getText(), this.text.getStart(), this.text.getLength());
    rules.setText(working);
    workingOffset = 0;
    int first = rules.first();
    return first == BreakIterator.DONE ? BreakIterator.DONE : workingOffset + first;
  }

  @Override
  public int following(int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CharacterIterator getText() {
    return text;
  }

  @Override
  public int last() {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public int next() {
    int current = current();
    int next = rules.next();
    if (next == BreakIterator.DONE)
      return next;
    else
      next += workingOffset;
    
    char c = working.current();
    int following = rules.next(); // lookahead
    if (following != BreakIterator.DONE) {
      following += workingOffset;
      if (rules.getRuleStatus() == 0 && laoSet.contains(c) && verifyPushBack(current, next)) {
        workingOffset = next - 1;
        working.setText(text.getText(), text.getStart() + workingOffset, text.getLength() - workingOffset);
        return next - 1;
      }
    rules.previous(); // undo the lookahead
    }
    
    return next;
  }

  @Override
  public int next(int n) {
    if (n < 0)
      throw new UnsupportedOperationException("Backwards traversal is unsupported");

    int result = current();
    while (n > 0) {
        result = next();
        --n;
    }
    return result;
  }

  @Override
  public int previous() {
    throw new UnsupportedOperationException("Backwards traversal is unsupported");
  }

  @Override
  public void setText(CharacterIterator text) {
    if (!(text instanceof CharArrayIterator))
      throw new UnsupportedOperationException("unsupported CharacterIterator");
    this.text = (CharArrayIterator) text;
    ccReorder(this.text.getText(), this.text.getStart(), this.text.getLength());
    working.setText(this.text.getText(), this.text.getStart(), this.text.getLength());
    rules.setText(working);
    workingOffset = 0;
  }
  
  @Override
  public void setText(String newText) {
    CharArrayIterator ci = new CharArrayIterator();
    ci.setText(newText.toCharArray(), 0, newText.length());
    setText(ci);
  }
  
  private boolean verifyPushBack(int current, int next) {
    int shortenedSyllable = next - current - 1;

    verifyText.setText(text.getText(), text.getStart() + current, shortenedSyllable);
    verify.setText(verifyText);
    if (verify.next() != shortenedSyllable || verify.getRuleStatus() == 0)
      return false;
    

    verifyText.setText(text.getText(), text.getStart() + next - 1, text.getLength() - next + 1);
    verify.setText(verifyText);

    return (verify.next() != BreakIterator.DONE && verify.getRuleStatus() != 0);
  }

  // TODO: only bubblesort around runs of combining marks, instead of the entire text.
  private void ccReorder(char[] text, int start, int length) {
    boolean reordered;
    do {
      int prevCC = 0;
      reordered = false;
      for (int i = start; i < start + length; i++) {
        final char c = text[i];
        final int cc = UCharacter.getCombiningClass(c);
        if (cc > 0 && cc < prevCC) {
          // swap
          text[i] = text[i - 1];
          text[i - 1] = c;
          reordered = true;
        } else {
          prevCC = cc;
        }
      }

    } while (reordered == true);
  }
  
  /**
   * Clone method.  Creates another LaoBreakIterator with the same behavior 
   * and current state as this one.
   * @return The clone.
   */
  @Override
  public LaoBreakIterator clone() {
    LaoBreakIterator other = (LaoBreakIterator) super.clone();
    other.rules = (RuleBasedBreakIterator) rules.clone();
    other.verify = (RuleBasedBreakIterator) verify.clone();
    if (text != null)
      other.text = text.clone();
    if (working != null)
      other.working = working.clone();
    if (verifyText != null)
      other.verifyText = verifyText.clone();
    return other;
  }
}
