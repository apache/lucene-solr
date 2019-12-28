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

package org.apache.lucene.search.uhighlight;

import java.text.BreakIterator;
import java.text.CharacterIterator;

/**
 * Wraps another {@link BreakIterator} to skip past breaks that would result in passages that are too
 * short.  It's still possible to get a short passage but only at the very end of the input text.
 * <p>
 * Important: This is not a general purpose {@link BreakIterator}; it's only designed to work in a way
 * compatible with the {@link UnifiedHighlighter}.  Some assumptions are checked with Java assertions.
 *
 * @lucene.experimental
 */
public class LengthGoalBreakIterator extends BreakIterator {

  private final BreakIterator baseIter;
  private final int lengthGoal;
  private final float fragmentAlignment; // how much text to align before match-fragment, valid in range [0, 1]
  private final boolean isMinimumLength; // if false then is "closest to" length

  /** Breaks will be at least {@code minLength} apart (to the extent possible),
   *  while trying to position the match inside the fragment according to {@code fragmentAlignment}. */
  public static LengthGoalBreakIterator createMinLength(BreakIterator baseIter, int minLength,
                                                        float fragmentAlignment) {
    return new LengthGoalBreakIterator(baseIter, minLength, fragmentAlignment, true);
  }

  /** For backwards compatibility you can initialise the break iterator without fragmentAlignment. */
  @Deprecated
  public static LengthGoalBreakIterator createMinLength(BreakIterator baseIter, int minLength) {
    return createMinLength(baseIter, minLength, 0.f);
  }

  /** Breaks will be on average {@code targetLength} apart; the closest break to this target (before or after)
   * is chosen. The match will be positioned according to {@code fragmentAlignment} as much as possible. */
  public static LengthGoalBreakIterator createClosestToLength(BreakIterator baseIter, int targetLength,
                                                              float fragmentAlignment) {
    return new LengthGoalBreakIterator(baseIter, targetLength, fragmentAlignment, false);
  }

  /** For backwards compatibility you can initialise the break iterator without fragmentAlignment. */
  @Deprecated
  public static LengthGoalBreakIterator createClosestToLength(BreakIterator baseIter, int targetLength) {
    return createClosestToLength(baseIter, targetLength, 0.f);
  }

  private LengthGoalBreakIterator(BreakIterator baseIter, int lengthGoal, float fragmentAlignment,
                                  boolean isMinimumLength) {
    this.baseIter = baseIter;
    this.lengthGoal = lengthGoal;
    if (fragmentAlignment < 0.f || fragmentAlignment > 1.f || !Float.isFinite(fragmentAlignment)) {
      throw new IllegalArgumentException("fragmentAlignment must be >= zero and <= one");
    }
    this.fragmentAlignment = fragmentAlignment;
    this.isMinimumLength = isMinimumLength;
  }

  // note: the only methods that will get called are setText(txt), getText(),
  // getSummaryPassagesNoHighlight: current(), first(), next()
  // highlightOffsetsEnums: preceding(int), and following(int)
  //   Nonetheless we make some attempt to implement the rest; mostly delegating.

  @Override
  public String toString() {
    String goalDesc = isMinimumLength ? "minLen" : "targetLen";
    return getClass().getSimpleName() + "{" + goalDesc + "=" + lengthGoal + ", fragAlign=" + fragmentAlignment +
        ", baseIter=" + baseIter + "}";
  }

  @Override
  public Object clone() {
    return new LengthGoalBreakIterator((BreakIterator) baseIter.clone(), lengthGoal, fragmentAlignment, isMinimumLength);
  }

  @Override
  public CharacterIterator getText() {
    return baseIter.getText();
  }

  @Override
  public void setText(String newText) {
    baseIter.setText(newText);
  }

  @Override
  public void setText(CharacterIterator newText) {
    baseIter.setText(newText);
  }

  @Override
  public int current() {
    return baseIter.current();
  }

  @Override
  public int first() {
    return baseIter.first();
  }

  @Override
  public int last() {
    return baseIter.last();
  }

  @Override
  public int next(int n) {
    assert false : "Not supported";
    return baseIter.next(n); // probably wrong
  }

  // Called by getSummaryPassagesNoHighlight to generate default summary.
  // This is the same implementation that following() has, except:
  //   - we use the full lengthGoal for targetIdx
  //   - in closestTo mode the current index has to be moved back to afterIdx if that's the closer one
  @Override
  public int next() {
    final int matchEndIndex = current();
    final int targetIdx = matchEndIndex + lengthGoal;
    if (targetIdx >= getText().getEndIndex()) {
      return baseIter.last();
    }
    final int afterIdx = baseIter.following(targetIdx - 1);
    if (afterIdx == DONE) {
      return baseIter.current();
    }
    if (afterIdx == targetIdx) { // right on the money
      return afterIdx;
    }
    if (isMinimumLength) { // thus never undershoot
      return afterIdx;
    }

    // note: it is a shame that we invoke preceding() *one more time*; BI's are sometimes expensive.

    // Find closest break to target
    final int beforeIdx = baseIter.preceding(targetIdx);
    if (targetIdx - beforeIdx < afterIdx - targetIdx && beforeIdx > matchEndIndex) {
      return beforeIdx;
    }
    // moveToBreak is necessary because we use current() each call to continue from previous position
    return moveToBreak(afterIdx);
  }

  @Override
  public int previous() {
    assert false : "Not supported";
    return baseIter.previous();
  }

  @Override
  public int following(int matchEndIndex) {
    final int targetIdx = (matchEndIndex + 1) + (int)(lengthGoal * (1.f - fragmentAlignment));
    if (targetIdx >= getText().getEndIndex()) {
      return baseIter.last();
    }
    final int afterIdx = baseIter.following(targetIdx - 1);
    if (afterIdx == DONE) {
      return baseIter.current();
    }
    if (afterIdx == targetIdx) { // right on the money
      return afterIdx;
    }
    if (isMinimumLength) { // thus never undershoot
      return afterIdx;
    }

    // note: it is a shame that we invoke preceding() *one more time*; BI's are sometimes expensive.

    // Find closest break to target
    final int beforeIdx = baseIter.preceding(targetIdx);
    if (targetIdx - beforeIdx < afterIdx - targetIdx && beforeIdx > matchEndIndex) {
      return beforeIdx;
    }
    return afterIdx;
  }

  private int moveToBreak(int idx) { // precondition: idx is a known break
    // bi.isBoundary(idx) has side-effect of moving the position.  Not obvious!
    //boolean moved = baseIter.isBoundary(idx); // probably not particularly expensive
    //assert moved && current() == idx;

    // TODO fix: Would prefer to do "- 1" instead of "- 2" but CustomSeparatorBreakIterator has a bug.
    int current = baseIter.following(idx - 2);
    assert current == idx : "following() didn't move us to the expected index.";
    return idx;
  }

  // called at start of new Passage given first word start offset
  @Override
  public int preceding(int matchStartIndex) {
    final int targetIdx = (matchStartIndex - 1) - (int)(lengthGoal * fragmentAlignment);
    if (targetIdx <= 0) {
      return 0;
    }
    final int beforeIdx = baseIter.preceding(targetIdx + 1);
    if (beforeIdx == DONE) {
      return 0;
    }
    if (beforeIdx == targetIdx) { // right on the money
      return beforeIdx;
    }
    if (isMinimumLength) { // thus never undershoot
      return beforeIdx;
    }

    // note: it is a shame that we invoke following() *one more time*; BI's are sometimes expensive.

    // Find closest break to target
    final int afterIdx = baseIter.following(targetIdx - 1);
    if (afterIdx - targetIdx < targetIdx - beforeIdx && afterIdx < matchStartIndex) {
      return afterIdx;
    }
    return beforeIdx;
  }

  @Override
  public boolean isBoundary(int offset) {
    assert false : "Not supported";
    return baseIter.isBoundary(offset);
  }
}
