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
  private int fragmentEndFromPreceding; // store the match-break end for reuse in following()
  private int fragmentEndFollowingLengthGoalFromPreceding; // store the remaining length to collect in following()

  /** Breaks will be at least {@code minLength} apart (to the extent possible). */
  public static LengthGoalBreakIterator createMinLength(BreakIterator baseIter, int minLength,
                                                        float fragmentAlignment) {
    return new LengthGoalBreakIterator(baseIter, minLength, fragmentAlignment,true);
  }

  /** For backwards compatibility you can initialise the break iterator without fragmentAlignment. */
  public static LengthGoalBreakIterator createMinLength(BreakIterator baseIter, int minLength) {
    return createMinLength(baseIter, minLength, 0.f);
  }

  /** Breaks will be on average {@code targetLength} apart; the closest break to this target (before or after)
   * is chosen. */
  public static LengthGoalBreakIterator createClosestToLength(BreakIterator baseIter, int targetLength,
                                                              float fragmentAlignment) {
    return new LengthGoalBreakIterator(baseIter, targetLength, fragmentAlignment, false);
  }

  /** For backwards compatibility you can initialise the break iterator without fragmentAlignment. */
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
    this.fragmentAlignment = Math.max(Math.min(fragmentAlignment, 1.f), 0.f);
    this.isMinimumLength = isMinimumLength;
    this.fragmentEndFromPreceding = 0;
    this.fragmentEndFollowingLengthGoalFromPreceding = 0;
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

  // called by getSummaryPassagesNoHighlight to generate default summary.
  @Override
  public int next() {
    this.fragmentEndFromPreceding = current();
    this.fragmentEndFollowingLengthGoalFromPreceding = lengthGoal;
    return following(this.fragmentEndFromPreceding);
  }

  @Override
  public int previous() {
    assert false : "Not supported";
    return baseIter.previous();
  }

  // NOTE: this.fragmentEndFromPreceding is used instead of the parameter!
  // This is a big diversion from the API a BreakIterator should implement, but specifically this optimization is fine.
  @Override
  public int following(int _unused_followingIdx) {
    if (fragmentEndFollowingLengthGoalFromPreceding <= 0) {
      return fragmentEndFromPreceding;
    }
    final int targetIdx = fragmentEndFromPreceding + fragmentEndFollowingLengthGoalFromPreceding;
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
    if (targetIdx - beforeIdx < afterIdx - targetIdx) {
      return beforeIdx;
    }
    // moveToBreak is necessary for when getSummaryPassagesNoHighlight calls next and current() is used
    return moveToBreak(afterIdx);
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
  public int preceding(int offset) {
    final int fragmentStart = Math.max(baseIter.preceding(offset), 0); // convert DONE to 0
    fragmentEndFromPreceding = baseIter.following(fragmentStart);
    if (fragmentEndFromPreceding == DONE) {
      fragmentEndFromPreceding = baseIter.last();
    }
    final int centerLength = fragmentEndFromPreceding - fragmentStart;
    final int extraPrecedingLengthGoal = (int)((lengthGoal - centerLength) * fragmentAlignment);
    if (extraPrecedingLengthGoal <= 0) {
      // With uneven alignment like 0.1 the preceding portion could be 0 because of rounding, while the following
      // could be a small positive value. This means to favor extra text after the match if not negative.
      fragmentEndFollowingLengthGoalFromPreceding = lengthGoal - centerLength;
      return fragmentStart;
    }
    final int targetIdx = fragmentStart - extraPrecedingLengthGoal;
    if (targetIdx <= 0) {
      fragmentEndFollowingLengthGoalFromPreceding = lengthGoal - fragmentEndFromPreceding;
      return 0;
    }
    final int beforeIdx = baseIter.preceding(targetIdx);
    if (beforeIdx == DONE) {
      fragmentEndFollowingLengthGoalFromPreceding = lengthGoal - fragmentEndFromPreceding;
      return 0;
    }
    if (beforeIdx == targetIdx) { // right on the money
      fragmentEndFollowingLengthGoalFromPreceding = (lengthGoal - fragmentEndFromPreceding) + beforeIdx;
      return beforeIdx;
    }
    if (isMinimumLength) { // thus never undershoot
      fragmentEndFollowingLengthGoalFromPreceding = (lengthGoal - fragmentEndFromPreceding) + beforeIdx;
      return beforeIdx;
    }

    // note: it is a shame that we invoke following() *one more time*; BI's are sometimes expensive.

    // Find closest break to target
    final int afterIdx = baseIter.following(targetIdx - 1);
    if (afterIdx - targetIdx < targetIdx - beforeIdx) {
      fragmentEndFollowingLengthGoalFromPreceding = (lengthGoal - fragmentEndFromPreceding) + afterIdx;
      return afterIdx;
    }
    fragmentEndFollowingLengthGoalFromPreceding = (lengthGoal - fragmentEndFromPreceding) + beforeIdx;
    return beforeIdx;
  }

  @Override
  public boolean isBoundary(int offset) {
    assert false : "Not supported";
    return baseIter.isBoundary(offset);
  }
}
