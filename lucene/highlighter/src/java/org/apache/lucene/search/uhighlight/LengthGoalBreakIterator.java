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
  private final boolean isMinimumLength; // if false then is "closest to" length

  /** Breaks will be at least {@code minLength} apart (to the extent possible). */
  public static LengthGoalBreakIterator createMinLength(BreakIterator baseIter, int minLength) {
    return new LengthGoalBreakIterator(baseIter, minLength, true);
  }

  /** Breaks will be on average {@code targetLength} apart; the closest break to this target (before or after)
   * is chosen. */
  public static LengthGoalBreakIterator createClosestToLength(BreakIterator baseIter, int targetLength) {
    return new LengthGoalBreakIterator(baseIter, targetLength, false);
  }

  private LengthGoalBreakIterator(BreakIterator baseIter, int lengthGoal, boolean isMinimumLength) {
    this.baseIter = baseIter;
    this.lengthGoal = lengthGoal;
    this.isMinimumLength = isMinimumLength;
  }

  // note: the only methods that will get called are setText(txt), getText(),
  // getSummaryPassagesNoHighlight: current(), first(), next()
  // highlightOffsetsEnums: preceding(int), and following(int)
  //   Nonetheless we make some attempt to implement the rest; mostly delegating.

  @Override
  public String toString() {
    String goalDesc = isMinimumLength ? "minLen" : "targetLen";
    return getClass().getSimpleName() + "{" + goalDesc + "=" + lengthGoal + ", baseIter=" + baseIter + "}";
  }

  @Override
  public Object clone() {
    return new LengthGoalBreakIterator((BreakIterator) baseIter.clone(), lengthGoal, isMinimumLength);
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
    return following(current());
  }

  @Override
  public int previous() {
    assert false : "Not supported";
    return baseIter.previous();
  }

  // called while the current position is the start of a new passage; find end of passage
  @Override
  public int following(int followingIdx) {
    final int startIdx = current();
    if (followingIdx < startIdx) {
      assert false : "Not supported";
      return baseIter.following(followingIdx);
    }
    final int targetIdx = startIdx + lengthGoal;
    // When followingIdx >= targetIdx, we can simply delegate since it will be >= the target
    if (followingIdx >= targetIdx - 1) {
      return baseIter.following(followingIdx);
    }
    // If target exceeds the text length, return the last index.
    if (targetIdx >= getText().getEndIndex()) {
      return baseIter.last();
    }

    // Find closest break >= the target
    final int afterIdx = baseIter.following(targetIdx - 1);
    if (afterIdx == DONE) { // we're at the end; can this happen?
      return current();
    }
    if (afterIdx == targetIdx) { // right on the money
      return afterIdx;
    }
    if (isMinimumLength) { // thus never undershoot
      return afterIdx;
    }

    // note: it is a shame that we invoke preceding() *in addition to* following(); BI's are sometimes expensive.

    // Find closest break < target
    final int beforeIdx = baseIter.preceding(targetIdx); // or could do baseIter.previous() but we hope the BI implements preceding()
    if (beforeIdx <= followingIdx) { // too far back
      return moveToBreak(afterIdx);
    }

    if (targetIdx - beforeIdx <= afterIdx - targetIdx) {
      return beforeIdx;
    }
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
    return baseIter.preceding(offset); // no change needed
  }

  @Override
  public boolean isBoundary(int offset) {
    assert false : "Not supported";
    return baseIter.isBoundary(offset);
  }
}
