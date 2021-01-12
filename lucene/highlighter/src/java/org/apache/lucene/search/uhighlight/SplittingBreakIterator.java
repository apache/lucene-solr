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
import java.text.StringCharacterIterator;

/**
 * Virtually slices the text on both sides of every occurrence of the specified character. If the
 * slice is 0-length which happens for adjacent slice characters or when they are at the beginning
 * or end, that character is reported as a boundary. For every slice between the specified
 * characters, it is further processed with a specified BreakIterator. A consequence is that the
 * enclosed BreakIterator will never "see" the splitting character. <br>
 * <em>Note: {@link #setText(CharacterIterator)} is unsupported. Use the string version.</em>
 *
 * @lucene.experimental
 */
public class SplittingBreakIterator extends BreakIterator {
  private final BreakIterator baseIter;
  private final char sliceChar;

  private String text;
  private int sliceStartIdx;
  private int sliceEndIdx;
  private int current;

  public SplittingBreakIterator(BreakIterator baseIter, char sliceChar) {
    this.baseIter = baseIter;
    this.sliceChar = sliceChar;
  }

  @Override
  public void setText(CharacterIterator newText) {
    throw new UnsupportedOperationException("unexpected");
  }

  @Override
  public void setText(String newText) {
    this.text = newText;
    first();
  }

  @Override
  public CharacterIterator getText() {
    StringCharacterIterator charIter = new StringCharacterIterator(text);
    // API doesn't say what the state should be but it should probably be at the current index.
    charIter.setIndex(current());
    return charIter;
  }

  @Override
  public int current() {
    assert current != DONE;
    return current; // MUST be updated by the other methods when result isn't DONE.
  }

  @Override
  public int first() {
    sliceStartIdx = 0;
    sliceEndIdx = text.indexOf(sliceChar);
    if (sliceEndIdx == -1) {
      sliceEndIdx = text.length();
    }
    if (sliceStartIdx == sliceEndIdx) {
      return current = sliceStartIdx;
    }
    baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
    // since setText() sets to first(), just grab current()
    return current = sliceStartIdx + baseIter.current();
  }

  @Override
  public int last() {
    sliceEndIdx = text.length();
    sliceStartIdx = text.lastIndexOf(sliceChar);
    if (sliceStartIdx == -1) {
      sliceStartIdx = 0;
    } else {
      sliceStartIdx++; // past sliceChar
    }
    if (sliceEndIdx == sliceStartIdx) {
      return current = sliceEndIdx;
    }
    baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
    return current = sliceStartIdx + baseIter.last();
  }

  @Override
  public int next() {
    int prevCurrent = current;
    current = sliceStartIdx == sliceEndIdx ? DONE : baseIter.next();
    if (current != DONE) {
      return current = current + sliceStartIdx;
    }
    if (sliceEndIdx >= text.length()) {
      current = prevCurrent; // keep current where it is
      return DONE;
    }
    sliceStartIdx = sliceEndIdx + 1;
    sliceEndIdx = text.indexOf(sliceChar, sliceStartIdx);
    if (sliceEndIdx == -1) {
      sliceEndIdx = text.length();
    }
    if (sliceStartIdx == sliceEndIdx) {
      return current = sliceStartIdx;
    }
    baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
    return current = sliceStartIdx + baseIter.current(); // use current() since at first() already
  }

  @Override
  public int previous() { // note: closely follows next() but reversed
    int prevCurrent = current;
    current = sliceStartIdx == sliceEndIdx ? DONE : baseIter.previous();
    if (current != DONE) {
      return current = current + sliceStartIdx;
    }
    if (sliceStartIdx == 0) {
      current = prevCurrent; // keep current where it is
      return DONE;
    }
    sliceEndIdx = sliceStartIdx - 1;
    sliceStartIdx = text.lastIndexOf(sliceChar, sliceEndIdx - 1);
    if (sliceStartIdx == -1) {
      sliceStartIdx = 0;
    } else {
      sliceStartIdx++; // past sliceChar
    }
    if (sliceStartIdx == sliceEndIdx) {
      return current = sliceStartIdx;
    }
    baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
    return current = sliceStartIdx + baseIter.last();
  }

  @Override
  public int following(int offset) {
    // if the offset is not in this slice, update the slice
    if (offset + 1 < sliceStartIdx || offset + 1 > sliceEndIdx) {
      if (offset == text.length()) { // DONE condition
        last(); // because https://bugs.openjdk.java.net/browse/JDK-8015110
        return DONE;
      }
      sliceStartIdx = text.lastIndexOf(sliceChar, offset); // no +1
      if (sliceStartIdx == -1) {
        sliceStartIdx = 0;
      } else {
        sliceStartIdx++; // move past separator
      }
      sliceEndIdx = text.indexOf(sliceChar, Math.max(offset + 1, sliceStartIdx));
      if (sliceEndIdx == -1) {
        sliceEndIdx = text.length();
      }
      if (sliceStartIdx != sliceEndIdx) { // otherwise, adjacent separator or separator at end
        baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
      }
    }

    // lookup following() in this slice:
    if (sliceStartIdx == sliceEndIdx) {
      return current = offset + 1;
    } else {
      // note: following() can never be first() if the first character is a boundary (it usually
      // is).
      //   So we have to check if we should call first() instead of following():
      if (offset == sliceStartIdx - 1) {
        // the first boundary following this offset is the very first boundary in this slice
        return current = sliceStartIdx + baseIter.first();
      } else {
        return current = sliceStartIdx + baseIter.following(offset - sliceStartIdx);
      }
    }
  }

  @Override
  public int preceding(int offset) { // note: closely follows following() but reversed
    if (offset - 1 < sliceStartIdx || offset - 1 > sliceEndIdx) {
      if (offset == 0) { // DONE condition
        first(); // because https://bugs.openjdk.java.net/browse/JDK-8015110
        return DONE;
      }
      sliceEndIdx = text.indexOf(sliceChar, offset); // no -1
      if (sliceEndIdx == -1) {
        sliceEndIdx = text.length();
      }
      sliceStartIdx = text.lastIndexOf(sliceChar, offset - 1);
      if (sliceStartIdx == -1) {
        sliceStartIdx = 0;
      } else {
        sliceStartIdx = Math.min(sliceStartIdx + 1, sliceEndIdx);
      }
      if (sliceStartIdx != sliceEndIdx) { // otherwise, adjacent separator or separator at end
        baseIter.setText(text.substring(sliceStartIdx, sliceEndIdx));
      }
    }
    // lookup preceding() in this slice:
    if (sliceStartIdx == sliceEndIdx) {
      return current = offset - 1;
    } else {
      // note: preceding() can never be last() if the last character is a boundary (it usually is).
      //   So we have to check if we should call last() instead of preceding():
      if (offset == sliceEndIdx + 1) {
        // the last boundary preceding this offset is the very last boundary in this slice
        return current = sliceStartIdx + baseIter.last();
      } else {
        return current = sliceStartIdx + baseIter.preceding(offset - sliceStartIdx);
      }
    }
  }

  @Override
  public int next(int n) {
    if (n < 0) {
      for (int i = 0; i < -n; i++) {
        if (previous() == DONE) {
          return DONE;
        }
      }
    } else {
      for (int i = 0; i < n; i++) {
        if (next() == DONE) {
          return DONE;
        }
      }
    }
    return current();
  }
}
