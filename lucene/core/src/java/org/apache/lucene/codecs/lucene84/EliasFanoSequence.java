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
package org.apache.lucene.codecs.lucene84;

final class EliasFanoSequence {

  final long[] upperBits = new long[ForUtil.BLOCK_SIZE >>> 5];
  final long[] lowerBits = new long[ForUtil.BLOCK_SIZE];
  int lowBitsPerValue;
  int upperIndex;
  int i;

  // Like FixedBitSet#nextSetBit
  private int nextUpperSetBit(int index) {
    int i = index >>> 6;
    long word = upperBits[i] >>> index;
    if (word != 0) {
      return index + Long.numberOfTrailingZeros(word);
    } else {
      while (++i < upperBits.length) {
        word = upperBits[i];
        if (word != 0) {
          return (i << 6) + Long.numberOfTrailingZeros(word);
        }
      }
    }
    return ForUtil.BLOCK_SIZE << 1;
  }

  long next() {
    ++i;
    upperIndex = nextUpperSetBit(upperIndex+1);
    long highBits = upperIndex - i;
    long lowBits = lowerBits[i];
    return (highBits << lowBitsPerValue) | lowBits;
  }

  /*private void advanceToHighTarget(long highTarget) {
    int upperLongIndex = (upperIndex + 1) >>> 6;
    long word = upperBits[upperLongIndex] >>> upperIndex;

    if (word != 0) {
      int popCount = Long.bitCount(word);
      long maxHighBits = (1 << (upperLongIndex + 1)) - 1 - i - popCount;
      if (maxHighBits >= highTarget) {
        // The looked up high target might be within the current long, stop here
        // TODO: rank selection
        ++i;
        upperIndex += 1 + Long.numberOfTrailingZeros(word);
        return;
      }
      i += popCount;
    }

    while (++upperLongIndex < upperBits.length) {
      word = upperBits[upperLongIndex];
      if (word != 0) {
        int popCount = Long.bitCount(word);
        long maxHighBits = (1 << (upperLongIndex + 1)) - 1 - i - popCount;
        if (maxHighBits >= highTarget) {
          // TODO: rank selection
          ++i;
          upperIndex = (upperLongIndex << 6) + Long.numberOfTrailingZeros(word);
        }
        i += popCount;
      }
    }
  }

  long advance(long target) {
    final long highTarget = target >>> lowBitsPerValue;
    advanceToHighTarget(highTarget);
    long highBits = upperIndex - i;
    while (highBits < highTarget) {
      ++i;
      upperIndex = nextUpperSetBit(upperIndex+1);
      highBits = upperIndex - i;
    }
    long lowBits = lowerBits[i];
    return (highBits << lowBitsPerValue) | lowBits;
  }*/

  long maybeAdvance(long target) {
    final long highTarget = target >>> lowBitsPerValue;
    final int minUpperIndex = (int) (highTarget + i);
    final int nextUpperIndex = upperIndex + 1;
    final int nextWordIndex = nextUpperIndex >>> 6;
    final int targetWordIndex = Math.min(minUpperIndex >>> 6, 4);
    if (targetWordIndex > nextWordIndex) {
      i += Long.bitCount(upperBits[nextUpperIndex] >>> nextUpperIndex);
      for (int j = nextWordIndex + 1; j < targetWordIndex; ++j) {
        i += Long.bitCount(upperBits[j]);
      }
      upperIndex = (targetWordIndex << 6) - 1;
    }
    return next();
  }

}
