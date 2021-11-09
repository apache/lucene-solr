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

package org.apache.lucene.search;

import java.util.concurrent.atomic.LongAccumulator;

/**
 * Maintains the maximum score and its corresponding document id concurrently
 */
final class MaxScoreAccumulator {
  // we use 2^10-1 to check the remainder with a bitwise operation
  static final int DEFAULT_INTERVAL = 0x3ff;

  // scores are always positive
  final LongAccumulator acc = new LongAccumulator(MaxScoreAccumulator::maxEncode, Long.MIN_VALUE);

  // non-final and visible for tests
  long modInterval;

  MaxScoreAccumulator() {
    this.modInterval = DEFAULT_INTERVAL;
  }

  /**
   * Return the max encoded DocAndScore in a way that is consistent with {@link
   * DocAndScore#compareTo}.
   */
  private static long maxEncode(long v1, long v2) {
    float score1 = Float.intBitsToFloat((int) (v1 >> 32));
    float score2 = Float.intBitsToFloat((int) (v2 >> 32));
    int cmp = Float.compare(score1, score2);
    if (cmp == 0) {
      // tie-break on the minimum doc base
      return (int) v1 < (int) v2 ? v1 : v2;
    } else if (cmp > 0) {
      return v1;
    }
    return v2;
  }

  void accumulate(int docBase, float score) {
    assert docBase >= 0 && score >= 0;
    long encode = (((long) Float.floatToIntBits(score)) << 32) | docBase;
    acc.accumulate(encode);
  }

  DocAndScore get() {
    long value = acc.get();
    if (value == Long.MIN_VALUE) {
      return null;
    }
    float score = Float.intBitsToFloat((int) (value >> 32));
    int docBase = (int) value;
    return new DocAndScore(docBase, score);
  }

  static class DocAndScore implements Comparable<DocAndScore> {
    final int docBase;
    final float score;

    DocAndScore(int docBase, float score) {
      this.docBase = docBase;
      this.score = score;
    }

    @Override
    public int compareTo(DocAndScore o) {
      int cmp = Float.compare(score, o.score);
      if (cmp == 0) {
        // tie-break on the minimum doc base
        // For a given minimum competitive score, we want to know the first segment
        // where this score occurred, hence the reverse order here.
        // On segments with a lower docBase, any document whose score is greater
        // than or equal to this score would be competitive, while on segments with a
        // higher docBase, documents need to have a strictly greater score to be
        // competitive since we tie break on doc ID.
        return Integer.compare(o.docBase, docBase);
      }
      return cmp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DocAndScore result = (DocAndScore) o;
      return docBase == result.docBase &&
          Float.compare(result.score, score) == 0;
    }

    @Override
    public String toString() {
      return "DocAndScore{" +
          "docBase=" + docBase +
          ", score=" + score +
          '}';
    }
  }
}