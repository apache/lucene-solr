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
  final LongAccumulator acc = new LongAccumulator(Long::max, Long.MIN_VALUE);

  // non-final and visible for tests
  long modInterval;

  MaxScoreAccumulator() {
    this.modInterval = DEFAULT_INTERVAL;
  }

  void accumulate(int docID, float score) {
    assert docID >= 0 && score >= 0;
    long encode = (((long) Float.floatToIntBits(score)) << 32) | docID;
    acc.accumulate(encode);
  }

  DocAndScore get() {
    long value = acc.get();
    if (value == Long.MIN_VALUE) {
      return null;
    }
    float score = Float.intBitsToFloat((int) (value >> 32));
    int docID = (int) value;
    return new DocAndScore(docID, score);
  }

  static class DocAndScore implements Comparable<DocAndScore> {
    final int docID;
    final float score;

    DocAndScore(int docID, float score) {
      this.docID = docID;
      this.score = score;
    }

    @Override
    public int compareTo(DocAndScore o) {
      int cmp = Float.compare(score, o.score);
      if (cmp == 0) {
        return Integer.compare(docID, o.docID);
      }
      return cmp;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      DocAndScore result = (DocAndScore) o;
      return docID == result.docID &&
          Float.compare(result.score, score) == 0;
    }

    @Override
    public String toString() {
      return "DocAndScore{" +
          "docID=" + docID +
          ", score=" + score +
          '}';
    }
  }
}