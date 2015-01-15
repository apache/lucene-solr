package org.apache.lucene.search;

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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.search.BooleanQuery.BooleanWeight;

/**
 * BulkSorer that is used for pure disjunctions: no MUST clauses and
 * minShouldMatch == 1. This scorer scores documents by batches of 2048 docs.
 */
final class BooleanScorer extends BulkScorer {

  static final int SHIFT = 11;
  static final int SIZE = 1 << SHIFT;
  static final int MASK = SIZE - 1;
  static final int SET_SIZE = 1 << (SHIFT - 6);
  static final int SET_MASK = SET_SIZE - 1;

  static class Bucket {
    double score;
    int freq;
  }

  final Bucket[] buckets = new Bucket[SIZE];
  // This is basically an inlined FixedBitSet... seems to help with bound checks
  final long[] matching = new long[SET_SIZE];

  final float[] coordFactors;
  final BulkScorer[] optionalScorers;
  final FakeScorer fakeScorer = new FakeScorer();

  boolean hasMatches;
  int max = 0;

  final class OrCollector implements LeafCollector {
    Scorer scorer;

    @Override
    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      hasMatches = true;
      final int i = doc & MASK;
      final int idx = i >>> 6;
      matching[idx] |= 1L << i;
      final Bucket bucket = buckets[i];
      bucket.freq++;
      bucket.score += scorer.score();
    }
  }

  final OrCollector orCollector = new OrCollector();

  BooleanScorer(BooleanWeight weight, boolean disableCoord, int maxCoord, Collection<BulkScorer> optionalScorers) {
    for (int i = 0; i < buckets.length; i++) {
      buckets[i] = new Bucket();
    }
    this.optionalScorers = optionalScorers.toArray(new BulkScorer[0]);

    coordFactors = new float[optionalScorers.size() + 1];
    for (int i = 0; i < coordFactors.length; i++) {
      coordFactors[i] = disableCoord ? 1.0f : weight.coord(i, maxCoord);
    }
  }

  private void scoreDocument(LeafCollector collector, int base, int i) throws IOException {
    final Bucket bucket = buckets[i];
    fakeScorer.freq = bucket.freq;
    fakeScorer.score = (float) bucket.score * coordFactors[bucket.freq];
    final int doc = base | i;
    fakeScorer.doc = doc;
    collector.collect(doc);
    bucket.freq = 0;
    bucket.score = 0;
  }

  private void scoreMatches(LeafCollector collector, int base) throws IOException {
    long matching[] = this.matching;
    for (int idx = 0; idx < matching.length; idx++) {
      long bits = matching[idx];
      while (bits != 0L) {
        int ntz = Long.numberOfTrailingZeros(bits);
        int doc = idx << 6 | ntz;
        scoreDocument(collector, base, doc);
        bits ^= 1L << ntz;
      }
    }
  }

  private boolean collectMatches() throws IOException {
    boolean more = false;
    for (BulkScorer scorer : optionalScorers) {
      more |= scorer.score(orCollector, max);
    }
    return more;
  }

  private boolean scoreWindow(LeafCollector collector, int base, int max) throws IOException {
    this.max = Math.min(base + SIZE, max);
    hasMatches = false;
    boolean more = collectMatches();

    if (hasMatches) {
      scoreMatches(collector, base);
      Arrays.fill(matching, 0L);
    }

    return more;
  }

  @Override
  public boolean score(LeafCollector collector, int max) throws IOException {
    fakeScorer.doc = -1;
    collector.setScorer(fakeScorer);

    for (int docBase = this.max & ~MASK; docBase < max; docBase += SIZE) {
      if (scoreWindow(collector, docBase, max) == false) {
        return false;
      }
    }

    return true;
  }
}
