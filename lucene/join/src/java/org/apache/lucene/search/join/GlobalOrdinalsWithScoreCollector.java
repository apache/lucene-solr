package org.apache.lucene.search.join;

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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;

import java.io.IOException;

abstract class GlobalOrdinalsWithScoreCollector implements Collector {

  final String field;
  final MultiDocValues.OrdinalMap ordinalMap;
  final LongBitSet collectedOrds;
  protected final Scores scores;

  GlobalOrdinalsWithScoreCollector(String field, MultiDocValues.OrdinalMap ordinalMap, long valueCount) {
    if (valueCount > Integer.MAX_VALUE) {
      // We simply don't support more than
      throw new IllegalStateException("Can't collect more than [" + Integer.MAX_VALUE + "] ids");
    }
    this.field = field;
    this.ordinalMap = ordinalMap;
    this.collectedOrds = new LongBitSet(valueCount);
    this.scores = new Scores(valueCount);
  }

  public LongBitSet getCollectorOrdinals() {
    return collectedOrds;
  }

  public float score(int globalOrdinal) {
    return scores.getScore(globalOrdinal);
  }

  protected abstract void doScore(int globalOrd, float existingScore, float newScore);

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    SortedDocValues docTermOrds = DocValues.getSorted(context.reader(), field);
    if (ordinalMap != null) {
      LongValues segmentOrdToGlobalOrdLookup = ordinalMap.getGlobalOrds(context.ord);
      return new OrdinalMapCollector(docTermOrds, segmentOrdToGlobalOrdLookup);
    } else {
      return new SegmentOrdinalCollector(docTermOrds);
    }
  }

  @Override
  public boolean needsScores() {
    return true;
  }

  final class OrdinalMapCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;
    private final LongValues segmentOrdToGlobalOrdLookup;
    private Scorer scorer;

    OrdinalMapCollector(SortedDocValues docTermOrds, LongValues segmentOrdToGlobalOrdLookup) {
      this.docTermOrds = docTermOrds;
      this.segmentOrdToGlobalOrdLookup = segmentOrdToGlobalOrdLookup;
    }

    @Override
    public void collect(int doc) throws IOException {
      final long segmentOrd = docTermOrds.getOrd(doc);
      if (segmentOrd != -1) {
        final int globalOrd = (int) segmentOrdToGlobalOrdLookup.get(segmentOrd);
        collectedOrds.set(globalOrd);
        float existingScore = scores.getScore(globalOrd);
        float newScore = scorer.score();
        doScore(globalOrd, existingScore, newScore);
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  final class SegmentOrdinalCollector implements LeafCollector {

    private final SortedDocValues docTermOrds;
    private Scorer scorer;

    SegmentOrdinalCollector(SortedDocValues docTermOrds) {
      this.docTermOrds = docTermOrds;
    }

    @Override
    public void collect(int doc) throws IOException {
      final int segmentOrd = docTermOrds.getOrd(doc);
      if (segmentOrd != -1) {
        collectedOrds.set(segmentOrd);
        float existingScore = scores.getScore(segmentOrd);
        float newScore = scorer.score();
        doScore(segmentOrd, existingScore, newScore);
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  static final class Max extends GlobalOrdinalsWithScoreCollector {

    public Max(String field, MultiDocValues.OrdinalMap ordinalMap, long valueCount) {
      super(field, ordinalMap, valueCount);
    }

    @Override
    protected void doScore(int globalOrd, float existingScore, float newScore) {
      scores.setScore(globalOrd, Math.max(existingScore, newScore));
    }

  }

  static final class Sum extends GlobalOrdinalsWithScoreCollector {

    public Sum(String field, MultiDocValues.OrdinalMap ordinalMap, long valueCount) {
      super(field, ordinalMap, valueCount);
    }

    @Override
    protected void doScore(int globalOrd, float existingScore, float newScore) {
      scores.setScore(globalOrd, existingScore + newScore);
    }

  }

  static final class Avg extends GlobalOrdinalsWithScoreCollector {

    private final Occurrences occurrences;

    public Avg(String field, MultiDocValues.OrdinalMap ordinalMap, long valueCount) {
      super(field, ordinalMap, valueCount);
      this.occurrences = new Occurrences(valueCount);
    }

    @Override
    protected void doScore(int globalOrd, float existingScore, float newScore) {
      occurrences.increment(globalOrd);
      scores.setScore(globalOrd, existingScore + newScore);
    }

    @Override
    public float score(int globalOrdinal) {
      return scores.getScore(globalOrdinal) / occurrences.getOccurrence(globalOrdinal);
    }
  }

  // Because the global ordinal is directly used as a key to a score we should be somewhat smart about allocation
  // the scores array. Most of the times not all docs match so splitting the scores array up in blocks can prevent creation of huge arrays.
  // Also working with smaller arrays is supposed to be more gc friendly
  //
  // At first a hash map implementation would make sense, but in the case that more than half of docs match this becomes more expensive
  // then just using an array.

  // Maybe this should become a method parameter?
  static final int arraySize = 4096;

  static final class Scores {

    final float[][] blocks;

    private Scores(long valueCount) {
      long blockSize = valueCount + arraySize - 1;
      blocks = new float[(int) ((blockSize) / arraySize)][];
    }

    public void setScore(int globalOrdinal, float score) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      float[] scores = blocks[block];
      if (scores == null) {
        blocks[block] = scores = new float[arraySize];
      }
      scores[offset] = score;
    }

    public float getScore(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      float[] scores = blocks[block];
      float score;
      if (scores != null) {
        score = scores[offset];
      } else {
        score =  0f;
      }
      return score;
    }

  }

  static final class Occurrences {

    final int[][] blocks;

    private Occurrences(long valueCount) {
      long blockSize = valueCount + arraySize - 1;
      blocks = new int[(int) (blockSize / arraySize)][];
    }

    public void increment(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      int[] occurrences = blocks[block];
      if (occurrences == null) {
        blocks[block] = occurrences = new int[arraySize];
      }
      occurrences[offset]++;
    }

    public int getOccurrence(int globalOrdinal) {
      int block = globalOrdinal / arraySize;
      int offset = globalOrdinal % arraySize;
      int[] occurrences = blocks[block];
      return occurrences[offset];
    }

  }

}
