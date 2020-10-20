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



import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;

/**
 * Expert: a FieldComparator compares hits so as to determine their
 * sort order when collecting the top results with {@link
 * TopFieldCollector}.  The concrete public FieldComparator
 * classes here correspond to the SortField types.
 *
 * <p>The document IDs passed to these methods must only
 * move forwards, since they are using doc values iterators
 * to retrieve sort values.</p>
 *
 * <p>This API is designed to achieve high performance
 * sorting, by exposing a tight interaction with {@link
 * FieldValueHitQueue} as it visits hits.  Whenever a hit is
 * competitive, it's enrolled into a virtual slot, which is
 * an int ranging from 0 to numHits-1. Segment transitions are
 * handled by creating a dedicated per-segment
 * {@link LeafFieldComparator} which also needs to interact
 * with the {@link FieldValueHitQueue} but can optimize based
 * on the segment to collect.</p>
 * 
 * <p>The following functions need to be implemented</p>
 * <ul>
 *  <li> {@link #compare} Compare a hit at 'slot a'
 *       with hit 'slot b'.
 * 
 *  <li> {@link #setTopValue} This method is called by
 *       {@link TopFieldCollector} to notify the
 *       FieldComparator of the top most value, which is
 *       used by future calls to
 *       {@link LeafFieldComparator#compareTop}.
 * 
 *  <li> {@link #getLeafComparator(org.apache.lucene.index.LeafReaderContext)} Invoked
 *       when the search is switching to the next segment.
 *       You may need to update internal state of the
 *       comparator, for example retrieving new values from
 *       DocValues.
 *
 *  <li> {@link #value} Return the sort value stored in
 *       the specified slot.  This is only called at the end
 *       of the search, in order to populate {@link
 *       FieldDoc#fields} when returning the top results.
 * </ul>
 *
 * @see LeafFieldComparator
 * @lucene.experimental
 */
public abstract class FieldComparator<T> {

  /**
   * Compare hit at slot1 with hit at slot2.
   * 
   * @param slot1 first slot to compare
   * @param slot2 second slot to compare
   * @return any {@code N < 0} if slot2's value is sorted after
   * slot1, any {@code N > 0} if the slot2's value is sorted before
   * slot1 and {@code 0} if they are equal
   */
  public abstract int compare(int slot1, int slot2);

  /**
   * Record the top value, for future calls to {@link
   * LeafFieldComparator#compareTop}.  This is only called for searches that
   * use searchAfter (deep paging), and is called before any
   * calls to {@link #getLeafComparator(LeafReaderContext)}.
   */
  public abstract void setTopValue(T value);

  /**
   * Return the actual value in the slot.
   *
   * @param slot the value
   * @return value in this slot
   */
  public abstract T value(int slot);

  /**
   * Get a per-segment {@link LeafFieldComparator} to collect the given
   * {@link org.apache.lucene.index.LeafReaderContext}. All docIDs supplied to
   * this {@link LeafFieldComparator} are relative to the current reader (you
   * must add docBase if you need to map it to a top-level docID).
   * 
   * @param context current reader context
   * @return the comparator to use for this segment
   * @throws IOException if there is a low-level IO error
   */
  public abstract LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException;

  /** Returns a negative integer if first is less than second,
   *  0 if they are equal and a positive integer otherwise. Default
   *  impl to assume the type implements Comparable and
   *  invoke .compareTo; be sure to override this method if
   *  your FieldComparator's type isn't a Comparable or
   *  if your values may sometimes be null */
  @SuppressWarnings("unchecked")
  public int compareValues(T first, T second) {
    if (first == null) {
      if (second == null) {
        return 0;
      } else {
        return -1;
      }
    } else if (second == null) {
      return 1;
    } else {
      return ((Comparable<T>) first).compareTo(second);
    }
  }

  /**
   * Informs the comparator that sort is done on this single field.
   * This is useful to enable some optimizations for skipping non-competitive documents.
   */
  public void setSingleSort() {
  }

  /** Sorts by descending relevance.  NOTE: if you are
   *  sorting only by descending relevance and then
   *  secondarily by ascending docID, performance is faster
   *  using {@link TopScoreDocCollector} directly (which {@link
   *  IndexSearcher#search} uses when no {@link Sort} is
   *  specified). */
  public static final class RelevanceComparator extends FieldComparator<Float> implements LeafFieldComparator {
    private final float[] scores;
    private float bottom;
    private Scorable scorer;
    private float topValue;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public RelevanceComparator(int numHits) {
      scores = new float[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(scores[slot2], scores[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      float score = scorer.score();
      assert !Float.isNaN(score);
      return Float.compare(score, bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      scores[slot] = scorer.score();
      assert !Float.isNaN(scores[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = scores[bottom];
    }

    @Override
    public void setTopValue(Float value) {
      topValue = value;
    }

    @Override
    public void setScorer(Scorable scorer) {
      // wrap with a ScoreCachingWrappingScorer so that successive calls to
      // score() will not incur score computation over and
      // over again.
      if (!(scorer instanceof ScoreCachingWrappingScorer)) {
        this.scorer = new ScoreCachingWrappingScorer(scorer);
      } else {
        this.scorer = scorer;
      }
    }
    
    @Override
    public Float value(int slot) {
      return Float.valueOf(scores[slot]);
    }

    // Override because we sort reverse of natural Float order:
    @Override
    public int compareValues(Float first, Float second) {
      // Reversed intentionally because relevance by default
      // sorts descending:
      return second.compareTo(first);
    }

    @Override
    public int compareTop(int doc) throws IOException {
      float docValue = scorer.score();
      assert !Float.isNaN(docValue);
      return Float.compare(docValue, topValue);
    }
  }
}
