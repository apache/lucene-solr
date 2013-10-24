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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.intervals.IntervalIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Expert: Common scoring functionality for different types of queries.
 *
 * <p>
 * A <code>Scorer</code> iterates over documents matching a
 * query in increasing order of doc Id.
 * </p>
 * <p>
 * Document scores are computed using a given <code>Similarity</code>
 * implementation.
 * </p>
 *
 * <p><b>NOTE</b>: The values Float.Nan,
 * Float.NEGATIVE_INFINITY and Float.POSITIVE_INFINITY are
 * not valid scores.  Certain collectors (eg {@link
 * TopScoreDocCollector}) will not properly collect hits
 * with these scores.
 */
public abstract class Scorer extends DocsEnum {
  /** the Scorer's parent Weight. in some cases this may be null */
  // TODO can we clean this up?
  protected final Weight weight;

  /**
   * Constructs a Scorer
   * @param weight The scorers <code>Weight</code>.
   */
  protected Scorer(Weight weight) {
    this.weight = weight;
  }

  /** Scores and collects all matching documents.
   * @param collector The collector to which all matching documents are passed.
   */
  public void score(Collector collector) throws IOException {
    assert docID() == -1; // not started
    collector.setScorer(this);
    int doc;
    while ((doc = nextDoc()) != NO_MORE_DOCS) {
      collector.collect(doc);
    }
  }
  
  /**
   * Expert: Retrieves an {@link IntervalIterator} for this scorer allowing
   * access to position and offset intervals for each
   * matching document.  Call this up-front and use it as
   * long as you are still using this scorer.  The
   * returned iterator is bound to scorer that created it;
   * after {@link #nextDoc} or {@link #advance} you must
   * call {@link IntervalIterator#scorerAdvanced} before
   * iterating over that document's intervals.
   * 
   * @param collectIntervals
   *          if <code>true</code> the {@link IntervalIterator} can be used to
   *          collect all individual sub-intervals this {@link IntervalIterator}
   *          is composed of via
   *          {@link IntervalIterator#collect(org.apache.lucene.search.intervals.IntervalCollector)}
   * @return an {@link IntervalIterator} over matching intervals
   * @throws IOException
   *           if a low-level I/O error is encountered
   *
   * @lucene.experimental
   */
  public IntervalIterator intervals(boolean collectIntervals) throws IOException {
    return null;
  };

  /**
   * Get the IntervalIterators from a list of scorers
   * @param collectIntervals true if positions will be collected
   * @param scorers the list of scorers to retrieve IntervalIterators from
   * @return a list of IntervalIterators pulled from the passed in Scorers
   * @throws java.io.IOException if a low-evel I/O error is encountered
   */
  public static IntervalIterator[] pullIterators(boolean collectIntervals, Scorer... scorers)
      throws IOException {
    IntervalIterator[] iterators = new IntervalIterator[scorers.length];
    for (int i = 0; i < scorers.length; i++) {
      if (scorers[i] == null) {
        iterators[i] = IntervalIterator.NO_MORE_INTERVALS;
      }
      else {
        iterators[i] = scorers[i].intervals(collectIntervals);
      }
    }
    return iterators;
  }

  /**
   * Expert: Collects matching documents in a range. Hook for optimization.
   * Note, <code>firstDocID</code> is added to ensure that {@link #nextDoc()}
   * was called before this method.
   * 
   * @param collector
   *          The collector to which all matching documents are passed.
   * @param max
   *          Do not score documents past this.
   * @param firstDocID
   *          The first document ID (ensures {@link #nextDoc()} is called before
   *          this method.
   * @return true if more matching documents may remain.
   */
  public boolean score(Collector collector, int max, int firstDocID) throws IOException {
    assert docID() == firstDocID;
    collector.setScorer(this);
    int doc;
    for (doc = firstDocID; doc < max; doc = nextDoc()) {
      collector.collect(doc);
    }
    return doc != NO_MORE_DOCS;
  }
  
  /** Returns the score of the current document matching the query.
   * Initially invalid, until {@link #nextDoc()} or {@link #advance(int)}
   * is called the first time, or when called from within
   * {@link Collector#collect}.
   */
  public abstract float score() throws IOException;
  
  /** returns parent Weight
   * @lucene.experimental
   */
  public Weight getWeight() {
    return weight;
  }

  @Override
  public int nextPosition() throws IOException {
    throw new UnsupportedOperationException("nextPosition() is not implemented on " + this.getClass());
  }

  @Override
  public String toString() {
    try {
      return String.format("%d:%d(%d)->%d(%d)", docID(), startPosition(), startOffset(), endPosition(), endOffset());
    } catch (IOException e) {
      return String.format("Cannot retrieve position due to IOException");
    }
  }
  
  /** Returns child sub-scorers
   * @lucene.experimental */
  public Collection<ChildScorer> getChildren() {
    return Collections.emptyList();
  }
  
  /** A child Scorer and its relationship to its parent.
   * the meaning of the relationship depends upon the parent query. 
   * @lucene.experimental */
  public static class ChildScorer {
    /**
     * Child Scorer. (note this is typically a direct child, and may
     * itself also have children).
     */
    public final Scorer child;
    /**
     * An arbitrary string relating this scorer to the parent.
     */
    public final String relationship;
    
    /**
     * Creates a new ChildScorer node with the specified relationship.
     * <p>
     * The relationship can be any be any string that makes sense to 
     * the parent Scorer. 
     */
    public ChildScorer(Scorer child, String relationship) {
      this.child = child;
      this.relationship = relationship;
    }
  }
}
