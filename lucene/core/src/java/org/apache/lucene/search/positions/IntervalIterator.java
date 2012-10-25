package org.apache.lucene.search.positions;

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
import org.apache.lucene.search.Scorer;

import java.io.IOException;

/**
 * Iterator over the matching {@link Interval}s of a {@link Scorer}
 *
 * @lucene.experimental
 */
public abstract class IntervalIterator {

  /** An empty array of IntervalIterators */
  public static final IntervalIterator[] EMPTY = new IntervalIterator[0];

  /** An IntervalIterator containing no further Intervals */
  public static final IntervalIterator NO_MORE_POSITIONS = new EmptyIntervalIterator();

  /** Integer representing no more documents */
  public static final int NO_MORE_DOCS = Integer.MAX_VALUE;

  protected final Scorer scorer;
  protected final boolean collectPositions;

  /**
   * Constructs an IntervalIterator over a {@link Scorer}
   * @param scorer the {@link Scorer} to pull positions from
   * @param collectPositions true if positions will be collected
   */
  public IntervalIterator(Scorer scorer, boolean collectPositions) {
    this.scorer = scorer;
    this.collectPositions = collectPositions;
  }

  /**
   * Called after the parent scorer has been advanced.  If the scorer is
   * currently positioned on docId, then subsequent calls to next() will
   * return Intervals for that document; otherwise, no Intervals are
   * available
   * @param docId the document the parent scorer was advanced to
   * @return the docId that the scorer is currently positioned at
   * @throws IOException if a low-level I/O error is encountered
   */
  public abstract int scorerAdvanced(int docId) throws IOException;

  /**
   * Get the next Interval on the current document.
   * @return the next Interval, or null if there are no remaining Intervals
   * @throws IOException if a low-level I/O error is encountered
   */
  public abstract Interval next() throws IOException;

  /**
   * If positions are to be collected, this will be called once
   * for each Interval returned by the iterator.  The constructor
   * must have been called with collectPositions=true.
   * @param collector an {@link IntervalCollector} to collect the
   *                  Interval positions
   */
  public abstract void collect(IntervalCollector collector);

  /**
   * Get any subiterators
   * @param inOrder true if the subiterators should be returned in order
   * @return
   */
  public abstract IntervalIterator[] subs(boolean inOrder);

  /**
   * Get the distance between matching subintervals
   */
  public abstract int matchDistance();

  /**
   * Get the current docID
   */
  public int docID() {
    return scorer.docID();
  }

  /**
   * Get this iterator's {@link Scorer}
   * @return
   */
  public Scorer getScorer() {
    return scorer;
  }

  /**
   * An iterator that is always exhausted
   */
  private static final class EmptyIntervalIterator extends
      IntervalIterator {
    
    public EmptyIntervalIterator() {
      super(null, false);
    }
    
    @Override
    public int scorerAdvanced(int docId) throws IOException {
      return IntervalIterator.NO_MORE_DOCS;
    }
    
    @Override
    public Interval next() throws IOException {
      return null;
    }
    
    @Override
    public void collect(IntervalCollector collectoc) {}
    
    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return EMPTY;
    }

    @Override
    public int matchDistance() {
      return Integer.MAX_VALUE;
    }
    
  }
  
}
