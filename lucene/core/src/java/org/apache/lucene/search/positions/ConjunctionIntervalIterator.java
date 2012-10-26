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
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;

import java.io.IOException;

/**
 * ConjuctionPositionIterator based on minimal interval semantics for AND
 * operator.
 * 
 * <a href=
 * "http://vigna.dsi.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics"
 * >"Efficient Optimally Lazy Algorithms for Minimal-Interval Semantic</a>
 * 
 * @lucene.experimental
 */
public final class ConjunctionIntervalIterator extends IntervalIterator {

  private final IntervalQueueAnd queue;
  private final int nrMustMatch;
  private SnapshotPositionCollector snapshot;
  private final IntervalIterator[] iterators;
  private int rightExtremeBegin;
  

  public ConjunctionIntervalIterator(Scorer scorer, boolean collectPositions,
      IntervalIterator... iterators) throws IOException {
    this(scorer, collectPositions, iterators.length, iterators);
  }
  
  public ConjunctionIntervalIterator(Scorer scorer, boolean collectPositions,
      int minimuNumShouldMatch, IntervalIterator... iterators)
      throws IOException {
    super(scorer, collectPositions);
    this.iterators = iterators;
    this.queue = new IntervalQueueAnd(iterators.length);
    this.nrMustMatch = minimuNumShouldMatch;
  }
  
  private void advance() throws IOException {
    final IntervalRef top = queue.top();
    Interval interval = null;
    if ((interval = iterators[top.index].next()) != null) {
      top.interval = interval;
      queue.updateRightExtreme(top);
      queue.updateTop();
    } else {
      queue.pop();
    }
  }
  
  @Override
  public Interval next() throws IOException {
    
    while (queue.size() >= nrMustMatch
        && queue.top().interval.begin == queue.currentCandidate.begin) {
      advance();
    }
    if (queue.size() < nrMustMatch) {
      return null;
    }
    do {
      queue.updateCurrentCandidate();
      Interval top = queue.top().interval; 
      if (collectPositions) {
        snapShotSubPositions(); // this looks odd? -> see SnapShotCollector below for
                                // details!
      }
      if (queue.currentCandidate.begin == top.begin
          && queue.currentCandidate.end == top.end) {
        return queue.currentCandidate;
      }
      rightExtremeBegin = queue.rightExtremeBegin;
      advance();
    } while (queue.size() >= nrMustMatch && queue.currentCandidate.end == queue.rightExtreme);
    return queue.currentCandidate; // TODO support payloads
  }
  
  
  @Override
  public int scorerAdvanced(final int docId) throws IOException {
    if (docId == NO_MORE_DOCS) {
      return NO_MORE_DOCS;
    }
    queue.reset();
    for (int i = 0; i < iterators.length; i++) {
      int scorerAdvanced = iterators[i].scorerAdvanced(docId);
      if (scorerAdvanced != docId) {
        System.out.println();  // nocommit!
      }
      assert scorerAdvanced == docId;
      final Interval interval = iterators[i].next();
      if (interval != null) {
        IntervalRef intervalRef = new IntervalRef(interval, i); // TODO maybe
                                                                // reuse?
        queue.updateRightExtreme(intervalRef);
        queue.add(intervalRef);
      }
    }
    return docId;
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return iterators;
  }
  
  
  private void snapShotSubPositions() {
    if (snapshot == null) {
      snapshot = new SnapshotPositionCollector(queue.size());
    }
    snapshot.reset();
    collectInternal(snapshot);
  }
  
  private void collectInternal(IntervalCollector collector) {
    assert collectPositions;
    collector.collectComposite(scorer, queue.currentCandidate, docID());
    for (IntervalIterator iter : iterators) {
      iter.collect(collector);
    }
    
  }
  
  @Override
  public void collect(IntervalCollector collector) {
    assert collectPositions;
    if (snapshot == null) {
      // we might not be initialized if the first interval matches
      collectInternal(collector);
    } else {
      snapshot.replay(collector);
    }
  }

  @Override
  public int matchDistance() {
    return (rightExtremeBegin) - (queue.currentTopEnd) -1; // align the match if pos are adjacent
  }
}