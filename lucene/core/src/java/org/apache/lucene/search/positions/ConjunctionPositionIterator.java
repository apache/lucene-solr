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
import java.io.IOException;

import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

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
// nocommit - javadoc
public final class ConjunctionPositionIterator extends BooleanPositionIterator {
  private final IntervalQueueAnd queue;
  private final int nrMustMatch;
  private SnapshotPositionCollector snapshot;

  public ConjunctionPositionIterator(Scorer scorer, boolean collectPositions,
      PositionIntervalIterator... iterators) throws IOException {
    this(scorer, collectPositions, iterators.length, iterators);
  }
  
  public ConjunctionPositionIterator(Scorer scorer, boolean collectPositions,
      int minimuNumShouldMatch, PositionIntervalIterator... iterators)
      throws IOException {
    super(scorer, iterators, new IntervalQueueAnd(iterators.length),
        collectPositions);
    this.queue = (IntervalQueueAnd) super.queue; // avoid lots of casts?
    this.nrMustMatch = minimuNumShouldMatch;
  }
  
  void advance() throws IOException {
    final IntervalRef top = queue.top();
    PositionInterval interval = null;
    if ((interval = iterators[top.index].next()) != null) {
      top.interval = interval;
      queue.updateRightExtreme(interval);
      queue.updateTop();
    } else {
      queue.pop();
    }
  }
  
  @Override
  public PositionInterval next() throws IOException {
    
    while (queue.size() >= nrMustMatch
        && queue.top().interval.begin == queue.currentCandidate.begin) {
      advance();
    }
    if (queue.size() < nrMustMatch) {
      return null;
    }
    do {
      queue.updateCurrentCandidate();
      PositionInterval top = queue.top().interval; 
      if (collectPositions) {
        snapShotSubPositions(); // this looks odd? -> see SnapShotCollector below for
                                // details!
      }
      if (queue.currentCandidate.begin == top.begin
          && queue.currentCandidate.end == top.end) {
        return queue.currentCandidate;
      }
     
      advance();
      if (queue.size() < nrMustMatch) {
        break;
      }
    } while (queue.topContainsQueueInterval());
    return queue.currentCandidate; // TODO support payloads
  }
  
  
  @Override
  public int advanceTo(int docId) throws IOException {
    queue.reset();
    int advancedTo = -1;
    for (int i = 0; i < iterators.length; i++) {
      currentDoc = iterators[i].advanceTo(docId);
      assert advancedTo == -1 || advancedTo == currentDoc;
      
      final PositionInterval interval = iterators[i].next();
      if (interval != null) {
        IntervalRef intervalRef = new IntervalRef(interval, i); // TODO maybe reuse?
        queue.updateRightExtreme(intervalRef.interval);
        queue.add(intervalRef);
      }
    }
    return currentDoc;
  }
  
  
  private void snapShotSubPositions() {
    if (snapshot == null) {
      snapshot = new SnapshotPositionCollector(queue.size());
    }
    snapshot.reset();
    collectInternal(snapshot);
  }
  
  private void collectInternal(PositionCollector collector) {
    assert collectPositions;
    collector.collectComposite(scorer, queue.currentCandidate, currentDoc);
    for (PositionIntervalIterator iter : iterators) {
      iter.collect(collector);
    }
    
  }
  
  @Override
  public void collect(PositionCollector collector) {
    assert collectPositions;
    if (snapshot == null) {
      // we might not be initialized if the first interval matches
      collectInternal(collector);
    } else {
      snapshot.replay(collector);
    }
  }
  
  /*
   * Due to the laziness of this position iterator and the minimizing algorithm
   * we advance the underlying iterators before the consumer can call collect on
   * the top level iterator. If we need to collect positions we need to record
   * the last possible match in order to allow the consumer to get the right
   * positions for the match. This is particularly important if leaf positions
   * are required.
   */
  private static final class SnapshotPositionCollector implements
      PositionCollector {
    private SingleSnapshot[] snapshots;
    private int index = 0;
    
    SnapshotPositionCollector(int subs) {
      snapshots = new SingleSnapshot[subs];
    }
    
    @Override
    public void collectLeafPosition(Scorer scorer, PositionInterval interval,
        int docID) {
      collect(scorer, interval, docID, true);
      
    }
    
    private void collect(Scorer scorer, PositionInterval interval, int docID,
        boolean isLeaf) {
      if (snapshots.length <= index) {
        grow(ArrayUtil.oversize(index + 1,
            (RamUsageEstimator.NUM_BYTES_OBJECT_REF * 2)
                + RamUsageEstimator.NUM_BYTES_OBJECT_HEADER
                + RamUsageEstimator.NUM_BYTES_BOOLEAN
                + RamUsageEstimator.NUM_BYTES_INT));
      }
      if (snapshots[index] == null) {
        snapshots[index] = new SingleSnapshot();
      }
      snapshots[index++].set(scorer, interval, isLeaf, docID);
    }
    
    @Override
    public void collectComposite(Scorer scorer, PositionInterval interval,
        int docID) {
      collect(scorer, interval, docID, false);
    }
    
    void replay(PositionCollector collector) {
      for (int i = 0; i < index; i++) {
        SingleSnapshot singleSnapshot = snapshots[i];
        if (singleSnapshot.isLeaf) {
          collector.collectLeafPosition(singleSnapshot.scorer,
              singleSnapshot.interval, singleSnapshot.docID);
        } else {
          collector.collectComposite(singleSnapshot.scorer,
              singleSnapshot.interval, singleSnapshot.docID);
        }
      }
    }
    
    void reset() {
      index = 0;
    }
    
    private void grow(int size) {
      final SingleSnapshot[] newArray = new SingleSnapshot[size];
      System.arraycopy(snapshots, 0, newArray, 0, index);
      snapshots = newArray;
    }
    
    private static final class SingleSnapshot {
      Scorer scorer;
      final PositionInterval interval = new PositionInterval();
      boolean isLeaf;
      int docID;
      
      void set(Scorer scorer, PositionInterval interval, boolean isLeaf,
          int docID) {
        this.scorer = scorer;
        this.interval.copy(interval);
        this.isLeaf = isLeaf;
        this.docID = docID;
      }
    }
    
  }
}