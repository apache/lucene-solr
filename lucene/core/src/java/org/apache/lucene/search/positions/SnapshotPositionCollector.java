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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An IntervalCollector that allows a snapshot of the state of an
 * IntervalIterator to be taken before it is advanced.
 *
 * Conjunction iterators advance their subiterators before the consumer
 * can call collect on the top level iterator.  If intervals are to be
 * collected, we need to record the last possible match so that we can
 * return the correct intervals for the match.
 *
 * @lucene.internal
 */
final class SnapshotPositionCollector implements IntervalCollector {

  private SingleSnapshot[] snapshots;
  private int index = 0;

  /**
   * Create a new collector with n snapshots
   * @param subs the number of subiterators to record
   */
  SnapshotPositionCollector(int subs) {
    snapshots = new SingleSnapshot[subs];
  }

  @Override
  public void collectLeafPosition(Scorer scorer, Interval interval,
      int docID) {
    collect(scorer, interval, docID, true);

  }

  private void collect(Scorer scorer, Interval interval, int docID,
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
  public void collectComposite(Scorer scorer, Interval interval,
      int docID) {
    collect(scorer, interval, docID, false);
  }

  void replay(IntervalCollector collector) {
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
    final Interval interval = new Interval();
    boolean isLeaf;
    int docID;

    void set(Scorer scorer, Interval interval, boolean isLeaf,
        int docID) {
      this.scorer = scorer;
      this.interval.copy(interval);
      this.isLeaf = isLeaf;
      this.docID = docID;
    }
  }

}
