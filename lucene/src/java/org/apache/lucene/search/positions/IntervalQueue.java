package org.apache.lucene.search.positions;

import org.apache.lucene.search.positions.IntervalQueue.IntervalRef;
import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;
import org.apache.lucene.util.PriorityQueue;

abstract class IntervalQueue extends PriorityQueue<IntervalRef> {
  final PositionInterval queueInterval = new PositionInterval(
      Integer.MIN_VALUE, Integer.MIN_VALUE);

  public void reset() {
    clear();
  }

  abstract public boolean topContainsQueueInterval();

  abstract public void updateQueueInterval();

  public IntervalQueue(int size) {
    super(size);
  }

  final static class IntervalRef {
    PositionInterval interval;
    int index;

    IntervalRef(PositionInterval interval, int index) {
      super();
      this.interval = interval;
      this.index = index;
    }
  }
}