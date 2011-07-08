package org.apache.lucene.search.positions;

import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;

final class IntervalQueueOr extends IntervalQueue {
  
  public IntervalQueueOr(int size) {
    super(size);
  }
  
  public void reset() {
    clear();
  }
  
  public boolean topContainsQueueInterval() {
    PositionInterval interval = top().interval;
    return interval.begin <= queueInterval.begin
        && queueInterval.end <= interval.end;
  }

  public void updateQueueInterval() {
    PositionInterval interval = top().interval;
    queueInterval.begin = interval.begin;
    queueInterval.end = interval.end;
  }
  
  @Override
  protected boolean lessThan(IntervalRef left, IntervalRef right) {
    final PositionInterval a = left.interval;
    final PositionInterval b = right.interval;
    return a.end < b.end|| (a.end == b.end && a.begin >= b.begin);
  }
}
