package org.apache.lucene.search.positions;

import org.apache.lucene.search.positions.PositionIntervalIterator.PositionInterval;

final class IntervalQueueAnd extends IntervalQueue {

  int rightExtreme = Integer.MIN_VALUE;
  
  public IntervalQueueAnd(int size) {
    super(size);
  }

  public void reset () {
    super.reset();
    queueInterval.begin = Integer.MIN_VALUE;
    queueInterval.end = Integer.MIN_VALUE;
    rightExtreme = Integer.MIN_VALUE;
  }

  public void updateRightExtreme(PositionInterval interval) {
    rightExtreme = Math.max(rightExtreme, Math.max(interval.end, interval.end));
  }
  
  public boolean topContainsQueueInterval() {
    PositionInterval interval = top().interval;
    return interval.begin <= queueInterval.begin
        && queueInterval.end <= rightExtreme;
  }

  public void updateQueueInterval() {
    PositionInterval interval = top().interval;
    queueInterval.begin = interval.begin;
    queueInterval.end = rightExtreme;
  }
  
  @Override
  protected boolean lessThan(IntervalRef left, IntervalRef right) {
    final PositionInterval a = left.interval;
    final PositionInterval b = right.interval;
    return a.begin < b.begin || (a.begin == b.begin && a.end >= b.end);
  }

}
