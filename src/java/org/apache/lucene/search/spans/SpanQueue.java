package org.apache.lucene.search.spans;

import org.apache.lucene.util.PriorityQueue;

class SpanQueue extends PriorityQueue {
  public SpanQueue(int size) {
    initialize(size);
  }

  protected final boolean lessThan(Object o1, Object o2) {
    Spans spans1 = (Spans)o1;
    Spans spans2 = (Spans)o2;
    if (spans1.doc() == spans2.doc()) {
      if (spans1.start() == spans2.start()) {
        return spans1.end() < spans2.end();
      } else {
        return spans1.start() < spans2.start();
      }
    } else {
      return spans1.doc() < spans2.doc();
    }
  }
}
