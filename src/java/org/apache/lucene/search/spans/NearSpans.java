package org.apache.lucene.search.spans;

/**
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.PriorityQueue;

class NearSpans implements Spans {
  private SpanNearQuery query;

  private List ordered = new ArrayList();         // spans in query order
  private int slop;                               // from query
  private boolean inOrder;                        // from query

  private SpansCell first;                        // linked list of spans
  private SpansCell last;                         // sorted by doc only

  private int totalLength;                        // sum of current lengths

  private CellQueue queue;                        // sorted queue of spans
  private SpansCell max;                          // max element in queue

  private boolean more = true;                    // true iff not done
  private boolean firstTime = true;               // true before first next()

  private class CellQueue extends PriorityQueue {
    public CellQueue(int size) {
      initialize(size);
    }
    
    protected final boolean lessThan(Object o1, Object o2) {
      SpansCell spans1 = (SpansCell)o1;
      SpansCell spans2 = (SpansCell)o2;
      if (spans1.doc() == spans2.doc()) {
        if (spans1.start() == spans2.start()) {
          if (spans1.end() == spans2.end()) {
            return spans1.index > spans2.index;
          } else {
            return spans1.end() < spans2.end();
          }
        } else {
          return spans1.start() < spans2.start();
        }
      } else {
        return spans1.doc() < spans2.doc();
      }
    }
  }


  /** Wraps a Spans, and can be used to form a linked list. */
  private class SpansCell implements Spans {
    private Spans spans;
    private SpansCell next;
    private int length = -1;
    private int index;

    public SpansCell(Spans spans, int index) {
      this.spans = spans;
      this.index = index;
    }

    public boolean next() throws IOException {
      if (length != -1)                           // subtract old length
        totalLength -= length;

      boolean more = spans.next();                // move to next

      if (more) {
        length = end() - start();                 // compute new length
        totalLength += length;                    // add new length to total

        if (max == null || doc() > max.doc() ||   // maintain max
            (doc() == max.doc() && end() > max.end()))
          max = this;
      }

      return more;
    }

    public boolean skipTo(int target) throws IOException {
      if (length != -1)                           // subtract old length
        totalLength -= length;

      boolean more = spans.skipTo(target);        // skip

      if (more) {
        length = end() - start();                 // compute new length
        totalLength += length;                    // add new length to total

        if (max == null || doc() > max.doc() ||   // maintain max
            (doc() == max.doc() && end() > max.end()))
          max = this;
      }

      return more;
    }

    public int doc() { return spans.doc(); }
    public int start() { return spans.start(); }
    public int end() { return spans.end(); }

    public String toString() { return spans.toString() + "#" + index; }
  }

  public NearSpans(SpanNearQuery query, IndexReader reader)
    throws IOException {
    this.query = query;
    this.slop = query.getSlop();
    this.inOrder = query.isInOrder();

    SpanQuery[] clauses = query.getClauses();     // initialize spans & list
    queue = new CellQueue(clauses.length);
    for (int i = 0; i < clauses.length; i++) {
      SpansCell cell =                            // construct clause spans
        new SpansCell(clauses[i].getSpans(reader), i);
      ordered.add(cell);                          // add to ordered
    }
  }

  public boolean next() throws IOException {
    if (firstTime) {
      initList(true);
      listToQueue();                              // initialize queue
      firstTime = false;
    } else if (more) {
      more = min().next();                        // trigger further scanning
      if (more)
        queue.adjustTop();                        // maintain queue
    }

    while (more) {

      boolean queueStale = false;

      if (min().doc() != max.doc()) {             // maintain list
        queueToList();
        queueStale = true;
      }

      // skip to doc w/ all clauses

      while (more && first.doc() < last.doc()) {
        more = first.skipTo(last.doc());          // skip first upto last
        firstToLast();                            // and move it to the end
        queueStale = true;
      }

      if (!more) return false;

      // found doc w/ all clauses

      if (queueStale) {                           // maintain the queue
        listToQueue();
        queueStale = false;
      }

      if (atMatch())
        return true;

      more = min().next();                        // trigger further scanning
      if (more)
        queue.adjustTop();                        // maintain queue
    }
    return false;                                 // no more matches
  }

  public boolean skipTo(int target) throws IOException {
    if (firstTime) {                              // initialize
      initList(false);
      for (SpansCell cell = first; more && cell!=null; cell=cell.next) {
        more = cell.skipTo(target);               // skip all
      }
      if (more) {
        listToQueue();
      }
      firstTime = false;
    } else {                                      // normal case
      while (more && min().doc() < target) {      // skip as needed
        more = min().skipTo(target);
        if (more)
          queue.adjustTop();
      }
    }
    if (more) {

      if (atMatch())                              // at a match?
        return true;

      return next();                              // no, scan
    }

    return false;
  }

  private SpansCell min() { return (SpansCell)queue.top(); }

  public int doc() { return min().doc(); }
  public int start() { return min().start(); }
  public int end() { return max.end(); }


  public String toString() {
    return "spans("+query.toString()+")@"+
      (firstTime?"START":(more?(doc()+":"+start()+"-"+end()):"END"));
  }

  private void initList(boolean next) throws IOException {
    for (int i = 0; more && i < ordered.size(); i++) {
      SpansCell cell = (SpansCell)ordered.get(i);
      if (next)
        more = cell.next();                       // move to first entry
      if (more) {
        addToList(cell);                          // add to list
      }
    }
  }

  private void addToList(SpansCell cell) {
    if (last != null) {			  // add next to end of list
      last.next = cell;
    } else
      first = cell;
    last = cell;
    cell.next = null;
  }

  private void firstToLast() {
    last.next = first;			  // move first to end of list
    last = first;
    first = first.next;
    last.next = null;
  }

  private void queueToList() {
    last = first = null;
    while (queue.top() != null) {
      addToList((SpansCell)queue.pop());
    }
  }

  private void listToQueue() {
    queue.clear();
    for (SpansCell cell = first; cell != null; cell = cell.next) {
      queue.put(cell);                      // build queue from list
    }
  }

  private boolean atMatch() {
    if (min().doc() == max.doc()) {               // at a match?
      int matchLength = max.end() - min().start();
      if (((matchLength - totalLength) <= slop)   // check slop
          && (!inOrder || matchIsOrdered())) {    // check order
        return true;
      }
    }
    return false;
  }

  private boolean matchIsOrdered() {
    int lastStart = -1;
    for (int i = 0; i < ordered.size(); i++) {
      int start = ((SpansCell)ordered.get(i)).start();
      if (!(start > lastStart))
        return false;
      lastStart = start;
    }
    return true;
  }

}
