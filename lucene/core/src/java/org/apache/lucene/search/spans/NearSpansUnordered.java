package org.apache.lucene.search.spans;

/**
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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.TermContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 * Similar to {@link NearSpansOrdered}, but for the unordered case.
 * 
 * Expert:
 * Only public for subclassing.  Most implementations should not need this class
 */
public class NearSpansUnordered extends Spans {
  private SpanNearQuery query;

  private List<SpansCell> ordered = new ArrayList<SpansCell>();         // spans in query order
  private Spans[] subSpans;  
  private int slop;                               // from query

  private SpansCell first;                        // linked list of spans
  private SpansCell last;                         // sorted by doc only

  private int totalLength;                        // sum of current lengths

  private CellQueue queue;                        // sorted queue of spans
  private SpansCell max;                          // max element in queue

  private boolean more = true;                    // true iff not done
  private boolean firstTime = true;               // true before first next()

  private class CellQueue extends PriorityQueue<SpansCell> {
    public CellQueue(int size) {
      super(size);
    }
    
    @Override
    protected final boolean lessThan(SpansCell spans1, SpansCell spans2) {
      if (spans1.doc() == spans2.doc()) {
        return NearSpansOrdered.docSpansOrdered(spans1, spans2);
      } else {
        return spans1.doc() < spans2.doc();
      }
    }
  }


  /** Wraps a Spans, and can be used to form a linked list. */
  private class SpansCell extends Spans {
    private Spans spans;
    private SpansCell next;
    private int length = -1;
    private int index;

    public SpansCell(Spans spans, int index) {
      this.spans = spans;
      this.index = index;
    }

    @Override
    public boolean next() throws IOException {
      return adjust(spans.next());
    }

    @Override
    public boolean skipTo(int target) throws IOException {
      return adjust(spans.skipTo(target));
    }
    
    private boolean adjust(boolean condition) {
      if (length != -1) {
        totalLength -= length;  // subtract old length
      }
      if (condition) {
        length = end() - start(); 
        totalLength += length; // add new length

        if (max == null || doc() > max.doc()
            || (doc() == max.doc()) && (end() > max.end())) {
          max = this;
        }
      }
      more = condition;
      return condition;
    }

    @Override
    public int doc() { return spans.doc(); }
    
    @Override
    public int start() { return spans.start(); }
    
    @Override
    public int end() { return spans.end(); }
                    // TODO: Remove warning after API has been finalized
    @Override
    public Collection<byte[]> getPayload() throws IOException {
      return new ArrayList<byte[]>(spans.getPayload());
    }

    // TODO: Remove warning after API has been finalized
    @Override
    public boolean isPayloadAvailable() {
      return spans.isPayloadAvailable();
    }

    @Override
    public String toString() { return spans.toString() + "#" + index; }
  }


  public NearSpansUnordered(SpanNearQuery query, AtomicReaderContext context, Bits acceptDocs, Map<Term,TermContext> termContexts)
    throws IOException {
    this.query = query;
    this.slop = query.getSlop();

    SpanQuery[] clauses = query.getClauses();
    queue = new CellQueue(clauses.length);
    subSpans = new Spans[clauses.length];    
    for (int i = 0; i < clauses.length; i++) {
      SpansCell cell =
        new SpansCell(clauses[i].getSpans(context, acceptDocs, termContexts), i);
      ordered.add(cell);
      subSpans[i] = cell.spans;
    }
  }
  public Spans[] getSubSpans() {
	  return subSpans;
  }
  @Override
  public boolean next() throws IOException {
    if (firstTime) {
      initList(true);
      listToQueue(); // initialize queue
      firstTime = false;
    } else if (more) {
      if (min().next()) { // trigger further scanning
        queue.updateTop(); // maintain queue
      } else {
        more = false;
      }
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

      if (atMatch()) {
        return true;
      }
      
      more = min().next();
      if (more) {
        queue.updateTop();                      // maintain queue
      }
    }
    return false;                                 // no more matches
  }

  @Override
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
        if (min().skipTo(target)) {
          queue.updateTop();
        } else {
          more = false;
        }
      }
    }
    return more && (atMatch() ||  next());
  }

  private SpansCell min() { return queue.top(); }

  @Override
  public int doc() { return min().doc(); }
  @Override
  public int start() { return min().start(); }
  @Override
  public int end() { return max.end(); }

  // TODO: Remove warning after API has been finalized
  /**
   * WARNING: The List is not necessarily in order of the the positions
   * @return Collection of <code>byte[]</code> payloads
   * @throws IOException
   */
  @Override
  public Collection<byte[]> getPayload() throws IOException {
    Set<byte[]> matchPayload = new HashSet<byte[]>();
    for (SpansCell cell = first; cell != null; cell = cell.next) {
      if (cell.isPayloadAvailable()) {
        matchPayload.addAll(cell.getPayload());
      }
    }
    return matchPayload;
  }

  // TODO: Remove warning after API has been finalized
  @Override
  public boolean isPayloadAvailable() {
    SpansCell pointer = min();
    while (pointer != null) {
      if (pointer.isPayloadAvailable()) {
        return true;
      }
      pointer = pointer.next;
    }

    return false;
  }

  @Override
  public String toString() {
    return getClass().getName() + "("+query.toString()+")@"+
      (firstTime?"START":(more?(doc()+":"+start()+"-"+end()):"END"));
  }

  private void initList(boolean next) throws IOException {
    for (int i = 0; more && i < ordered.size(); i++) {
      SpansCell cell = ordered.get(i);
      if (next)
        more = cell.next();                       // move to first entry
      if (more) {
        addToList(cell);                          // add to list
      }
    }
  }

  private void addToList(SpansCell cell) throws IOException {
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

  private void queueToList() throws IOException {
    last = first = null;
    while (queue.top() != null) {
      addToList(queue.pop());
    }
  }
  
  private void listToQueue() {
    queue.clear(); // rebuild queue
    for (SpansCell cell = first; cell != null; cell = cell.next) {
      queue.add(cell);                      // add to queue from list
    }
  }

  private boolean atMatch() {
    return (min().doc() == max.doc())
        && ((max.end() - min().start() - totalLength) <= slop);
  }
}
