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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.Scorer;

import java.io.IOException;


/**
 * Iterates over the individual positions of a term in a document
 */
public final class TermIntervalIterator extends IntervalIterator {

  private final Interval interval;
  int positionsPending;
  private final DocsAndPositionsEnum docsAndPos;
  private int docID = -1;

  /**
   * Constructs a new TermIntervalIterator
   * @param scorer the parent Scorer
   * @param docsAndPos a DocsAndPositionsEnum positioned on the current document
   * @param doPayloads true if payloads should be retrieved for the positions
   * @param collectIntervals true if positions will be collected
   */
  public TermIntervalIterator(Scorer scorer, DocsAndPositionsEnum docsAndPos,
                              boolean doPayloads, boolean collectIntervals) {
    super(scorer, collectIntervals);
    this.docsAndPos = docsAndPos;
    this.interval = new Interval();
  }

  @Override
  public Interval next() throws IOException {
    if (--positionsPending >= 0) {
      interval.begin = interval.end = docsAndPos.nextPosition();
      interval.offsetBegin = docsAndPos.startOffset();
      interval.offsetEnd = docsAndPos.endOffset();
      return interval;
    }
    positionsPending = 0;
    return null;
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public IntervalIterator[] subs(boolean inOrder) {
    return EMPTY;
  }

  @Override
  public void collect(IntervalCollector collector) {
    collector.collectLeafPosition(scorer, interval, docID);
  }

  @Override
  public int scorerAdvanced(int docId) throws IOException {
    interval.reset();
    if (docsAndPos.docID() == docId) {
      positionsPending = docsAndPos.freq();
    } else {
      positionsPending = -1;
    }
    return docID = docsAndPos.docID();
  }
  
  @Override
  public String toString() {
    return "TermPositions [interval=" + interval + ", positionsPending="
        + positionsPending + ", docID=" + docID + "]";
  }

  @Override
  public int matchDistance() {
    return 0;
  }
// TODO not supported yet - need to figure out what that means really to support payloads
//  private static final class PayloadInterval extends Interval {
//    private int pos = -1;
//    private final DocsAndPositionsEnum payloads;
//    private final TermIntervalIterator termPos;
//
//    public PayloadInterval(DocsAndPositionsEnum payloads, TermIntervalIterator pos) {
//      this.payloads = payloads;
//      this.termPos = pos;
//    }
//
//    @Override
//    public BytesRef nextPayload() throws IOException {
//      if (pos == termPos.positionsPending) {
//        return null;
//      } else {
//        pos = termPos.positionsPending;
//        return payloads.getPayload();
//      }
//    }
//
//    @Override
//    public void reset() {
//      super.reset();
//      pos = -1;
//    }
//
//  }
}