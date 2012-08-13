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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;


/**
 * 
 */
//nocommmit javadocs
public final class TermIntervalIterator extends IntervalIterator {
  private final Interval interval;
  int positionsPending;
  private final DocsAndPositionsEnum docsAndPos;
  private int docID = -1;

  public TermIntervalIterator(Scorer scorer, DocsAndPositionsEnum docsAndPos, boolean doPayloads,  boolean collectPositions) {
    super(scorer, collectPositions);
    this.docsAndPos = docsAndPos;
    this.interval = doPayloads ? new PayloadInterval(docsAndPos, this)
        : new Interval();
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
  
  private static final class PayloadInterval extends Interval {
    private int pos = -1;
    private final DocsAndPositionsEnum payloads;
    private final TermIntervalIterator termPos;

    public PayloadInterval(DocsAndPositionsEnum payloads, TermIntervalIterator pos) {
      this.payloads = payloads;
      this.termPos = pos;
    }

    @Override
    public boolean payloadAvailable() {
      return payloads.hasPayload();
    }

    @Override
    public boolean nextPayload(BytesRef ref) throws IOException {
      if (pos == termPos.positionsPending) {
        return false;
      } else {
        pos = termPos.positionsPending;
        final BytesRef payload = payloads.getPayload();
        ref.bytes = payload.bytes;
        ref.length = payload.length;
        ref.offset = payload.offset;
        return true;
      }
    }

    @Override
    public void reset() {
      super.reset();
      pos = -1;
    }

  }
}