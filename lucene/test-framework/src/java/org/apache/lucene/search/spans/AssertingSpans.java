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
package org.apache.lucene.search.spans;

import java.io.IOException;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;

/** 
 * Wraps a Spans with additional asserts
 */
class AssertingSpans extends Spans {
  final Spans in;
  int doc = -1;
  
  /** 
   * tracks current state of this spans
   */
  static enum State { 
    /**
     * document iteration has not yet begun ({@link #docID()} = -1) 
     */
    DOC_START,
    
    /**
     * two-phase iterator has moved to a new docid, but {@link TwoPhaseIterator#matches()} has
     * not been called or it returned false (so you should not do things with the enum)
     */
    DOC_UNVERIFIED,
    
    /**
     * iterator set to a valid docID, but position iteration has not yet begun ({@link #startPosition() == -1})
     */
    POS_START,
    
    /**
     * iterator set to a valid docID, and positioned (-1 < {@link #startPosition()} < {@link #NO_MORE_POSITIONS})
     */
    ITERATING,
    
    /**
     * positions exhausted ({@link #startPosition()} = {@link #NO_MORE_POSITIONS})
     */
    POS_FINISHED,
    
    /** 
     * documents exhausted ({@link #docID()} = {@link #NO_MORE_DOCS}) 
     */
    DOC_FINISHED 
  };
  
  State state = State.DOC_START;
  
  AssertingSpans(Spans in) {
    this.in = in;
  }
  
  @Override
  public int nextStartPosition() throws IOException {
    assert state != State.DOC_START : "invalid position access, state=" + state + ": " + in;
    assert state != State.DOC_FINISHED : "invalid position access, state=" + state + ": " + in;
    assert state != State.DOC_UNVERIFIED : "invalid position access, state=" + state + ": " + in;
    
    checkCurrentPositions();
    
    // move to next position
    int prev = in.startPosition();
    int start = in.nextStartPosition();
    assert start >= prev : "invalid startPosition (positions went backwards, previous=" + prev + "): " + in;
    
    // transition state if necessary
    if (start == NO_MORE_POSITIONS) {
      state = State.POS_FINISHED;
    } else {
      state = State.ITERATING;
    }
    
    // check new positions
    checkCurrentPositions();
    return start;
  }
  
  private void checkCurrentPositions() {    
    int start = in.startPosition();
    int end = in.endPosition();
    
    if (state == State.DOC_START || state == State.DOC_UNVERIFIED || state == State.POS_START) {
      assert start == -1 : "invalid startPosition (should be -1): " + in;
      assert end == -1 : "invalid endPosition (should be -1): " + in;
    } else if (state == State.POS_FINISHED) {
      assert start == NO_MORE_POSITIONS : "invalid startPosition (should be NO_MORE_POSITIONS): " + in;
      assert end == NO_MORE_POSITIONS : "invalid endPosition (should be NO_MORE_POSITIONS): " + in;
    } else {
      assert start >= 0 : "invalid startPosition (negative): " + in;
      assert start <= end : "invalid startPosition (> endPosition): " + in;
    }    
  }
  
  @Override
  public int startPosition() {
    checkCurrentPositions();
    return in.startPosition();
  }
  
  @Override
  public int endPosition() {
    checkCurrentPositions();
    return in.endPosition();
  }

  @Override
  public int width() {
    assert state == State.ITERATING;
    final int distance = in.width();
    assert distance >= 0;
    return distance;
  }

  @Override
  public void collect(SpanCollector collector) throws IOException {
    assert state == State.ITERATING : "collect() called in illegal state: " + state + ": " + in;
    in.collect(collector);
  }

  @Override
  public int docID() {
    int doc = in.docID();
    assert doc == this.doc : "broken docID() impl: docID() = " + doc + ", but next/advance last returned: " + this.doc + ": " + in;
    return doc;
  }
  
  @Override
  public int nextDoc() throws IOException {
    assert state != State.DOC_FINISHED : "nextDoc() called after NO_MORE_DOCS: " + in;
    int nextDoc = in.nextDoc();
    assert nextDoc > doc : "backwards nextDoc from " + doc + " to " + nextDoc + ": " + in;
    if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
      state = State.DOC_FINISHED;
    } else {
      assert in.startPosition() == -1 : "invalid initial startPosition() [should be -1]: " + in;
      assert in.endPosition() == -1 : "invalid initial endPosition() [should be -1]: " + in;
      state = State.POS_START;
    }
    doc = nextDoc;
    return docID();
  }
  
  @Override
  public int advance(int target) throws IOException {
    assert state != State.DOC_FINISHED : "advance() called after NO_MORE_DOCS: " + in;
    assert target > doc : "target must be > docID(), got " + target + " <= " + doc + ": " + in;
    int advanced = in.advance(target);
    assert advanced >= target : "backwards advance from: " + target + " to: " + advanced + ": " + in;
    if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
      state = State.DOC_FINISHED;
    } else {
      assert in.startPosition() == -1 : "invalid initial startPosition() [should be -1]: " + in;
      assert in.endPosition() == -1 : "invalid initial endPosition() [should be -1]: " + in;
      state = State.POS_START;
    }
    doc = advanced;
    return docID();
  }
  
  @Override
  public String toString() {
    return "Asserting(" + in + ")";
  }

  @Override
  public long cost() {
    return in.cost();
  }

  @Override
  public float positionsCost() {
    float cost = in.positionsCost();
    assert ! Float.isNaN(cost) : "positionsCost() should not be NaN";
    assert cost > 0 : "positionsCost() must be positive";
    return cost;
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    final TwoPhaseIterator iterator = in.asTwoPhaseIterator();
    if (iterator == null) {
      return null;
    }
    return new AssertingTwoPhaseView(iterator);
  }

  class AssertingTwoPhaseView extends TwoPhaseIterator {
    final TwoPhaseIterator in;
    int lastDoc = -1;
    
    AssertingTwoPhaseView(TwoPhaseIterator iterator) {
      super(new AssertingDISI(iterator.approximation()));
      this.in = iterator;
    }
    
    @Override
    public boolean matches() throws IOException {
      if (approximation.docID() == -1 || approximation.docID() == DocIdSetIterator.NO_MORE_DOCS) {
        throw new AssertionError("matches() should not be called on doc ID " + approximation.docID());
      }
      if (lastDoc == approximation.docID()) {
        throw new AssertionError("matches() has been called twice on doc ID " + approximation.docID());
      }
      lastDoc = approximation.docID();
      boolean v = in.matches();
      if (v) {
        state = State.POS_START;
      }
      return v;
    }

    @Override
    public float matchCost() {
      float cost = in.matchCost();
      if (Float.isNaN(cost)) {
        throw new AssertionError("matchCost()=" + cost + " should not be NaN on doc ID " + approximation.docID());
      }
      if (cost < 0) {
        throw new AssertionError("matchCost()=" + cost + " should be non negative on doc ID " + approximation.docID());
      }
      return cost;
    }
  }
  
  class AssertingDISI extends DocIdSetIterator {
    final DocIdSetIterator in;
    
    AssertingDISI(DocIdSetIterator in) {
      this.in = in;
    }
    
    @Override
    public int docID() {
      assert in.docID() == AssertingSpans.this.docID();
      return in.docID();
    }
    
    @Override
    public int nextDoc() throws IOException {
      assert state != State.DOC_FINISHED : "nextDoc() called after NO_MORE_DOCS: " + in;
      int nextDoc = in.nextDoc();
      assert nextDoc > doc : "backwards nextDoc from " + doc + " to " + nextDoc + ": " + in;
      if (nextDoc == DocIdSetIterator.NO_MORE_DOCS) {
        state = State.DOC_FINISHED;
      } else {
        state = State.DOC_UNVERIFIED;
      }
      doc = nextDoc;
      return docID();
    }
    
    @Override
    public int advance(int target) throws IOException {
      assert state != State.DOC_FINISHED : "advance() called after NO_MORE_DOCS: " + in;
      assert target > doc : "target must be > docID(), got " + target + " <= " + doc + ": " + in;
      int advanced = in.advance(target);
      assert advanced >= target : "backwards advance from: " + target + " to: " + advanced + ": " + in;
      if (advanced == DocIdSetIterator.NO_MORE_DOCS) {
        state = State.DOC_FINISHED;
      } else {
        state = State.DOC_UNVERIFIED;
      }
      doc = advanced;
      return docID();
    }
    
    @Override
    public long cost() {
      return in.cost();
    }
  }
}
