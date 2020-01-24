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
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.search.TermAutomatonQuery.EnumAndScorer;
import org.apache.lucene.search.TermAutomatonQuery.TermAutomatonWeight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RunAutomaton;

// TODO: add two-phase and needsScores support. maybe use conjunctionDISI internally?
class TermAutomatonScorer extends Scorer {
  private final EnumAndScorer[] subs;
  private final EnumAndScorer[] subsOnDoc;
  private final PriorityQueue<EnumAndScorer> docIDQueue;
  private final PriorityQueue<EnumAndScorer> posQueue;
  private final RunAutomaton runAutomaton;
  private final Map<Integer,BytesRef> idToTerm;

  // We reuse this array to check for matches starting from an initial
  // position; we increase posShift every time we move to a new possible
  // start:
  private PosState[] positions;
  int posShift;

  // This is -1 if wildcard (null) terms were not used, else it's the id
  // of the wildcard term:
  private final int anyTermID;
  private final LeafSimScorer docScorer;

  private int numSubsOnDoc;

  private final long cost;

  private int docID = -1;
  private int freq;

  public TermAutomatonScorer(TermAutomatonWeight weight, EnumAndScorer[] subs, int anyTermID, Map<Integer,BytesRef> idToTerm, LeafSimScorer docScorer) throws IOException {
    super(weight);
    //System.out.println("  automaton:\n" + weight.automaton.toDot());
    this.runAutomaton = new TermRunAutomaton(weight.automaton, subs.length);
    this.docScorer = docScorer;
    this.idToTerm = idToTerm;
    this.subs = subs;
    this.docIDQueue = new DocIDQueue(subs.length);
    this.posQueue = new PositionQueue(subs.length);
    this.anyTermID = anyTermID;
    this.subsOnDoc = new EnumAndScorer[subs.length];
    this.positions = new PosState[4];
    for(int i=0;i<this.positions.length;i++) {
      this.positions[i] = new PosState();
    }
    long cost = 0;

    // Init docIDQueue:
    for(EnumAndScorer sub : subs) {
      if (sub != null) {
        cost += sub.posEnum.cost();
        subsOnDoc[numSubsOnDoc++] = sub;
      }
    }
    this.cost = cost;
  }

  /** Sorts by docID so we can quickly pull out all scorers that are on
   *  the same (lowest) docID. */
  private static class DocIDQueue extends PriorityQueue<EnumAndScorer> {
    public DocIDQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(EnumAndScorer a, EnumAndScorer b) {
      return a.posEnum.docID() < b.posEnum.docID();
    }
  }

  /** Sorts by position so we can visit all scorers on one doc, by
   *  position. */
  private static class PositionQueue extends PriorityQueue<EnumAndScorer> {
    public PositionQueue(int maxSize) {
      super(maxSize);
    }

    @Override
    protected boolean lessThan(EnumAndScorer a, EnumAndScorer b) {
      return a.pos < b.pos;
    }
  }

  /** Pops all enums positioned on the current (minimum) doc */
  private void popCurrentDoc() {
    assert numSubsOnDoc == 0;
    assert docIDQueue.size() > 0;
    subsOnDoc[numSubsOnDoc++] = docIDQueue.pop();
    docID = subsOnDoc[0].posEnum.docID();
    while (docIDQueue.size() > 0 && docIDQueue.top().posEnum.docID() == docID) {
      subsOnDoc[numSubsOnDoc++] = docIDQueue.pop();
    }
  }

  /** Pushes all previously pop'd enums back into the docIDQueue */
  private void pushCurrentDoc() {
    for(int i=0;i<numSubsOnDoc;i++) {
      docIDQueue.add(subsOnDoc[i]);
    }
    numSubsOnDoc = 0;
  }

  @Override
  public DocIdSetIterator iterator() {
    return new DocIdSetIterator() {
      @Override
      public int docID() {
        return docID;
      }

      @Override
      public long cost() {
        return cost;
      }

      @Override
      public int nextDoc() throws IOException {
        // we only need to advance docs that are positioned since all docs in the
        // pq are guaranteed to be beyond the current doc already
        for(int i=0;i<numSubsOnDoc;i++) {
          EnumAndScorer sub = subsOnDoc[i];
          if (sub.posEnum.nextDoc() != NO_MORE_DOCS) {
            sub.posLeft = sub.posEnum.freq()-1;
            sub.pos = sub.posEnum.nextPosition();
          }
        }
        pushCurrentDoc();
        return doNext();
      }

      @Override
      public int advance(int target) throws IOException {
        // Both positioned docs and docs in the pq might be behind target

        // 1. Advance the PQ
        if (docIDQueue.size() > 0) {
          EnumAndScorer top = docIDQueue.top();
          while (top.posEnum.docID() < target) {
            if (top.posEnum.advance(target) != NO_MORE_DOCS) {
              top.posLeft = top.posEnum.freq()-1;
              top.pos = top.posEnum.nextPosition();
            }
            top = docIDQueue.updateTop();
          }
        }

        // 2. Advance subsOnDoc
        for(int i=0;i<numSubsOnDoc;i++) {
          EnumAndScorer sub = subsOnDoc[i];
          if (sub.posEnum.advance(target) != NO_MORE_DOCS) {
            sub.posLeft = sub.posEnum.freq()-1;
            sub.pos = sub.posEnum.nextPosition();
          }
        }
        pushCurrentDoc();
        return doNext();
      }

      private int doNext() throws IOException {
        assert numSubsOnDoc == 0;
        assert docIDQueue.top().posEnum.docID() > docID;
        while (true) {
          //System.out.println("  doNext: cycle");
          popCurrentDoc();
          //System.out.println("    docID=" + docID);
          if (docID == NO_MORE_DOCS) {
            return docID;
          }
          countMatches();
          if (freq > 0) {
            return docID;
          }
          for(int i=0;i<numSubsOnDoc;i++) {
            EnumAndScorer sub = subsOnDoc[i];
            if (sub.posEnum.nextDoc() != NO_MORE_DOCS) {
              sub.posLeft = sub.posEnum.freq()-1;
              sub.pos = sub.posEnum.nextPosition();
            }
          }
          pushCurrentDoc();
        }
      }
    };
  }

  private PosState getPosition(int pos) {
    return positions[pos-posShift];
  }

  private void shift(int pos) {
    int limit = pos-posShift;
    for(int i=0;i<limit;i++) {
      positions[i].count = 0;
    }
    posShift = pos;
  }

  private void countMatches() throws IOException {
    freq = 0;
    for(int i=0;i<numSubsOnDoc;i++) {
      posQueue.add(subsOnDoc[i]);
    }
    // System.out.println("\ncountMatches: " + numSubsOnDoc + " terms in doc=" + docID + " anyTermID=" + anyTermID + " id=" + reader.document(docID).get("id"));
    // System.out.println("\ncountMatches: " + numSubsOnDoc + " terms in doc=" + docID + " anyTermID=" + anyTermID);

    int lastPos = -1;

    posShift = -1;

    while (posQueue.size() != 0) {
      EnumAndScorer sub = posQueue.pop();

      // This is a graph intersection, and pos is the state this token
      // leaves from.  Until index stores posLength (which we could
      // stuff into a payload using a simple TokenFilter), this token
      // always transitions from state=pos to state=pos+1:
      final int pos = sub.pos;

      if (posShift == -1) {
        posShift = pos;
      }

      if (pos+1-posShift >= positions.length) {
        PosState[] newPositions = new PosState[ArrayUtil.oversize(pos+1-posShift, RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
        System.arraycopy(positions, 0, newPositions, 0, positions.length);
        for(int i=positions.length;i<newPositions.length;i++) {
          newPositions[i] = new PosState();
        }
        positions = newPositions;
      }

      // System.out.println("  term=" + idToTerm.get(sub.termID).utf8ToString() + " pos=" + pos + " (count=" + getPosition(pos).count + " lastPos=" + lastPos + ") posQueue.size=" + posQueue.size() + " posShift=" + posShift);

      PosState posState;
      PosState nextPosState;

      // Maybe advance ANY matches:
      if (lastPos != -1) {
        if (anyTermID != -1) {
          int startLastPos = lastPos;
          while (lastPos < pos) {
            posState = getPosition(lastPos);
            if (posState.count == 0 && lastPos > startLastPos) {
              // Petered out...
              lastPos = pos;
              break;
            }
            // System.out.println("  iter lastPos=" + lastPos + " count=" + posState.count);

            nextPosState = getPosition(lastPos+1);

            // Advance all states from lastPos -> pos, if they had an any arc:
            for(int i=0;i<posState.count;i++) {
              int state = runAutomaton.step(posState.states[i], anyTermID);
              if (state != -1) {
                // System.out.println("    add pos=" + (lastPos+1) + " state=" + state);
                nextPosState.add(state);
              }
            }

            lastPos++;
          }
        }
      }

      posState = getPosition(pos);
      nextPosState = getPosition(pos+1);

      // If there are no pending matches at neither this position or the
      // next position, then it's safe to shift back to positions[0]:
      if (posState.count == 0 && nextPosState.count == 0) {
        shift(pos);
        posState = getPosition(pos);
        nextPosState = getPosition(pos+1);
      }

      // Match current token:
      for(int i=0;i<posState.count;i++) {
        // System.out.println("    check cur state=" + posState.states[i]);
        int state = runAutomaton.step(posState.states[i], sub.termID);
        if (state != -1) {
          // System.out.println("      --> " + state);
          nextPosState.add(state);
          if (runAutomaton.isAccept(state)) {
            // System.out.println("      *** (1)");
            freq++;
          }
        }
      }

      // Also consider starting a new match from this position:
      int state = runAutomaton.step(0, sub.termID);
      if (state != -1) {
        // System.out.println("  add init state=" + state);
        nextPosState.add(state);
        if (runAutomaton.isAccept(state)) {
          // System.out.println("      *** (2)");
          freq++;
        }
      }

      if (sub.posLeft > 0) {
        // Put this sub back into the posQueue:
        sub.pos = sub.posEnum.nextPosition();
        sub.posLeft--;
        posQueue.add(sub);
      }

      lastPos = pos;
    }

    int limit = lastPos+1-posShift;
    // reset
    for(int i=0;i<=limit;i++) {
      positions[i].count = 0;
    }
  }

  @Override
  public String toString() {
    return "TermAutomatonScorer(" + weight + ")";
  }

  @Override
  public int docID() {
    return docID;
  }

  @Override
  public float score() throws IOException {
    // TODO: we could probably do better here, e.g. look @ freqs of actual terms involved in this doc and score differently
    return docScorer.score(docID, freq);
  }

  @Override
  public float getMaxScore(int upTo) throws IOException {
    return docScorer.getSimScorer().score(Float.MAX_VALUE, 1L);
  }

  static class TermRunAutomaton extends RunAutomaton {
    public TermRunAutomaton(Automaton a, int termCount) {
      super(a, termCount);
    }
  }

  private static class PosState {
    // Which automaton states we are in at this position
    int[] states = new int[2];

    // How many states
    int count;

    public void add(int state) {
      if (states.length == count) {
        states = ArrayUtil.grow(states);
      }
      states[count++] = state;
    }
  }
}
