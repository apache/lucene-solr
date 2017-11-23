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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A {@link Scorer} whose number of matches is per-document. */
final class CoveringScorer extends Scorer {

  final int numScorers;
  final int maxDoc;
  final LongValues minMatchValues;

  boolean matches; // if true then the doc matches, otherwise we don't know and need to check
  int doc;  // current doc ID
  DisiWrapper topList; // list of matches
  int freq; // number of scorers on the desired doc ID
  long minMatch; // current required number of matches

  // priority queue that stores all scorers
  final DisiPriorityQueue subScorers;

  final long cost;

  CoveringScorer(Weight weight, Collection<Scorer> scorers, LongValues minMatchValues, int maxDoc) {
    super(weight);

    this.numScorers = scorers.size();
    this.maxDoc = maxDoc;
    this.minMatchValues = minMatchValues;
    this.doc = -1;

    subScorers = new DisiPriorityQueue(scorers.size());

    for (Scorer scorer : scorers) {
      subScorers.add(new DisiWrapper(scorer));
    }

    this.cost = scorers.stream().map(Scorer::iterator).mapToLong(DocIdSetIterator::cost).sum();
  }

  @Override
  public final Collection<ChildScorer> getChildren() throws IOException {
    List<ChildScorer> matchingChildren = new ArrayList<>();
    setTopListAndFreqIfNecessary();
    for (DisiWrapper s = topList; s != null; s = s.next) {
      matchingChildren.add(new ChildScorer(s.scorer, "SHOULD"));
    }
    return matchingChildren;
  }

  private final DocIdSetIterator approximation = new DocIdSetIterator() {

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID() + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      // reset state
      matches = false;
      topList = null;

      doc = target;
      setMinMatch();

      DisiWrapper top = subScorers.top();
      int numMatches = 0;
      int maxPotentialMatches = numScorers;
      while (top.doc < target) {
        if (maxPotentialMatches < minMatch) {
          // No need to keep trying to advance to `target` since no match is possible.
          if (target >= maxDoc - 1) {
            doc = NO_MORE_DOCS;
          } else {
            doc = target + 1;
          }
          setMinMatch();
          return doc;
        }
        top.doc = top.iterator.advance(target);
        boolean match = top.doc == target;
        top = subScorers.updateTop();
        if (match) {
          numMatches++;
          if (numMatches >= minMatch) {
            // success, no need to check other iterators
            matches = true;
            return doc;
          }
        } else {
          maxPotentialMatches--;
        }
      }

      doc = top.doc;
      setMinMatch();
      return doc;
    }

    private void setMinMatch() throws IOException {
      if (doc >= maxDoc) {
        // advanceExact may not be called on out-of-range doc ids
        minMatch = 1;
      } else if (minMatchValues.advanceExact(doc)) {
        // values < 1 are treated as 1: we require at least one match
        minMatch = Math.max(1, minMatchValues.longValue());
      } else {
        // this will make sure the document does not match
        minMatch = Long.MAX_VALUE;
      }
    }

    @Override
    public long cost() {
      return maxDoc;
    }

  };

  private final TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

    @Override
    public boolean matches() throws IOException {
      if (matches) {
        return true;
      }
      if (topList == null) {
        advanceAll(doc);
      }
      if (subScorers.top().doc != doc) {
        assert subScorers.top().doc > doc;
        return false;
      }
      setTopListAndFreq();
      assert topList.doc == doc;
      return matches = freq >= minMatch;
    }

    @Override
    public float matchCost() {
      return numScorers;
    }

  };

  @Override
  public DocIdSetIterator iterator() {
    return TwoPhaseIterator.asDocIdSetIterator(twoPhase);
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return twoPhase;
  }

  private void advanceAll(int target) throws IOException {
    DisiWrapper top = subScorers.top();
    while (top.doc < target) {
      top.doc = top.iterator.advance(target);
      top = subScorers.updateTop();
    }
  }

  private void setTopListAndFreq() {
    topList = subScorers.topList();
    freq = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      freq++;
    }
  }

  private void setTopListAndFreqIfNecessary() throws IOException {
    if (topList == null) {
      advanceAll(doc);
      setTopListAndFreq();
    }
  }

  @Override
  public float score() throws IOException {
    // we need to know about all matches
    setTopListAndFreqIfNecessary();
    double score = 0;
    for (DisiWrapper w = topList; w != null; w = w.next) {
      score += w.scorer.score();
    }
    return (float) score;
  }

  @Override
  public int docID() {
    return doc;
  }

}
