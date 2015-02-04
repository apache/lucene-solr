package org.apache.lucene.search;

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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.lucene.search.ScorerPriorityQueue.ScorerWrapper;

/**
 * Base class for Scorers that score disjunctions.
 */
abstract class DisjunctionScorer extends Scorer {
  private final ScorerPriorityQueue subScorers;

  /** The document number of the current match. */
  protected int doc = -1;
  /** Number of matching scorers for the current match. */
  private int freq = -1;
  /** Linked list of scorers which are on the current doc */
  private ScorerWrapper topScorers;

  protected DisjunctionScorer(Weight weight, Scorer subScorers[]) {
    super(weight);
    if (subScorers.length <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    this.subScorers = new ScorerPriorityQueue(subScorers.length);
    for (Scorer scorer : subScorers) {
      this.subScorers.add(new ScorerWrapper(scorer));
    }
  }
  
  @Override
  public final Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
    for (ScorerWrapper scorer : subScorers) {
      children.add(new ChildScorer(scorer.scorer, "SHOULD"));
    }
    return children;
  }

  @Override
  public final long cost() {
    long sum = 0;
    for (ScorerWrapper scorer : subScorers) {
      sum += scorer.cost;
    }
    return sum;
  } 
  
  @Override
  public final int docID() {
   return doc;
  }
 
  @Override
  public final int nextDoc() throws IOException {
    assert doc != NO_MORE_DOCS;

    ScorerWrapper top = subScorers.top();
    final int doc = this.doc;
    while (top.doc == doc) {
      top.doc = top.scorer.nextDoc();
      if (top.doc == NO_MORE_DOCS) {
        subScorers.pop();
        if (subScorers.size() == 0) {
          return this.doc = NO_MORE_DOCS;
        }
        top = subScorers.top();
      } else {
        top = subScorers.updateTop();
      }
    }

    freq = -1;
    return this.doc = top.doc;
  }
  
  @Override
  public final int advance(int target) throws IOException {
    assert doc != NO_MORE_DOCS;

    ScorerWrapper top = subScorers.top();
    while (top.doc < target) {
      top.doc = top.scorer.advance(target);
      if (top.doc == NO_MORE_DOCS) {
        subScorers.pop();
        if (subScorers.size() == 0) {
          return this.doc = NO_MORE_DOCS;
        }
        top = subScorers.top();
      } else {
        top = subScorers.updateTop();
      }
    }

    freq = -1;
    return this.doc = top.doc;
  }

  @Override
  public final int freq() throws IOException {
    if (freq < 0) {
      topScorers = subScorers.topList();
      int freq = 1;
      for (ScorerWrapper w = topScorers.next; w != null; w = w.next) {
        freq += 1;
      }
      this.freq = freq;
    }
    return freq;
  }

  @Override
  public final float score() throws IOException {
    final int freq = freq(); // compute the top scorers if necessary
    return score(topScorers, freq);
  }

  /** Compute the score for the given linked list of scorers. */
  protected abstract float score(ScorerWrapper topList, int freq) throws IOException;

}
