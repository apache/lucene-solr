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

/**
 * Base class for Scorers that score disjunctions.
 */
abstract class DisjunctionScorer extends Scorer {
  private final Scorer subScorers[];
  private int numScorers;

  /** The document number of the current match. */
  protected int doc = -1;
  /** Number of matching scorers for the current match. */
  protected int freq = -1;
  
  protected DisjunctionScorer(Weight weight, Scorer subScorers[]) {
    super(weight);
    this.subScorers = subScorers;
    this.numScorers = subScorers.length;
    if (numScorers <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }
    heapify();
  }
  
  /** 
   * Organize subScorers into a min heap with scorers generating the earliest document on top.
   */
  private void heapify() {
    for (int i = (numScorers >>> 1) - 1; i >= 0; i--) {
      heapAdjust(i);
    }
  }
  
  /** 
   * The subtree of subScorers at root is a min heap except possibly for its root element.
   * Bubble the root down as required to make the subtree a heap.
   */
  private void heapAdjust(int root) {
    Scorer scorer = subScorers[root];
    int doc = scorer.docID();
    int i = root;
    while (i <= (numScorers >>> 1) - 1) {
      int lchild = (i << 1) + 1;
      Scorer lscorer = subScorers[lchild];
      int ldoc = lscorer.docID();
      int rdoc = Integer.MAX_VALUE, rchild = (i << 1) + 2;
      Scorer rscorer = null;
      if (rchild < numScorers) {
        rscorer = subScorers[rchild];
        rdoc = rscorer.docID();
      }
      if (ldoc < doc) {
        if (rdoc < ldoc) {
          subScorers[i] = rscorer;
          subScorers[rchild] = scorer;
          i = rchild;
        } else {
          subScorers[i] = lscorer;
          subScorers[lchild] = scorer;
          i = lchild;
        }
      } else if (rdoc < doc) {
        subScorers[i] = rscorer;
        subScorers[rchild] = scorer;
        i = rchild;
      } else {
        return;
      }
    }
  }

  /** 
   * Remove the root Scorer from subScorers and re-establish it as a heap
   */
  private void heapRemoveRoot() {
    if (numScorers == 1) {
      subScorers[0] = null;
      numScorers = 0;
    } else {
      subScorers[0] = subScorers[numScorers - 1];
      subScorers[numScorers - 1] = null;
      --numScorers;
      heapAdjust(0);
    }
  }
  
  @Override
  public final Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>(numScorers);
    for (int i = 0; i < numScorers; i++) {
      children.add(new ChildScorer(subScorers[i], "SHOULD"));
    }
    return children;
  }

  @Override
  public final long cost() {
    long sum = 0;
    for (int i = 0; i < numScorers; i++) {
      sum += subScorers[i].cost();
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
    while(true) {
      if (subScorers[0].nextDoc() != NO_MORE_DOCS) {
        heapAdjust(0);
      } else {
        heapRemoveRoot();
        if (numScorers == 0) {
          return doc = NO_MORE_DOCS;
        }
      }
      int docID = subScorers[0].docID();
      if (docID != doc) {
        freq = -1;
        return doc = docID;
      }
    }
  }
  
  @Override
  public final int advance(int target) throws IOException {
    assert doc != NO_MORE_DOCS;
    while(true) {
      if (subScorers[0].advance(target) != NO_MORE_DOCS) {
        heapAdjust(0);
      } else {
        heapRemoveRoot();
        if (numScorers == 0) {
          return doc = NO_MORE_DOCS;
        }
      }
      int docID = subScorers[0].docID();
      if (docID >= target) {
        freq = -1;
        return doc = docID;
      }
    }
  }
  
  // if we haven't already computed freq + score, do so
  private void visitScorers() throws IOException {
    reset();
    freq = 1;
    accum(subScorers[0]);
    visit(1);
    visit(2);
  }
  
  // TODO: remove recursion.
  private void visit(int root) throws IOException {
    if (root < numScorers && subScorers[root].docID() == doc) {
      freq++;
      accum(subScorers[root]);
      visit((root<<1)+1);
      visit((root<<1)+2);
    }
  }
  
  @Override
  public final float score() throws IOException {
    visitScorers();
    return getFinal();
  }

  @Override
  public final int freq() throws IOException {
    if (freq < 0) {
      visitScorers();
    }
    return freq;
  }
  
  /** Reset score state for a new match */
  protected abstract void reset();
  
  /** Factor in sub-scorer match */
  protected abstract void accum(Scorer subScorer) throws IOException;
  
  /** Return final score */
  protected abstract float getFinal();
}
