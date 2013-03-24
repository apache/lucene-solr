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
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.ArrayUtil;

/**
 * A Scorer for OR like queries, counterpart of <code>ConjunctionScorer</code>.
 * This Scorer implements {@link Scorer#advance(int)} and uses advance() on the given Scorers.
 * 
 * This implementation uses the minimumMatch constraint actively to efficiently
 * prune the number of candidates, it is hence a mixture between a pure DisjunctionScorer
 * and a ConjunctionScorer.
 */
class MinShouldMatchSumScorer extends Scorer {

  /** The overall number of non-finalized scorers */
  private int numScorers;
  /** The minimum number of scorers that should match */
  private final int mm;

  /** A static array of all subscorers sorted by decreasing cost */
  private final Scorer sortedSubScorers[];
  /** A monotonically increasing index into the array pointing to the next subscorer that is to be excluded */
  private int sortedSubScorersIdx = 0;

  private final Scorer subScorers[]; // the first numScorers-(mm-1) entries are valid
  private int nrInHeap; // 0..(numScorers-(mm-1)-1)

  /** mmStack is supposed to contain the most costly subScorers that still did
   *  not run out of docs, sorted by increasing sparsity of docs returned by that subScorer.
   *  For now, the cost of subscorers is assumed to be inversely correlated with sparsity.
   */
  private final Scorer mmStack[]; // of size mm-1: 0..mm-2, always full

  /** The document number of the current match. */
  private int doc = -1;
  /** The number of subscorers that provide the current match. */
  protected int nrMatchers = -1;
  private double score = Float.NaN;

  /**
   * Construct a <code>MinShouldMatchSumScorer</code>.
   * 
   * @param weight The weight to be used.
   * @param subScorers A collection of at least two subscorers.
   * @param minimumNrMatchers The positive minimum number of subscorers that should
   * match to match this query.
   * <br>When <code>minimumNrMatchers</code> is bigger than
   * the number of <code>subScorers</code>, no matches will be produced.
   * <br>When minimumNrMatchers equals the number of subScorers,
   * it is more efficient to use <code>ConjunctionScorer</code>.
   */
  public MinShouldMatchSumScorer(Weight weight, List<Scorer> subScorers, int minimumNrMatchers) throws IOException {
    super(weight);
    this.nrInHeap = this.numScorers = subScorers.size();

    if (minimumNrMatchers <= 0) {
      throw new IllegalArgumentException("Minimum nr of matchers must be positive");
    }
    if (numScorers <= 1) {
      throw new IllegalArgumentException("There must be at least 2 subScorers");
    }

    this.mm = minimumNrMatchers;
    this.sortedSubScorers = subScorers.toArray(new Scorer[this.numScorers]);
    // sorting by decreasing subscorer cost should be inversely correlated with
    // next docid (assuming costs are due to generating many postings)
    ArrayUtil.mergeSort(sortedSubScorers, new Comparator<Scorer>() {
      @Override
      public int compare(Scorer o1, Scorer o2) {
        return Long.signum(o2.cost() - o1.cost());
      }
    });
    // take mm-1 most costly subscorers aside
    this.mmStack = new Scorer[mm-1];
    for (int i = 0; i < mm-1; i++) {
      mmStack[i] = sortedSubScorers[i];
    }
    nrInHeap -= mm-1;
    this.sortedSubScorersIdx = mm-1;
    // take remaining into heap, if any, and heapify
    this.subScorers = new Scorer[nrInHeap];
    for (int i = 0; i < nrInHeap; i++) {
      this.subScorers[i] = this.sortedSubScorers[mm-1+i];
    }
    minheapHeapify();
    assert minheapCheck();
  }
  
  /**
   * Construct a <code>DisjunctionScorer</code>, using one as the minimum number
   * of matching subscorers.
   */
  public MinShouldMatchSumScorer(Weight weight, List<Scorer> subScorers) throws IOException {
    this(weight, subScorers, 1);
  }

  @Override
  public final Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<ChildScorer>(numScorers);
    for (int i = 0; i < numScorers; i++) {
      children.add(new ChildScorer(subScorers[i], "SHOULD"));
    }
    return children;
  }
  
  @Override
  public int nextDoc() throws IOException {
    assert doc != NO_MORE_DOCS;
    while (true) {
      // to remove current doc, call next() on all subScorers on current doc within heap
      while (subScorers[0].docID() == doc) {
        if (subScorers[0].nextDoc() != NO_MORE_DOCS) {
          minheapSiftDown(0);
        } else {
          minheapRemoveRoot();
          numScorers--;
          if (numScorers < mm) {
            return doc = NO_MORE_DOCS;
          }
        }
        //assert minheapCheck();
      }

      evaluateSmallestDocInHeap();

      if (nrMatchers >= mm) { // doc satisfies mm constraint
        break;
      }
    }
    return doc;
  }
  
  private void evaluateSmallestDocInHeap() throws IOException {
    // within heap, subScorer[0] now contains the next candidate doc
    doc = subScorers[0].docID();
    if (doc == NO_MORE_DOCS) {
      nrMatchers = Integer.MAX_VALUE; // stop looping
      return;
    }
    // 1. score and count number of matching subScorers within heap
    score = subScorers[0].score();
    nrMatchers = 1;
    countMatches(1);
    countMatches(2);
    // 2. score and count number of matching subScorers within stack,
    // short-circuit: stop when mm can't be reached for current doc, then perform on heap next()
    // TODO instead advance() might be possible, but complicates things
    for (int i = mm-2; i >= 0; i--) { // first advance sparsest subScorer
      if (mmStack[i].docID() >= doc || mmStack[i].advance(doc) != NO_MORE_DOCS) {
        if (mmStack[i].docID() == doc) { // either it was already on doc, or got there via advance()
          nrMatchers++;
          score += mmStack[i].score();
        } else { // scorer advanced to next after doc, check if enough scorers left for current doc
          if (nrMatchers + i < mm) { // too few subScorers left, abort advancing
            return; // continue looping TODO consider advance() here
          }
        }
      } else { // subScorer exhausted
        numScorers--;
        if (numScorers < mm) { // too few subScorers left
          doc = NO_MORE_DOCS;
          nrMatchers = Integer.MAX_VALUE; // stop looping
          return;
        }
        if (mm-2-i > 0) {
          // shift RHS of array left
          System.arraycopy(mmStack, i+1, mmStack, i, mm-2-i);
        }
        // find next most costly subScorer within heap TODO can this be done better?
        while (!minheapRemove(sortedSubScorers[sortedSubScorersIdx++])) {
          //assert minheapCheck();
        }
        // add the subScorer removed from heap to stack
        mmStack[mm-2] = sortedSubScorers[sortedSubScorersIdx-1];
        
        if (nrMatchers + i < mm) { // too few subScorers left, abort advancing
          return; // continue looping TODO consider advance() here
        }
      }
    }
  }

  // TODO: this currently scores, but so did the previous impl
  // TODO: remove recursion.
  // TODO: consider separating scoring out of here, then modify this
  // and afterNext() to terminate when nrMatchers == minimumNrMatchers
  // then also change freq() to just always compute it from scratch
  private void countMatches(int root) throws IOException {
    if (root < nrInHeap && subScorers[root].docID() == doc) {
      nrMatchers++;
      score += subScorers[root].score();
      countMatches((root<<1)+1);
      countMatches((root<<1)+2);
    }
  }

  /**
   * Returns the score of the current document matching the query. Initially
   * invalid, until {@link #nextDoc()} is called the first time.
   */
  @Override
  public float score() throws IOException {
    return (float) score;
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int freq() throws IOException {
    return nrMatchers;
  }

  /**
   * Advances to the first match beyond the current whose document number is
   * greater than or equal to a given target. <br>
   * The implementation uses the advance() method on the subscorers.
   * 
   * @param target the target document number.
   * @return the document whose number is greater than or equal to the given
   *         target, or -1 if none exist.
   */
  @Override
  public int advance(int target) throws IOException {
    if (numScorers < mm)
      return doc = NO_MORE_DOCS;
    // advance all Scorers in heap at smaller docs to at least target
    while (subScorers[0].docID() < target) {
      if (subScorers[0].advance(target) != NO_MORE_DOCS) {
        minheapSiftDown(0);
      } else {
        minheapRemoveRoot();
        numScorers--;
        if (numScorers < mm) {
          return doc = NO_MORE_DOCS;
        }
      }
      //assert minheapCheck();
    }

    evaluateSmallestDocInHeap();

    if (nrMatchers >= mm) {
      return doc;
    } else {
      return nextDoc();
    }
  }
  
  @Override
  public long cost() {
    // cost for merging of lists analog to DisjunctionSumScorer
    long costCandidateGeneration = 0;
    for (int i = 0; i < nrInHeap; i++)
      costCandidateGeneration += subScorers[i].cost();
    // TODO is cost for advance() different to cost for iteration + heap merge
    //      and how do they compare overall to pure disjunctions? 
    final float c1 = 1.0f,
                c2 = 1.0f; // maybe a constant, maybe a proportion between costCandidateGeneration and sum(subScorer_to_be_advanced.cost())?
    return (long) (
           c1 * costCandidateGeneration +        // heap-merge cost
           c2 * costCandidateGeneration * (mm-1) // advance() cost
           );
  }
  
  /**
   * Organize subScorers into a min heap with scorers generating the earliest document on top.
   */
  protected final void minheapHeapify() {
    for (int i = (nrInHeap >> 1) - 1; i >= 0; i--) {
      minheapSiftDown(i);
    }
  }
  
  /**
   * The subtree of subScorers at root is a min heap except possibly for its root element.
   * Bubble the root down as required to make the subtree a heap.
   */
  protected final void minheapSiftDown(int root) {
    // TODO could this implementation also move rather than swapping neighbours?
    Scorer scorer = subScorers[root];
    int doc = scorer.docID();
    int i = root;
    while (i <= (nrInHeap >> 1) - 1) {
      int lchild = (i << 1) + 1;
      Scorer lscorer = subScorers[lchild];
      int ldoc = lscorer.docID();
      int rdoc = Integer.MAX_VALUE, rchild = (i << 1) + 2;
      Scorer rscorer = null;
      if (rchild < nrInHeap) {
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

  protected final void minheapSiftUp(int i) {
    Scorer scorer = subScorers[i];
    final int doc = scorer.docID();
    // find right place for scorer
    while (i > 0) {
      int parent = (i - 1) >> 1;
      Scorer pscorer = subScorers[parent];
      int pdoc = pscorer.docID();
      if (pdoc > doc) { // move root down, make space
        subScorers[i] = subScorers[parent];
        i = parent;
      } else { // done, found right place
        break;
      }
    }
    subScorers[i] = scorer;
  }

  /**
   * Remove the root Scorer from subScorers and re-establish it as a heap
   */
  protected final void minheapRemoveRoot() {
    if (nrInHeap == 1) {
      //subScorers[0] = null; // not necessary
      nrInHeap = 0;
    } else {
      nrInHeap--;
      subScorers[0] = subScorers[nrInHeap];
      //subScorers[nrInHeap] = null; // not necessary
      minheapSiftDown(0);
    }
  }
  
  /**
   * Removes a given Scorer from the heap by placing end of heap at that
   * position and bubbling it either up or down
   */
  protected final boolean minheapRemove(Scorer scorer) {
    // find scorer: O(nrInHeap)
    for (int i = 0; i < nrInHeap; i++) {
      if (subScorers[i] == scorer) { // remove scorer
        subScorers[i] = subScorers[--nrInHeap];
        //if (i != nrInHeap) subScorers[nrInHeap] = null; // not necessary
        minheapSiftUp(i);
        minheapSiftDown(i);
        return true;
      }
    }
    return false; // scorer already exhausted
  }
  
  boolean minheapCheck() {
    return minheapCheck(0);
  }
  private boolean minheapCheck(int root) {
    if (root >= nrInHeap)
      return true;
    int lchild = (root << 1) + 1;
    int rchild = (root << 1) + 2;
    if (lchild < nrInHeap && subScorers[root].docID() > subScorers[lchild].docID())
      return false;
    if (rchild < nrInHeap && subScorers[root].docID() > subScorers[rchild].docID())
      return false;
    return minheapCheck(lchild) && minheapCheck(rchild);
  }
  
}