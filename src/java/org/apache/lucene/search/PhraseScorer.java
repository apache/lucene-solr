package org.apache.lucene.search;

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

import org.apache.lucene.index.*;

abstract class PhraseScorer extends Scorer {
  private Weight weight;
  protected byte[] norms;
  protected float value;

  private boolean firstTime = true;
  private boolean more = true;
  protected PhraseQueue pq;
  protected PhrasePositions first, last;

  private float freq;

  PhraseScorer(Weight weight, TermPositions[] tps, Similarity similarity,
               byte[] norms) {
    super(similarity);
    this.norms = norms;
    this.weight = weight;
    this.value = weight.getValue();

    // convert tps to a list
    for (int i = 0; i < tps.length; i++) {
      PhrasePositions pp = new PhrasePositions(tps[i], i);
      if (last != null) {			  // add next to end of list
        last.next = pp;
      } else
        first = pp;
      last = pp;
    }

    pq = new PhraseQueue(tps.length);             // construct empty pq

  }

  public int doc() { return first.doc; }

  public boolean next() throws IOException {
    if (firstTime) {
      init();
      firstTime = false;
    } else if (more) {
      more = last.next();                         // trigger further scanning
    }
    return doNext();
  }
  
  // next without initial increment
  private boolean doNext() throws IOException {
    while (more) {
      while (more && first.doc < last.doc) {      // find doc w/ all the terms
        more = first.skipTo(last.doc);            // skip first upto last
        firstToLast();                            // and move it to the end
      }

      if (more) {
        // found a doc with all of the terms
        freq = phraseFreq();                      // check for phrase
        if (freq == 0.0f)                         // no match
          more = last.next();                     // trigger further scanning
        else
          return true;                            // found a match
      }
    }
    return false;                                 // no more matches
  }

  public float score() throws IOException {
    //System.out.println("scoring " + first.doc);
    float raw = getSimilarity().tf(freq) * value; // raw score
    return raw * Similarity.decodeNorm(norms[first.doc]); // normalize
  }

  public boolean skipTo(int target) throws IOException {
    for (PhrasePositions pp = first; more && pp != null; pp = pp.next) {
      more = pp.skipTo(target);
    }
    if (more)
      sort();                                     // re-sort
    return doNext();
  }

  protected abstract float phraseFreq() throws IOException;

  private void init() throws IOException {
    for (PhrasePositions pp = first; more && pp != null; pp = pp.next) 
      more = pp.next();
    if(more)
      sort();
  }
  
  private void sort() {
    pq.clear();
    for (PhrasePositions pp = first; pp != null; pp = pp.next)
      pq.put(pp);
    pqToList();
  }

  protected final void pqToList() {
    last = first = null;
    while (pq.top() != null) {
      PhrasePositions pp = (PhrasePositions) pq.pop();
      if (last != null) {			  // add next to end of list
        last.next = pp;
      } else
        first = pp;
      last = pp;
      pp.next = null;
    }
  }

  protected final void firstToLast() {
    last.next = first;			  // move first to end of list
    last = first;
    first = first.next;
    last.next = null;
  }

  public Explanation explain(final int doc) throws IOException {
    Explanation tfExplanation = new Explanation();

    while (next() && doc() < doc) {}

    float phraseFreq = (doc() == doc) ? freq : 0.0f;
    tfExplanation.setValue(getSimilarity().tf(phraseFreq));
    tfExplanation.setDescription("tf(phraseFreq=" + phraseFreq + ")");

    return tfExplanation;
  }

  public String toString() { return "scorer(" + weight + ")"; }

}
