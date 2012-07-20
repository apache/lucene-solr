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

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.search.positions.Interval;
import org.apache.lucene.search.positions.IntervalIterator;
import org.apache.lucene.search.positions.IntervalIterator.IntervalCollector;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

/** Expert: Scoring functionality for phrase queries.
 * <br>A document is considered matching if it contains the phrase-query terms  
 * at "valid" positions. What "valid positions" are
 * depends on the type of the phrase query: for an exact phrase query terms are required 
 * to appear in adjacent locations, while for a sloppy phrase query some distance between 
 * the terms is allowed. The abstract method {@link #phraseFreq()} of extending classes
 * is invoked for each document containing all the phrase query terms, in order to 
 * compute the frequency of the phrase query in that document. A non zero frequency
 * means a match. 
 */
abstract class PhraseScorer extends Scorer {
  PhrasePositions min, max;

  private float freq; //phrase frequency in current doc as computed by phraseFreq().

  final Similarity.SloppySimScorer docScorer;
  protected final PhraseQuery.PostingsAndFreq[] postings;

  PhraseScorer(Weight weight, PhraseQuery.PostingsAndFreq[] postings,
      Similarity.SloppySimScorer docScorer) throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.postings = postings;
    reset(false);
  }
  void reset(boolean needsOffsets) throws IOException {
    // convert tps to a list of phrase positions.
    // note: phrase-position differs from term-position in that its position
    // reflects the phrase offset: pp.pos = tp.pos - offset.
    // this allows to easily identify a matching (exact) phrase 
    // when all PhrasePositions have exactly the same position.
    if (postings.length > 0) {
      min = new PhrasePositions(postings[0].postings, postings[0].position, 0, postings[0].terms);
      max = min;
      max.doc = -1;
      for (int i = 1; i < postings.length; i++) {
        PhrasePositions pp = new PhrasePositions(postings[i].postings, postings[i].position, i, postings[i].terms);
        max.next = pp;
        max = pp;
        max.doc = -1;
      }
      max.next = min; // make it cyclic for easier manipulation
    }
  }

  @Override
  public int docID() {
    return max.doc; 
  }

  @Override
  public int nextDoc() throws IOException {
    return advance(max.doc);
  }
  
  @Override
  public float score() throws IOException {
    return docScorer.score(max.doc, freq);
  }

  protected boolean advanceMin(int target) throws IOException {
    if (!min.skipTo(target)) { 
      max.doc = NO_MORE_DOCS; // for further calls to docID() 
      return false;
    }
    min = min.next; // cyclic
    max = max.next; // cyclic
    return true;
  }
  
  @Override
  public int advance(int target) throws IOException {
    freq = 0.0f;
    if (!advanceMin(target)) {
      return NO_MORE_DOCS;
    }        
    boolean restart=false;
    while (freq == 0.0f) {
      while (min.doc < max.doc || restart) {
        restart = false;
        if (!advanceMin(max.doc)) {
          return NO_MORE_DOCS;
        }        
      }
      // found a doc with all of the terms
      freq = phraseFreq(); // check for phrase
      restart = true;
    } 

    // found a match
    return max.doc;
  }
  
  /**
   * phrase frequency in current doc as computed by phraseFreq().
   */
  @Override
  public final float freq() {
    return freq;
  }

  /**
   * For a document containing all the phrase query terms, compute the
   * frequency of the phrase in that document. 
   * A non zero frequency means a match.
   * <br>Note, that containing all phrase terms does not guarantee a match - they have to be found in matching locations.  
   * @return frequency of the phrase in current doc, 0 if not found. 
   */
  abstract float phraseFreq() throws IOException;

  @Override
  public String toString() { return "scorer(" + weight + ")"; }
  
  
  final static class AdvancingIntervalIterator extends IntervalIterator {

    public AdvancingIntervalIterator(Scorer scorer, boolean collectPositions, final DocsAndPositionsEnum[] enums, final IntervalIterator delegate) {
      super(scorer, collectPositions);
      this.enums = enums;
      this.delegate = delegate;
    }

    private final DocsAndPositionsEnum[] enums;
    private final IntervalIterator delegate;
    @Override
    public int scorerAdvanced(int docId) throws IOException {
      assert docId == docID();
      for (DocsAndPositionsEnum oneEnum : enums) {
        int advance = oneEnum.advance(docId);
        assert advance == docId;
      }
      delegate.scorerAdvanced(docId);
      return docId;
    }

    @Override
    public Interval next() throws IOException {
      return delegate.next();
    }

    @Override
    public void collect(IntervalCollector collector) {
      delegate.collect(collector);
    }

    @Override
    public IntervalIterator[] subs(boolean inOrder) {
      return delegate.subs(inOrder);
    }

    @Override
    public int matchDistance() {
      return delegate.matchDistance();
    }
    
  }
 
}
