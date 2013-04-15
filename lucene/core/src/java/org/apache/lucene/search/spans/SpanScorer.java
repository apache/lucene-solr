package org.apache.lucene.search.spans;

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

import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.similarities.Similarity;

/**
 * Public for extension only.
 */
public class SpanScorer extends Scorer {
  protected Spans spans;

  protected boolean more = true;

  protected int doc;
  protected float freq;
  protected int numMatches;
  protected final Similarity.SloppySimScorer docScorer;
  
  protected SpanScorer(Spans spans, Weight weight, Similarity.SloppySimScorer docScorer)
  throws IOException {
    super(weight);
    this.docScorer = docScorer;
    this.spans = spans;

    doc = -1;
    more = spans.next();
  }

  @Override
  public int nextDoc() throws IOException {
    if (!setFreqCurrentDoc()) {
      doc = NO_MORE_DOCS;
    }
    return doc;
  }

  @Override
  public int advance(int target) throws IOException {
    if (!more) {
      return doc = NO_MORE_DOCS;
    }
    if (spans.doc() < target) { // setFreqCurrentDoc() leaves spans.doc() ahead
      more = spans.skipTo(target);
    }
    if (!setFreqCurrentDoc()) {
      doc = NO_MORE_DOCS;
    }
    return doc;
  }
  
  protected boolean setFreqCurrentDoc() throws IOException {
    if (!more) {
      return false;
    }
    doc = spans.doc();
    freq = 0.0f;
    numMatches = 0;
    do {
      int matchLength = spans.end() - spans.start();
      freq += docScorer.computeSlopFactor(matchLength);
      numMatches++;
      more = spans.next();
    } while (more && (doc == spans.doc()));
    return true;
  }

  @Override
  public int docID() { return doc; }

  @Override
  public float score() throws IOException {
    return docScorer.score(doc, freq);
  }
  
  @Override
  public int freq() throws IOException {
    return numMatches;
  }
  
  /** Returns the intermediate "sloppy freq" adjusted for edit distance 
   *  @lucene.internal */
  // only public so .payloads can see it.
  public float sloppyFreq() throws IOException {
    return freq;
  }
  
  @Override
  public long cost() {
    return spans.cost();
  }
}
