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
import org.apache.lucene.util.BytesRef;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {

  protected int lastDoc = -1;
  protected final DocsAndFreqs[] docsAndFreqs;
  private final DocsAndFreqs lead;
  private final Scorer[] scorers;
  private final float coord;

  ConjunctionScorer(Weight weight, List<? extends DocIdSetIterator> required, List<Scorer> scorers) {
    this(weight, required, scorers, 1f);
  }

  /** Create a new {@link ConjunctionScorer}, note that {@code scorers} must be a subset of {@code required}. */
  ConjunctionScorer(Weight weight, List<? extends DocIdSetIterator> required, List<Scorer> scorers, float coord) {
    super(weight);
    assert required.containsAll(scorers);
    this.coord = coord;
    this.docsAndFreqs = new DocsAndFreqs[required.size()];
    for (int i = 0; i < required.size(); ++i) {
      docsAndFreqs[i] = new DocsAndFreqs(required.get(i));
    }
    // Sort the array the first time to allow the least frequent DocsEnum to
    // lead the matching.
    ArrayUtil.timSort(docsAndFreqs, new Comparator<DocsAndFreqs>() {
      @Override
      public int compare(DocsAndFreqs o1, DocsAndFreqs o2) {
        return Long.compare(o1.cost, o2.cost);
      }
    });

    lead = docsAndFreqs[0]; // least frequent DocsEnum leads the intersection

    this.scorers = scorers.toArray(new Scorer[scorers.size()]);
  }

  private int doNext(int doc) throws IOException {
    for(;;) {
      // doc may already be NO_MORE_DOCS here, but we don't check explicitly
      // since all scorers should advance to NO_MORE_DOCS, match, then
      // return that value.
      advanceHead: for(;;) {
        for (int i = 1; i < docsAndFreqs.length; i++) {
          // invariant: docsAndFreqs[i].doc <= doc at this point.

          // docsAndFreqs[i].doc may already be equal to doc if we "broke advanceHead"
          // on the previous iteration and the advance on the lead scorer exactly matched.
          if (docsAndFreqs[i].doc < doc) {
            docsAndFreqs[i].doc = docsAndFreqs[i].iterator.advance(doc);

            if (docsAndFreqs[i].doc > doc) {
              // DocsEnum beyond the current doc - break and advance lead to the new highest doc.
              doc = docsAndFreqs[i].doc;
              break advanceHead;
            }
          }
        }
        // success - all DocsEnums are on the same doc
        return doc;
      }
      // advance head for next iteration
      doc = lead.doc = lead.iterator.advance(doc);
    }
  }

  @Override
  public int advance(int target) throws IOException {
    lead.doc = lead.iterator.advance(target);
    return lastDoc = doNext(lead.doc);
  }

  @Override
  public int docID() {
    return lastDoc;
  }

  @Override
  public int nextDoc() throws IOException {
    lead.doc = lead.iterator.nextDoc();
    return lastDoc = doNext(lead.doc);
  }

  @Override
  public float score() throws IOException {
    // TODO: sum into a double and cast to float if we ever send required clauses to BS1
    float sum = 0.0f;
    for (Scorer scorer : scorers) {
      sum += scorer.score();
    }
    return sum * coord;
  }

  @Override
  public int freq() {
    return docsAndFreqs.length;
  }

  @Override
  public int nextPosition() throws IOException {
    return -1;
  }

  @Override
  public int startOffset() throws IOException {
    return -1;
  }

  @Override
  public int endOffset() throws IOException {
    return -1;
  }

  @Override
  public BytesRef getPayload() throws IOException {
    return null;
  }

  @Override
  public long cost() {
    return lead.iterator.cost();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>(docsAndFreqs.length);
    for (Scorer scorer : scorers) {
      children.add(new ChildScorer(scorer, "MUST"));
    }
    return children;
  }

  static final class DocsAndFreqs {
    final long cost;
    final DocIdSetIterator iterator;
    int doc = -1;

    DocsAndFreqs(DocIdSetIterator iterator) {
      this.iterator = iterator;
      this.cost = iterator.cost();
    }
  }
}
