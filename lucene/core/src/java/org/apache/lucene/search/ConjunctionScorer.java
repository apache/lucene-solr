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

import org.apache.lucene.util.ArrayUtil;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {
  protected int lastDoc = -1;
  protected final DocsAndFreqs[] docsAndFreqs;
  private final DocsAndFreqs lead;
  private final float coord;

  ConjunctionScorer(Weight weight, Scorer[] scorers) {
    this(weight, scorers, 1f);
  }
  
  ConjunctionScorer(Weight weight, Scorer[] scorers, float coord) {
    super(weight);
    this.coord = coord;
    this.docsAndFreqs = new DocsAndFreqs[scorers.length];
    for (int i = 0; i < scorers.length; i++) {
      docsAndFreqs[i] = new DocsAndFreqs(scorers[i]);
    }
    // Sort the array the first time to allow the least frequent DocsEnum to
    // lead the matching.
    ArrayUtil.mergeSort(docsAndFreqs, new Comparator<DocsAndFreqs>() {
      @Override
      public int compare(DocsAndFreqs o1, DocsAndFreqs o2) {
        return Long.signum(o1.cost - o2.cost);
      }
    });

    lead = docsAndFreqs[0]; // least frequent DocsEnum leads the intersection
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
            docsAndFreqs[i].doc = docsAndFreqs[i].scorer.advance(doc);

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
      doc = lead.doc = lead.scorer.advance(doc);
    }
  }

  @Override
  public int advance(int target) throws IOException {
    lead.doc = lead.scorer.advance(target);
    return lastDoc = doNext(lead.doc);
  }

  @Override
  public int docID() {
    return lastDoc;
  }

  @Override
  public int nextDoc() throws IOException {
    lead.doc = lead.scorer.nextDoc();
    return lastDoc = doNext(lead.doc);
  }

  @Override
  public float score() throws IOException {
    // TODO: sum into a double and cast to float if we ever send required clauses to BS1
    float sum = 0.0f;
    for (DocsAndFreqs docs : docsAndFreqs) {
      sum += docs.scorer.score();
    }
    return sum * coord;
  }
  
  @Override
  public int freq() {
    return docsAndFreqs.length;
  }

  @Override
  public long cost() {
    return lead.scorer.cost();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<ChildScorer>(docsAndFreqs.length);
    for (DocsAndFreqs docs : docsAndFreqs) {
      children.add(new ChildScorer(docs.scorer, "MUST"));
    }
    return children;
  }

  static final class DocsAndFreqs {
    final long cost;
    final Scorer scorer;
    int doc = -1;
   
    DocsAndFreqs(Scorer scorer) {
      this.scorer = scorer;
      this.cost = scorer.cost();
    }
  }
}
