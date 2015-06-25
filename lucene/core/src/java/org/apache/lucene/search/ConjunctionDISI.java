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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.util.CollectionUtil;

/** A conjunction of DocIdSetIterators.
 * This iterates over the doc ids that are present in each given DocIdSetIterator.
 * <br>Public only for use in {@link org.apache.lucene.search.spans}.
 * @lucene.internal
 */
public class ConjunctionDISI extends DocIdSetIterator {

  /** Create a conjunction over the provided iterators, taking advantage of
   *  {@link TwoPhaseIterator}. */
  public static ConjunctionDISI intersect(List<? extends DocIdSetIterator> iterators) {
    if (iterators.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    final List<TwoPhaseIterator> twoPhaseIterators = new ArrayList<>();
    for (DocIdSetIterator iter : iterators) {
      addIterator(iter, allIterators, twoPhaseIterators);
    }

    if (twoPhaseIterators.isEmpty()) {
      return new ConjunctionDISI(allIterators);
    } else {
      return new TwoPhase(allIterators, twoPhaseIterators);
    }
  }

  /** Adds the iterator, possibly splitting up into two phases or collapsing if it is another conjunction */
  private static void addIterator(DocIdSetIterator disi, List<DocIdSetIterator> allIterators, List<TwoPhaseIterator> twoPhaseIterators) {
    // Check for exactly this class for collapsing. Subclasses can do their own optimizations.
    if (disi.getClass() == ConjunctionDISI.class || disi.getClass() == TwoPhase.class) {
      ConjunctionDISI conjunction = (ConjunctionDISI) disi;
      // subconjuctions have already split themselves into two phase iterators and others, so we can take those
      // iterators as they are and move them up to this conjunction
      allIterators.add(conjunction.lead);
      Collections.addAll(allIterators, conjunction.others);
      if (conjunction.getClass() == TwoPhase.class) {
        TwoPhase twoPhase = (TwoPhase) conjunction;
        Collections.addAll(twoPhaseIterators, twoPhase.twoPhaseView.twoPhaseIterators);
      }
    } else {
      TwoPhaseIterator twoPhaseIter = TwoPhaseIterator.asTwoPhaseIterator(disi);
      if (twoPhaseIter != null) {
        allIterators.add(twoPhaseIter.approximation());
        twoPhaseIterators.add(twoPhaseIter);
      } else { // no approximation support, use the iterator as-is
        allIterators.add(disi);
      }
    }
  }

  final DocIdSetIterator lead;
  final DocIdSetIterator[] others;

  ConjunctionDISI(List<? extends DocIdSetIterator> iterators) {
    assert iterators.size() >= 2;
    // Sort the array the first time to allow the least frequent DocsEnum to
    // lead the matching.
    CollectionUtil.timSort(iterators, new Comparator<DocIdSetIterator>() {
      @Override
      public int compare(DocIdSetIterator o1, DocIdSetIterator o2) {
        return Long.compare(o1.cost(), o2.cost());
      }
    });
    lead = iterators.get(0);
    others = iterators.subList(1, iterators.size()).toArray(new DocIdSetIterator[0]);
  }

  protected boolean matches() throws IOException {
    return true;
  }

  TwoPhaseIterator asTwoPhaseIterator() {
    return null;
  }

  private int doNext(int doc) throws IOException {
    for(;;) {

      if (doc == NO_MORE_DOCS) {
        // we need this check because it is only ok to call #matches when positioned
        return NO_MORE_DOCS;
      }

      advanceHead: for(;;) {
        for (DocIdSetIterator other : others) {
          // invariant: docsAndFreqs[i].doc <= doc at this point.

          // docsAndFreqs[i].doc may already be equal to doc if we "broke advanceHead"
          // on the previous iteration and the advance on the lead scorer exactly matched.
          if (other.docID() < doc) {
            final int next = other.advance(doc);

            if (next > doc) {
              // DocsEnum beyond the current doc - break and advance lead to the new highest doc.
              doc = lead.advance(next);
              break advanceHead;
            }
          }
        }

        if (matches()) {
          // success - all DocsEnums are on the same doc
          return doc;
        } else {
          doc = lead.nextDoc();
          break advanceHead;
        }
      }
    }
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(lead.advance(target));
  }

  @Override
  public int docID() {
    return lead.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return doNext(lead.nextDoc());
  }

  @Override
  public long cost() {
    return lead.cost();
  }

  /**
   * {@link TwoPhaseIterator} view of a {@link TwoPhase} conjunction.
   */
  private static class TwoPhaseConjunctionDISI extends TwoPhaseIterator {

    private final TwoPhaseIterator[] twoPhaseIterators;

    private TwoPhaseConjunctionDISI(List<? extends DocIdSetIterator> iterators, List<TwoPhaseIterator> twoPhaseIterators) {
      super(new ConjunctionDISI(iterators));
      assert twoPhaseIterators.size() > 0;
      this.twoPhaseIterators = twoPhaseIterators.toArray(new TwoPhaseIterator[twoPhaseIterators.size()]);
    }

    @Override
    public boolean matches() throws IOException {
      for (TwoPhaseIterator twoPhaseIterator : twoPhaseIterators) {
        if (twoPhaseIterator.matches() == false) {
          return false;
        }
      }
      return true;
    }

  }

  /**
   * A conjunction DISI built on top of approximations. This implementation
   * verifies that documents actually match by consulting the provided
   * {@link TwoPhaseIterator}s.
   *
   * Another important difference with {@link ConjunctionDISI} is that this
   * implementation supports approximations too: the approximation of this
   * impl is the conjunction of the approximations of the wrapped iterators.
   * This allows eg. {@code +"A B" +C} to be approximated as
   * {@code +(+A +B) +C}.
   */
  // NOTE: this is essentially the same as TwoPhaseDocIdSetIterator.asDocIdSetIterator
  // but is its own impl in order to be able to expose a two-phase view
  private static class TwoPhase extends ConjunctionDISI {

    final TwoPhaseConjunctionDISI twoPhaseView;

    private TwoPhase(List<? extends DocIdSetIterator> iterators, List<TwoPhaseIterator> twoPhaseIterators) {
      super(iterators);
      twoPhaseView = new TwoPhaseConjunctionDISI(iterators, twoPhaseIterators);
    }

    @Override
    public TwoPhaseConjunctionDISI asTwoPhaseIterator() {
      return twoPhaseView;
    }

    @Override
    protected boolean matches() throws IOException {
      return twoPhaseView.matches();
    }
  }

}
