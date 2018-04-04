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
package org.apache.lucene.search.intervals;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.CollectionUtil;

/**
 * A conjunction of DocIdSetIterators.
 * This iterates over the doc ids that are present in each given DocIdSetIterator.
 *
 * @lucene.internal
 */
final class ConjunctionDISI extends DocIdSetIterator {

  /**
   * Create a conjunction over the provided DocIdSetIterators.
   */
  public static DocIdSetIterator intersectIterators(List<? extends DocIdSetIterator> iterators) {
    if (iterators.size() < 2) {
      throw new IllegalArgumentException("Cannot make a ConjunctionDISI of less than 2 iterators");
    }
    final List<DocIdSetIterator> allIterators = new ArrayList<>();
    for (DocIdSetIterator iterator : iterators) {
      addIterator(iterator, allIterators);
    }

    return new ConjunctionDISI(allIterators);
  }

  private static void addIterator(DocIdSetIterator disi, List<DocIdSetIterator> allIterators) {
    if (disi.getClass() == ConjunctionDISI.class) { // Check for exactly this class for collapsing
      ConjunctionDISI conjunction = (ConjunctionDISI) disi;
      // subconjuctions have already split themselves into two phase iterators and others, so we can take those
      // iterators as they are and move them up to this conjunction
      allIterators.add(conjunction.lead1);
      allIterators.add(conjunction.lead2);
      Collections.addAll(allIterators, conjunction.others);
    } else {
      allIterators.add(disi);
    }
  }

  final DocIdSetIterator lead1, lead2;
  final DocIdSetIterator[] others;

  private ConjunctionDISI(List<? extends DocIdSetIterator> iterators) {
    assert iterators.size() >= 2;
    // Sort the array the first time to allow the least frequent DocsEnum to
    // lead the matching.
    CollectionUtil.timSort(iterators, Comparator.comparingLong(DocIdSetIterator::cost));
    lead1 = iterators.get(0);
    lead2 = iterators.get(1);
    others = iterators.subList(2, iterators.size()).toArray(new DocIdSetIterator[0]);
  }

  private int doNext(int doc) throws IOException {
    advanceHead:
    for (; ; ) {
      assert doc == lead1.docID();

      // find agreement between the two iterators with the lower costs
      // we special case them because they do not need the
      // 'other.docID() < doc' check that the 'others' iterators need
      final int next2 = lead2.advance(doc);
      if (next2 != doc) {
        doc = lead1.advance(next2);
        if (next2 != doc) {
          continue;
        }
      }

      // then find agreement with other iterators
      for (DocIdSetIterator other : others) {
        // other.doc may already be equal to doc if we "continued advanceHead"
        // on the previous iteration and the advance on the lead scorer exactly matched.
        if (other.docID() < doc) {
          final int next = other.advance(doc);

          if (next > doc) {
            // iterator beyond the current doc - advance lead and continue to the new highest doc.
            doc = lead1.advance(next);
            continue advanceHead;
          }
        }
      }

      // success - all iterators are on the same doc
      return doc;
    }
  }

  @Override
  public int advance(int target) throws IOException {
    return doNext(lead1.advance(target));
  }

  @Override
  public int docID() {
    return lead1.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return doNext(lead1.nextDoc());
  }

  @Override
  public long cost() {
    return lead1.cost(); // overestimate
  }

}
