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
import java.util.Collection;
import java.util.Collections;

/** A Scorer for queries with a required subscorer
 * and an excluding (prohibited) sub {@link Scorer}.
 * <br>
 * This <code>Scorer</code> implements {@link Scorer#advance(int)},
 * and it uses the advance() on the given scorers.
 */
class ReqExclScorer extends Scorer {

  private final Scorer reqScorer;
  // approximations of the scorers, or the scorers themselves if they don't support approximations
  private final DocIdSetIterator reqApproximation;
  private final DocIdSetIterator exclApproximation;
  // two-phase views of the scorers, or null if they do not support approximations
  private final TwoPhaseIterator reqTwoPhaseIterator;
  private final TwoPhaseIterator exclTwoPhaseIterator;

  /** Construct a <code>ReqExclScorer</code>.
   * @param reqScorer The scorer that must match, except where
   * @param exclScorer indicates exclusion.
   */
  public ReqExclScorer(Scorer reqScorer, Scorer exclScorer) {
    super(reqScorer.weight);
    this.reqScorer = reqScorer;
    reqTwoPhaseIterator = reqScorer.asTwoPhaseIterator();
    if (reqTwoPhaseIterator == null) {
      reqApproximation = reqScorer;
    } else {
      reqApproximation = reqTwoPhaseIterator.approximation();
    }
    exclTwoPhaseIterator = exclScorer.asTwoPhaseIterator();
    if (exclTwoPhaseIterator == null) {
      exclApproximation = exclScorer;
    } else {
      exclApproximation = exclTwoPhaseIterator.approximation();
    }
  }

  @Override
  public int nextDoc() throws IOException {
    return toNonExcluded(reqApproximation.nextDoc());
  }

  /** Confirms whether or not the given {@link TwoPhaseIterator}
   *  matches on the current document. */
  private static boolean matches(TwoPhaseIterator it) throws IOException {
    return it == null || it.matches();
  }

  /** Confirm whether there is a match given the current positions of the
   *  req and excl approximations. This method has 2 important properties:
   *   - it only calls matches() on excl if the excl approximation is on
   *     the same doc ID as the req approximation
   *   - it does NOT call matches() on req if the excl approximation is exact
   *     and is on the same doc ID as the req approximation */
  private static boolean matches(int doc, int exclDoc,
      TwoPhaseIterator reqTwoPhaseIterator,
      TwoPhaseIterator exclTwoPhaseIterator) throws IOException {
    assert exclDoc >= doc;
    if (doc == exclDoc && matches(exclTwoPhaseIterator)) {
      return false;
    }
    return matches(reqTwoPhaseIterator);
  }

  /** Advance to the next non-excluded doc. */
  private int toNonExcluded(int doc) throws IOException {
    int exclDoc = exclApproximation.docID();
    for (;; doc = reqApproximation.nextDoc()) {
      if (doc == NO_MORE_DOCS) {
        return NO_MORE_DOCS;
      }
      if (exclDoc < doc) {
        exclDoc = exclApproximation.advance(doc);
      }
      if (matches(doc, exclDoc, reqTwoPhaseIterator, exclTwoPhaseIterator)) {
        return doc;
      }
    }
  }

  @Override
  public int docID() {
    return reqScorer.docID();
  }

  @Override
  public int freq() throws IOException {
    return reqScorer.freq();
  }

  @Override
  public long cost() {
    return reqScorer.cost();
  }

  @Override
  public float score() throws IOException {
    return reqScorer.score(); // reqScorer may be null when next() or skipTo() already return false
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    return Collections.singleton(new ChildScorer(reqScorer, "MUST"));
  }

  @Override
  public int advance(int target) throws IOException {
    return toNonExcluded(reqApproximation.advance(target));
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    if (reqTwoPhaseIterator == null) {
      return null;
    }
    return new TwoPhaseIterator(reqApproximation) {

      @Override
      public boolean matches() throws IOException {
        final int doc = reqApproximation.docID();
        // check if the doc is not excluded
        int exclDoc = exclApproximation.docID();
        if (exclDoc < doc) {
          exclDoc = exclApproximation.advance(doc);
        }
        return ReqExclScorer.matches(doc, exclDoc, reqTwoPhaseIterator, exclTwoPhaseIterator);
      }

    };
  }
}
