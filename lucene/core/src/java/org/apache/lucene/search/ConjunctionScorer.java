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
import java.util.List;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {

  private final ConjunctionDISI disi;
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
    this.disi = ConjunctionDISI.intersect(required);
    this.scorers = scorers.toArray(new Scorer[scorers.size()]);
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    return disi.asTwoPhaseIterator();
  }

  @Override
  public int advance(int target) throws IOException {
    return disi.advance(target);
  }

  @Override
  public int docID() {
    return disi.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return disi.nextDoc();
  }

  @Override
  public float score() throws IOException {
    double sum = 0.0d;
    for (Scorer scorer : scorers) {
      sum += scorer.score();
    }
    return coord * (float)sum;
  }

  @Override
  public int freq() {
    return scorers.length;
  }

  @Override
  public long cost() {
    return disi.cost();
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
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
