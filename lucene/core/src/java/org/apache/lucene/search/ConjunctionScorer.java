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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/** Scorer for conjunctions, sets of queries, all of which are required. */
class ConjunctionScorer extends Scorer {

  final DocIdSetIterator disi;
  final Scorer[] scorers;
  final Collection<Scorer> required;

  /** Create a new {@link ConjunctionScorer}, note that {@code scorers} must be a subset of {@code required}. */
  ConjunctionScorer(Weight weight, Collection<Scorer> required, Collection<Scorer> scorers) {
    super(weight);
    assert required.containsAll(scorers);
    this.disi = ConjunctionDISI.intersectScorers(required);
    this.scorers = scorers.toArray(new Scorer[scorers.size()]);
    this.required = required;
  }

  @Override
  public TwoPhaseIterator twoPhaseIterator() {
    return TwoPhaseIterator.unwrap(disi);
  }

  @Override
  public DocIdSetIterator iterator() {
    return disi;
  }

  @Override
  public int docID() {
    return disi.docID();
  }

  @Override
  public float score() throws IOException {
    double sum = 0.0d;
    for (Scorer scorer : scorers) {
      sum += scorer.score();
    }
    return (float) sum;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    ArrayList<ChildScorer> children = new ArrayList<>();
    for (Scorer scorer : required) {
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
