package org.apache.lucene.spatial.composite;

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

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

/**
 * A constant-scoring {@link Scorer}.
 *
 * @lucene.internal
 */
public final class ConstantScoreScorer extends Scorer {
  // TODO refactor CSQ's Scorer to be re-usable and look like this

  private final Weight weight;
  private final float score;
  private final TwoPhaseIterator twoPhaseIterator;
  private final DocIdSetIterator disi;

  public ConstantScoreScorer(Weight weight, float score, DocIdSetIterator disi) {
    super(weight);
    this.weight = weight;
    this.score = score;
    this.twoPhaseIterator = null;
    this.disi = disi;
  }

  protected ConstantScoreScorer(Weight weight, float score, TwoPhaseIterator twoPhaseIterator) {
    super(weight);
    this.weight = weight;
    this.score = score;
    this.twoPhaseIterator = twoPhaseIterator;
    this.disi = TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator);
  }

  @Override
  public TwoPhaseIterator asTwoPhaseIterator() {
    return twoPhaseIterator;
  }

  @Override
  public float score() throws IOException {
    return score;
  }

  @Override
  public int freq() throws IOException {
    return 1;
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
  public int advance(int target) throws IOException {
    return disi.advance(target);
  }

  @Override
  public long cost() {
    return disi.cost();
  }
}
