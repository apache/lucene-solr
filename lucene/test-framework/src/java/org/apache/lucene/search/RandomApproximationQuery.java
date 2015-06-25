package org.apache.lucene.search;

import java.io.IOException;
import java.util.Random;
import java.util.Set;

import com.carrotsearch.randomizedtesting.generators.RandomInts;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Bits;

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

/**
 * A {@link Query} that adds random approximations to its scorers.
 */
public class RandomApproximationQuery extends Query {

  private final Query query;
  private final Random random;

  public RandomApproximationQuery(Query query, Random random) {
    this.query = query;
    this.random = random;
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    final Query rewritten = query.rewrite(reader);
    if (rewritten != query) {
      return new RandomApproximationQuery(rewritten, random);
    }
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RandomApproximationQuery == false) {
      return false;
    }
    final RandomApproximationQuery that = (RandomApproximationQuery) obj;
    if (this.getBoost() != that.getBoost()) {
      return false;
    }
    if (this.query.equals(that.query) == false) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return 31 * query.hashCode() + Float.floatToIntBits(getBoost());
  }

  @Override
  public String toString(String field) {
    return query.toString(field);
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight weight = query.createWeight(searcher, needsScores);
    return new RandomApproximationWeight(weight, new Random(random.nextLong()));
  }

  private static class RandomApproximationWeight extends Weight {

    private final Weight weight;
    private final Random random;

    RandomApproximationWeight(Weight weight, Random random) {
      super(weight.getQuery());
      this.weight = weight;
      this.random = random;
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      weight.extractTerms(terms);
    }

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return weight.explain(context, doc);
    }

    @Override
    public float getValueForNormalization() throws IOException {
      return weight.getValueForNormalization();
    }

    @Override
    public void normalize(float norm, float topLevelBoost) {
      weight.normalize(norm, topLevelBoost);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      final Scorer scorer = weight.scorer(context);
      if (scorer == null) {
        return null;
      }
      return new RandomApproximationScorer(scorer, new Random(random.nextLong()));
    }

  }

  private static class RandomApproximationScorer extends Scorer {

    private final Scorer scorer;
    private final RandomTwoPhaseView twoPhaseView;

    RandomApproximationScorer(Scorer scorer, Random random) {
      super(scorer.getWeight());
      this.scorer = scorer;
      this.twoPhaseView = new RandomTwoPhaseView(random, scorer);
    }

    @Override
    public TwoPhaseIterator asTwoPhaseIterator() {
      return twoPhaseView;
    }

    @Override
    public float score() throws IOException {
      return scorer.score();
    }

    @Override
    public int freq() throws IOException {
      return scorer.freq();
    }

    @Override
    public int docID() {
      return scorer.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return scorer.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return scorer.advance(target);
    }

    @Override
    public long cost() {
      return scorer.cost();
    }

  }

  private static class RandomTwoPhaseView extends TwoPhaseIterator {

    private final DocIdSetIterator disi;
    private int lastDoc = -1;

    RandomTwoPhaseView(Random random, DocIdSetIterator disi) {
      super(new RandomApproximation(random, disi));
      this.disi = disi;
    }

    @Override
    public boolean matches() throws IOException {
      if (approximation.docID() == -1 || approximation.docID() == DocIdSetIterator.NO_MORE_DOCS) {
        throw new AssertionError("matches() should not be called on doc ID " + approximation.docID());
      }
      if (lastDoc == approximation.docID()) {
        throw new AssertionError("matches() has been called twice on doc ID " + approximation.docID());
      }
      lastDoc = approximation.docID();
      return approximation.docID() == disi.docID();
    }

  }

  private static class RandomApproximation extends DocIdSetIterator {

    private final Random random;
    private final DocIdSetIterator disi;

    int doc = -1;

    public RandomApproximation(Random random, DocIdSetIterator disi) {
      this.random = random;
      this.disi = disi;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (disi.docID() < target) {
        disi.advance(target);
      }
      if (disi.docID() == NO_MORE_DOCS) {
        return doc = NO_MORE_DOCS;
      }
      return doc = RandomInts.randomIntBetween(random, target, disi.docID());
    }

    @Override
    public long cost() {
      return disi.cost();
    }
  }

}
