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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;

public class TestDisjunctionScoreBlockBoundaryPropagator extends LuceneTestCase {

  private static class FakeWeight extends Weight {

    FakeWeight() {
      super(new MatchNoDocsQuery());
    }

    @Override
    public void extractTerms(Set<Term> terms) {}

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
      return null;
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      return null;
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }

  private static class FakeScorer extends Scorer {

    final int boundary;
    final float maxScore;

    FakeScorer(int boundary, float maxScore) throws IOException {
      super(new FakeWeight());
      this.boundary = boundary;
      this.maxScore = maxScore;
    }

    @Override
    public int docID() {
      return 0;
    }

    @Override
    public float score() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setMinCompetitiveScore(float minCompetitiveScore) {}

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return maxScore;
    }

    @Override
    public int advanceShallow(int target) {
      assert target <= boundary;
      return boundary;
    }
  }

  public void testBasics() throws IOException {
    Scorer scorer1 = new FakeScorer(20, 0.5f);
    Scorer scorer2 = new FakeScorer(50, 1.5f);
    Scorer scorer3 = new FakeScorer(30, 2f);
    Scorer scorer4 = new FakeScorer(80, 3f);
    List<Scorer> scorers = Arrays.asList(scorer1, scorer2, scorer3, scorer4);
    Collections.shuffle(scorers, random());
    DisjunctionScoreBlockBoundaryPropagator propagator = new DisjunctionScoreBlockBoundaryPropagator(scorers);
    assertEquals(20, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(0.2f);
    assertEquals(20, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(0.7f);
    assertEquals(30, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(1.2f);
    assertEquals(30, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(1.7f);
    assertEquals(30, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(2.2f);
    assertEquals(80, propagator.advanceShallow(0));

    propagator.setMinCompetitiveScore(5f);
    assertEquals(80, propagator.advanceShallow(0));
  }

}
