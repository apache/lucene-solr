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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class TestMaxScoreSumPropagator extends LuceneTestCase {

  private static class FakeWeight extends Weight {

    FakeWeight() {
      super(new MatchNoDocsQuery());
    }

    @Override
    public void extractTerms(Set<Term> terms) {

    }

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

    final float maxScore;
    float minCompetitiveScore;

    FakeScorer(float maxScore) throws IOException {
      super(new FakeWeight());
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
    public void setMinCompetitiveScore(float minCompetitiveScore) {
      this.minCompetitiveScore = minCompetitiveScore;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      assert upTo == NO_MORE_DOCS;
      return maxScore;
    }
  }

  public void test0Clause() throws IOException {
    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Collections.emptyList());
    p.setMinCompetitiveScore(0f); // no exception
    p.setMinCompetitiveScore(0.5f); // no exception
  }

  public void test1Clause() throws IOException {
    FakeScorer a = new FakeScorer(1);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Collections.singletonList(a));
    assertEquals(1f, p.getMaxScore(NO_MORE_DOCS), 0f);
    p.setMinCompetitiveScore(0f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    p.setMinCompetitiveScore(0.5f);
    assertEquals(Math.nextDown(0.5f), a.minCompetitiveScore, 0f);
    p.setMinCompetitiveScore(1f);
    assertEquals(Math.nextDown(1f), a.minCompetitiveScore, 0f);
  }

  public void test2Clauses() throws IOException {
    FakeScorer a = new FakeScorer(1);
    FakeScorer b = new FakeScorer(2);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b));
    assertEquals(3f, p.getMaxScore(NO_MORE_DOCS), 0f);

    p.setMinCompetitiveScore(1f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(0f, b.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(2f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(2f) - 1f, b.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(2.5f);
    assertEquals(Math.nextDown(2.5f) - 2f, a.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(2.5f) - 1f, b.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(3f);
    assertEquals(Math.nextDown(3f) - 2f, a.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(3f) - 1f, b.minCompetitiveScore, 0f);
  }

  public void test3Clauses() throws IOException {
    FakeScorer a = new FakeScorer(1);
    FakeScorer b = new FakeScorer(2);
    FakeScorer c = new FakeScorer(1.5f);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b, c));
    assertEquals(4.5f, p.getMaxScore(NO_MORE_DOCS), 0f);

    p.setMinCompetitiveScore(1f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(0f, b.minCompetitiveScore, 0f);
    assertEquals(0f, c.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(2f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(0f, b.minCompetitiveScore, 0f);
    assertEquals(0f, c.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(3f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(3f) - 1f - 1.5f, b.minCompetitiveScore, 0f);
    assertEquals(0f, c.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(4f);
    assertEquals(Math.nextDown(4f) - 2f - 1.5f, a.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(4f) - 1f - 1.5f, b.minCompetitiveScore, 0f);
    assertEquals(Math.nextDown(4f) - 1f - 2f, c.minCompetitiveScore, 0f);
  }

  public void test2ClausesRandomScore() throws IOException {
    for (int iter = 0; iter < 10; ++iter) {
      FakeScorer a = new FakeScorer(random().nextFloat());
      FakeScorer b = new FakeScorer(Math.nextUp(a.getMaxScore(NO_MORE_DOCS)) + random().nextFloat());

      MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b));
      assertEquals(a.getMaxScore(NO_MORE_DOCS) + b.getMaxScore(NO_MORE_DOCS), p.getMaxScore(NO_MORE_DOCS), 0f);
      assertMinCompetitiveScore(Arrays.asList(a, b), p, Math.nextUp(a.getMaxScore(NO_MORE_DOCS)));
      assertMinCompetitiveScore(Arrays.asList(a, b), p, (a.getMaxScore(NO_MORE_DOCS) + b.getMaxScore(NO_MORE_DOCS)) / 2);
      assertMinCompetitiveScore(Arrays.asList(a, b), p, Math.nextDown(a.getMaxScore(NO_MORE_DOCS) + b.getMaxScore(NO_MORE_DOCS)));
      assertMinCompetitiveScore(Arrays.asList(a, b), p, a.getMaxScore(NO_MORE_DOCS) + b.getMaxScore(NO_MORE_DOCS));
    }
  }

  public void testNClausesRandomScore() throws IOException {
    for (int iter = 0; iter < 100; ++iter) {
      List<FakeScorer> scorers = new ArrayList<>();
      int numScorers = TestUtil.nextInt(random(), 3, 4 << random().nextInt(8));
      double sumOfMaxScore = 0;
      for (int i = 0; i < numScorers; ++i) {
        float maxScore = random().nextFloat();
        scorers.add(new FakeScorer(maxScore));
        sumOfMaxScore += maxScore;
      }

      MaxScoreSumPropagator p = new MaxScoreSumPropagator(scorers);
      assertTrue(p.getMaxScore(NO_MORE_DOCS)  >= (float) sumOfMaxScore);
      for (int i = 0; i < 10; ++i) {
        final float minCompetitiveScore = random().nextFloat() * numScorers;
        assertMinCompetitiveScore(scorers, p, minCompetitiveScore);
        // reset
        for (FakeScorer scorer : scorers) {
          scorer.minCompetitiveScore = 0;
        }
      }
    }
  }

  private void assertMinCompetitiveScore(Collection<FakeScorer> scorers, MaxScoreSumPropagator p, float minCompetitiveScore) throws IOException {
    p.setMinCompetitiveScore(minCompetitiveScore);

    for (FakeScorer scorer : scorers) {
      if (scorer.minCompetitiveScore == 0f) {
        // no propagation is performed, still visiting all hits
        break;
      }
      double scoreSum = scorer.minCompetitiveScore;
      for (FakeScorer scorer2 : scorers) {
        if (scorer2 != scorer) {
          scoreSum += scorer2.getMaxScore(NO_MORE_DOCS);
        }
      }
      assertTrue(
          "scoreSum=" + scoreSum + ", minCompetitiveScore=" + minCompetitiveScore,
          (float) scoreSum <= minCompetitiveScore);
    }
  }
}

