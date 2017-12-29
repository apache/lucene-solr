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

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestMaxScoreSumPropagator extends LuceneTestCase {

  private static class FakeScorer extends Scorer {
    
    final float maxScore;
    float minCompetitiveScore;
    
    FakeScorer(float maxScore) {
      super(null);
      this.maxScore = maxScore;
    }

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float score() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float maxScore() {
      return maxScore;
    }

    @Override
    public void setMinCompetitiveScore(float minCompetitiveScore) {
      this.minCompetitiveScore = minCompetitiveScore;
    }
  }

  public void test0Clause() {
    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Collections.emptyList());
    assertEquals(0f, p.maxScore(), 0f);
    p.setMinCompetitiveScore(0f); // no exception
    p.setMinCompetitiveScore(0.5f); // no exception
  }

  public void test1Clause() {
    FakeScorer a = new FakeScorer(1);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Collections.singletonList(a));
    assertEquals(1f, p.maxScore(), 0f);
    p.setMinCompetitiveScore(0f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    p.setMinCompetitiveScore(0.5f);
    assertEquals(0.5f, a.minCompetitiveScore, 0f);
    p.setMinCompetitiveScore(1f);
    assertEquals(1f, a.minCompetitiveScore, 0f);
  }

  public void test2Clauses() {
    FakeScorer a = new FakeScorer(1);
    FakeScorer b = new FakeScorer(2);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b));
    assertEquals(3f, p.maxScore(), 0f);

    p.setMinCompetitiveScore(1f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(0f, b.minCompetitiveScore, 0f);
    
    p.setMinCompetitiveScore(2f);
    assertEquals(0f, a.minCompetitiveScore, 0f);
    assertEquals(1f, b.minCompetitiveScore, 0f);
    
    p.setMinCompetitiveScore(2.5f);
    assertEquals(0.5f, a.minCompetitiveScore, 0f);
    assertEquals(1.5f, b.minCompetitiveScore, 0f);
    
    p.setMinCompetitiveScore(3f);
    assertEquals(1f, a.minCompetitiveScore, 0f);
    assertEquals(2f, b.minCompetitiveScore, 0f);
  }

  public void test3Clauses() {
    FakeScorer a = new FakeScorer(1);
    FakeScorer b = new FakeScorer(2);
    FakeScorer c = new FakeScorer(1.5f);

    MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b, c));
    assertEquals(4.5f, p.maxScore(), 0f);

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
    assertEquals(0.5f, b.minCompetitiveScore, 0f);
    assertEquals(0f, c.minCompetitiveScore, 0f);

    p.setMinCompetitiveScore(4f);
    assertEquals(0.5f, a.minCompetitiveScore, 0f);
    assertEquals(1.5f, b.minCompetitiveScore, 0f);
    assertEquals(1f, c.minCompetitiveScore, 0f);
  }

  public void test2ClausesRandomScore() {
    for (int iter = 0; iter < 10; ++iter) {
      FakeScorer a = new FakeScorer(random().nextFloat());
      FakeScorer b = new FakeScorer(Math.nextUp(a.maxScore()) + random().nextFloat());

      MaxScoreSumPropagator p = new MaxScoreSumPropagator(Arrays.asList(a, b));
      assertEquals(a.maxScore() + b.maxScore(), p.maxScore(), 0f);
      assertMinCompetitiveScore(Arrays.asList(a, b), p, Math.nextUp(a.maxScore()));
      assertMinCompetitiveScore(Arrays.asList(a, b), p, (a.maxScore() + b.maxScore()) / 2);
      assertMinCompetitiveScore(Arrays.asList(a, b), p, Math.nextDown(a.maxScore() + b.maxScore()));
      assertMinCompetitiveScore(Arrays.asList(a, b), p, a.maxScore() + b.maxScore());
    }
  }

  public void testNClausesRandomScore() {
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
      assertTrue(p.maxScore() >= (float) sumOfMaxScore);
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

  private void assertMinCompetitiveScore(Collection<FakeScorer> scorers, MaxScoreSumPropagator p, float minCompetitiveScore) {
    p.setMinCompetitiveScore(minCompetitiveScore);

    for (FakeScorer scorer : scorers) {
      if (scorer.minCompetitiveScore == 0f) {
        // no propagation is performed, still visiting all hits
        break;
      }
      double scoreSum = scorer.minCompetitiveScore;
      for (FakeScorer scorer2 : scorers) {
        if (scorer2 != scorer) {
          scoreSum += scorer2.maxScore();
        }
      }
      assertTrue(
          "scoreSum=" + scoreSum + ", minCompetitiveScore=" + minCompetitiveScore,
          (float) scoreSum <= minCompetitiveScore);
    }
  }

}
