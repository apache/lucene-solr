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
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

public class TestBoolean2ScorerSupplier extends LuceneTestCase {

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

    private final DocIdSetIterator it;

    FakeScorer(long cost) {
      super(new FakeWeight());
      this.it = DocIdSetIterator.all(Math.toIntExact(cost));
    }

    @Override
    public int docID() {
      return it.docID();
    }

    @Override
    public float score() throws IOException {
      return 1;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return 1;
    }

    public DocIdSetIterator iterator() {
      return it;
    }

    @Override
    public String toString() {
      return "FakeScorer(cost=" + it.cost() + ")";
    }

  }

  private static class FakeScorerSupplier extends ScorerSupplier {

    private final long cost;
    private final Long leadCost;

    FakeScorerSupplier(long cost) {
      this.cost = cost;
      this.leadCost = null;
    }

    FakeScorerSupplier(long cost, long leadCost) {
      this.cost = cost;
      this.leadCost = leadCost;
    }

    @Override
    public Scorer get(long leadCost) throws IOException {
      if (this.leadCost != null) {
        assertEquals(this.toString(), this.leadCost.longValue(), leadCost);
      }
      return new FakeScorer(cost);
    }

    @Override
    public long cost() {
      return cost;
    }
    
    @Override
    public String toString() {
      return "FakeLazyScorer(cost=" + cost + ",leadCost=" + leadCost + ")";
    }

  }

  public void testConjunctionCost() {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42));
    assertEquals(42, new Boolean2ScorerSupplier(null, subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).cost());

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12));
    assertEquals(12, new Boolean2ScorerSupplier(null, subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).cost());

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(20));
    assertEquals(12, new Boolean2ScorerSupplier(null, subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).cost());
  }

  public void testDisjunctionCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42));
    ScorerSupplier s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0);
    assertEquals(42, s.cost());
    assertEquals(42, s.get(random().nextInt(100)).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12));
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0);
    assertEquals(42 + 12, s.cost());
    assertEquals(42 + 12, s.get(random().nextInt(100)).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20));
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0);
    assertEquals(42 + 12 + 20, s.cost());
    assertEquals(42 + 12 + 20, s.get(random().nextInt(100)).iterator().cost());
  }

  public void testDisjunctionWithMinShouldMatchCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12));
    ScorerSupplier s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 1);
    assertEquals(42 + 12, s.cost());
    assertEquals(42 + 12, s.get(random().nextInt(100)).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20));
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 1);
    assertEquals(42 + 12 + 20, s.cost());
    assertEquals(42 + 12 + 20, s.get(random().nextInt(100)).iterator().cost());
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 2);
    assertEquals(12 + 20, s.cost());
    assertEquals(12 + 20, s.get(random().nextInt(100)).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30));
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 1);
    assertEquals(42 + 12 + 20 + 30, s.cost());
    assertEquals(42 + 12 + 20 + 30, s.get(random().nextInt(100)).iterator().cost());
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 2);
    assertEquals(12 + 20 + 30, s.cost());
    assertEquals(12 + 20 + 30, s.get(random().nextInt(100)).iterator().cost());
    s = new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 3);
    assertEquals(12 + 20, s.cost());
    assertEquals(12 + 20, s.get(random().nextInt(100)).iterator().cost());
  }

  public void testDuelCost() throws Exception {
    final int iters = atLeast(1000);
    for (int iter = 0; iter < iters; ++iter) {
      Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
      for (Occur occur : Occur.values()) {
        subs.put(occur, new ArrayList<>());
      }
      int numClauses = TestUtil.nextInt(random(), 1, 10);
      int numShoulds = 0;
      int numRequired = 0;
      for (int j = 0; j < numClauses; ++j) {
        Occur occur = RandomPicks.randomFrom(random(), Occur.values());
        subs.get(occur).add(new FakeScorerSupplier(random().nextInt(100)));
        if (occur == Occur.SHOULD) {
          ++numShoulds;
        } else if (occur == Occur.FILTER || occur == Occur.MUST) {
          numRequired++;
        }
      }
      ScoreMode scoreMode = RandomPicks.randomFrom(random(), ScoreMode.values());
      if (scoreMode.needsScores() == false && numRequired > 0) {
        numClauses -= numShoulds;
        numShoulds = 0;
        subs.get(Occur.SHOULD).clear();
      }
      if (numShoulds + numRequired == 0) {
        // only negative clauses, invalid
        continue;
      }
      int minShouldMatch = numShoulds == 0 ? 0 : TestUtil.nextInt(random(), 0, numShoulds - 1);
      Boolean2ScorerSupplier supplier = new Boolean2ScorerSupplier(new FakeWeight(),
          subs, scoreMode, minShouldMatch);
      long cost1 = supplier.cost();
      long cost2 = supplier.get(Long.MAX_VALUE).iterator().cost();
      assertEquals("clauses=" + subs + ", minShouldMatch=" + minShouldMatch, cost1, cost2);
    }
  }

  // test the tester...
  public void testFakeScorerSupplier() {
    FakeScorerSupplier randomAccessSupplier = new FakeScorerSupplier(random().nextInt(100), 30);
    expectThrows(AssertionError.class, () -> randomAccessSupplier.get(70));
    FakeScorerSupplier sequentialSupplier = new FakeScorerSupplier(random().nextInt(100), 70);
    expectThrows(AssertionError.class, () -> sequentialSupplier.get(30));
  }

  public void testConjunctionLeadCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // If the clauses are less costly than the lead cost, the min cost is the new lead cost
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42, 12));
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12, 12));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(Long.MAX_VALUE); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // If the lead cost is less that the clauses' cost, then we don't modify it
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42, 7));
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12, 7));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(7); // triggers assertions as a side-effect
  }

  public void testDisjunctionLeadCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, 54));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 54));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(100); // triggers assertions as a side-effect

    subs.get(Occur.SHOULD).clear();
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, 20));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 20));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(20); // triggers assertions as a side-effect
  }

  public void testDisjunctionWithMinShouldMatchLeadCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // minShouldMatch is 2 so the 2 least costly clauses will lead iteration
    // and their cost will be 30+12=42
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(50, 42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, 42));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 2).get(100); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // If the leadCost is less than the msm cost, then it wins
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, 20));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 20));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, 20));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 2).get(20); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, 62));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 62));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, 62));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20, 62));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 2).get(100); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, 32));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, 32));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, 32));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20, 32));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 3).get(100); // triggers assertions as a side-effect
  }

  public void testProhibitedLeadCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // The MUST_NOT clause is called with the same lead cost as the MUST clause
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 42));
    subs.get(Occur.MUST_NOT).add(new FakeScorerSupplier(30, 42));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(100); // triggers assertions as a side-effect

    subs.get(Occur.MUST).clear();
    subs.get(Occur.MUST_NOT).clear();
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 42));
    subs.get(Occur.MUST_NOT).add(new FakeScorerSupplier(80, 42));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(100); // triggers assertions as a side-effect

    subs.get(Occur.MUST).clear();
    subs.get(Occur.MUST_NOT).clear();
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 20));
    subs.get(Occur.MUST_NOT).add(new FakeScorerSupplier(30, 20));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, RandomPicks.randomFrom(random(), ScoreMode.values()), 0).get(20); // triggers assertions as a side-effect
  }

  public void testMixedLeadCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // The SHOULD clause is always called with the same lead cost as the MUST clause
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, 42));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, ScoreMode.COMPLETE, 0).get(100); // triggers assertions as a side-effect

    subs.get(Occur.MUST).clear();
    subs.get(Occur.SHOULD).clear();
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(80, 42));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, ScoreMode.COMPLETE, 0).get(100); // triggers assertions as a side-effect

    subs.get(Occur.MUST).clear();
    subs.get(Occur.SHOULD).clear();
    subs.get(Occur.MUST).add(new FakeScorerSupplier(42, 20));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(80, 20));
    new Boolean2ScorerSupplier(new FakeWeight(), subs, ScoreMode.COMPLETE, 0).get(20); // triggers assertions as a side-effect
  }

}
