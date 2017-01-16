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

import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

public class TestBoolean2ScorerSupplier extends LuceneTestCase {

  private static class FakeScorer extends Scorer {

    private final DocIdSetIterator it;

    FakeScorer(long cost) {
      super(null);
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
    public int freq() throws IOException {
      return 1;
    }

    @Override
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
    private final Boolean randomAccess;

    FakeScorerSupplier(long cost) {
      this.cost = cost;
      this.randomAccess = null;
    }

    FakeScorerSupplier(long cost, boolean randomAccess) {
      this.cost = cost;
      this.randomAccess = randomAccess;
    }

    @Override
    public Scorer get(boolean randomAccess) throws IOException {
      if (this.randomAccess != null) {
        assertEquals(this.toString(), this.randomAccess, randomAccess);
      }
      return new FakeScorer(cost);
    }

    @Override
    public long cost() {
      return cost;
    }
    
    @Override
    public String toString() {
      return "FakeLazyScorer(cost=" + cost + ",randomAccess=" + randomAccess + ")";
    }

  }

  private static Boolean2ScorerSupplier scorerSupplier(Map<Occur, Collection<ScorerSupplier>> subs,
      boolean needsScores, int minShouldMatch) {
    int maxCoord = subs.get(Occur.SHOULD).size() + subs.get(Occur.MUST).size();
    float[] coords = new float[maxCoord];
    Arrays.fill(coords, 1f);
    return new Boolean2ScorerSupplier(null, subs, true, coords, maxCoord, needsScores, minShouldMatch);
  }

  public void testConjunctionCost() {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42));
    assertEquals(42, scorerSupplier(subs, random().nextBoolean(), 0).cost());

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12));
    assertEquals(12, scorerSupplier(subs, random().nextBoolean(), 0).cost());

    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(20));
    assertEquals(12, scorerSupplier(subs, random().nextBoolean(), 0).cost());
  }

  public void testDisjunctionCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42));
    ScorerSupplier s = scorerSupplier(subs, random().nextBoolean(), 0);
    assertEquals(42, s.cost());
    assertEquals(42, s.get(random().nextBoolean()).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12));
    s = scorerSupplier(subs, random().nextBoolean(), 0);
    assertEquals(42 + 12, s.cost());
    assertEquals(42 + 12, s.get(random().nextBoolean()).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20));
    s = scorerSupplier(subs, random().nextBoolean(), 0);
    assertEquals(42 + 12 + 20, s.cost());
    assertEquals(42 + 12 + 20, s.get(random().nextBoolean()).iterator().cost());
  }

  public void testDisjunctionWithMinShouldMatchCost() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12));
    ScorerSupplier s = scorerSupplier(subs, random().nextBoolean(), 1);
    assertEquals(42 + 12, s.cost());
    assertEquals(42 + 12, s.get(random().nextBoolean()).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20));
    s = scorerSupplier(subs, random().nextBoolean(), 1);
    assertEquals(42 + 12 + 20, s.cost());
    assertEquals(42 + 12 + 20, s.get(random().nextBoolean()).iterator().cost());
    s = scorerSupplier(subs, random().nextBoolean(), 2);
    assertEquals(12 + 20, s.cost());
    assertEquals(12 + 20, s.get(random().nextBoolean()).iterator().cost());

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30));
    s = scorerSupplier(subs, random().nextBoolean(), 1);
    assertEquals(42 + 12 + 20 + 30, s.cost());
    assertEquals(42 + 12 + 20 + 30, s.get(random().nextBoolean()).iterator().cost());
    s = scorerSupplier(subs, random().nextBoolean(), 2);
    assertEquals(12 + 20 + 30, s.cost());
    assertEquals(12 + 20 + 30, s.get(random().nextBoolean()).iterator().cost());
    s = scorerSupplier(subs, random().nextBoolean(), 3);
    assertEquals(12 + 20, s.cost());
    assertEquals(12 + 20, s.get(random().nextBoolean()).iterator().cost());
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
      boolean needsScores = random().nextBoolean();
      if (needsScores == false && numRequired > 0) {
        numClauses -= numShoulds;
        numShoulds = 0;
        subs.get(Occur.SHOULD).clear();
      }
      if (numShoulds + numRequired == 0) {
        // only negative clauses, invalid
        continue;
      }
      int minShouldMatch = numShoulds == 0 ? 0 : TestUtil.nextInt(random(), 0, numShoulds - 1);
      Boolean2ScorerSupplier supplier = scorerSupplier(subs, needsScores, minShouldMatch);
      long cost1 = supplier.cost();
      long cost2 = supplier.get(false).iterator().cost();
      assertEquals("clauses=" + subs + ", minShouldMatch=" + minShouldMatch, cost1, cost2);
    }
  }

  // test the tester...
  public void testFakeScorerSupplier() {
    FakeScorerSupplier randomAccessSupplier = new FakeScorerSupplier(random().nextInt(100), true);
    expectThrows(AssertionError.class, () -> randomAccessSupplier.get(false));
    FakeScorerSupplier sequentialSupplier = new FakeScorerSupplier(random().nextInt(100), false);
    expectThrows(AssertionError.class, () -> sequentialSupplier.get(true));
  }

  public void testConjunctionRandomAccess() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // If sequential access is required, only the least costly clause does not use random-access
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42, true));
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12, false));
    scorerSupplier(subs, random().nextBoolean(), 0).get(false); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // If random access is required, then we propagate to sub clauses
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(42, true));
    subs.get(RandomPicks.randomFrom(random(), Arrays.asList(Occur.FILTER, Occur.MUST))).add(new FakeScorerSupplier(12, true));
    scorerSupplier(subs, random().nextBoolean(), 0).get(true); // triggers assertions as a side-effect
  }

  public void testDisjunctionRandomAccess() throws IOException {
    // disjunctions propagate
    for (boolean randomAccess : new boolean[] {false, true}) {
      Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
      for (Occur occur : Occur.values()) {
        subs.put(occur, new ArrayList<>());
      }
      subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, randomAccess));
      subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, randomAccess));
      scorerSupplier(subs, random().nextBoolean(), 0).get(randomAccess); // triggers assertions as a side-effect
    }
  }

  public void testDisjunctionWithMinShouldMatchRandomAccess() throws IOException {
    Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // Only the most costly clause uses random-access in that case:
    // most of time, we will find agreement between the 2 least costly
    // clauses and only then check whether the 3rd one matches too
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, false));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, false));
    scorerSupplier(subs, random().nextBoolean(), 2).get(false); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    // When random-access is true, just propagate
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, true));
    scorerSupplier(subs, random().nextBoolean(), 2).get(true); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, false));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, false));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20, false));
    scorerSupplier(subs, random().nextBoolean(), 2).get(false); // triggers assertions as a side-effect

    subs = new EnumMap<>(Occur.class);
    for (Occur occur : Occur.values()) {
      subs.put(occur, new ArrayList<>());
    }

    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(42, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(12, false));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(30, true));
    subs.get(Occur.SHOULD).add(new FakeScorerSupplier(20, false));
    scorerSupplier(subs, random().nextBoolean(), 3).get(false); // triggers assertions as a side-effect
  }

  public void testProhibitedRandomAccess() throws IOException {
    for (boolean randomAccess : new boolean[] {false, true}) {
      Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
      for (Occur occur : Occur.values()) {
        subs.put(occur, new ArrayList<>());
      }

      // The MUST_NOT clause always uses random-access
      subs.get(Occur.MUST).add(new FakeScorerSupplier(42, randomAccess));
      subs.get(Occur.MUST_NOT).add(new FakeScorerSupplier(TestUtil.nextInt(random(), 1, 100), true));
      scorerSupplier(subs, random().nextBoolean(), 0).get(randomAccess); // triggers assertions as a side-effect
    }
  }

  public void testMixedRandomAccess() throws IOException {
    for (boolean randomAccess : new boolean[] {false, true}) {
      Map<Occur, Collection<ScorerSupplier>> subs = new EnumMap<>(Occur.class);
      for (Occur occur : Occur.values()) {
        subs.put(occur, new ArrayList<>());
      }

      // The SHOULD clause always uses random-access if there is a MUST clause
      subs.get(Occur.MUST).add(new FakeScorerSupplier(42, randomAccess));
      subs.get(Occur.SHOULD).add(new FakeScorerSupplier(TestUtil.nextInt(random(), 1, 100), true));
      scorerSupplier(subs, true, 0).get(randomAccess); // triggers assertions as a side-effect
    }
  }

}
