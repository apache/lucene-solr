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
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.WeakHashMap;

import org.apache.lucene.index.AssertingAtomicReader;

/** Wraps a Scorer with additional checks */
public class AssertingScorer extends Scorer {

  // we need to track scorers using a weak hash map because otherwise we
  // could loose references because of eg.
  // AssertingScorer.score(Collector) which needs to delegate to work correctly
  private static Map<Scorer, WeakReference<AssertingScorer>> ASSERTING_INSTANCES = Collections.synchronizedMap(new WeakHashMap<Scorer, WeakReference<AssertingScorer>>());

  public static Scorer wrap(Random random, Scorer other) {
    if (other == null || other instanceof AssertingScorer) {
      return other;
    }
    final AssertingScorer assertScorer = new AssertingScorer(random, other);
    ASSERTING_INSTANCES.put(other, new WeakReference<>(assertScorer));
    return assertScorer;
  }

  static Scorer getAssertingScorer(Random random, Scorer other) {
    if (other == null || other instanceof AssertingScorer) {
      return other;
    }
    final WeakReference<AssertingScorer> assertingScorerRef = ASSERTING_INSTANCES.get(other);
    final AssertingScorer assertingScorer = assertingScorerRef == null ? null : assertingScorerRef.get();
    if (assertingScorer == null) {
      // can happen in case of memory pressure or if
      // scorer1.score(collector) calls
      // collector.setScorer(scorer2) with scorer1 != scorer2, such as
      // BooleanScorer. In that case we can't enable all assertions
      return new AssertingScorer(random, other);
    } else {
      return assertingScorer;
    }
  }

  final Random random;
  final Scorer in;
  final AssertingAtomicReader.AssertingDocsEnum docsEnumIn;

  private AssertingScorer(Random random, Scorer in) {
    super(in.weight);
    this.random = random;
    this.in = in;
    this.docsEnumIn = new AssertingAtomicReader.AssertingDocsEnum(in);
  }

  public Scorer getIn() {
    return in;
  }

  boolean iterating() {
    switch (docID()) {
    case -1:
    case NO_MORE_DOCS:
      return false;
    default:
      return true;
    }
  }

  @Override
  public float score() throws IOException {
    assert iterating();
    final float score = in.score();
    assert !Float.isNaN(score);
    assert !Float.isNaN(score);
    return score;
  }

  @Override
  public Collection<ChildScorer> getChildren() {
    // We cannot hide that we hold a single child, else
    // collectors (e.g. ToParentBlockJoinCollector) that
    // need to walk the scorer tree will miss/skip the
    // Scorer we wrap:
    return Collections.singletonList(new ChildScorer(in, "SHOULD"));
  }

  @Override
  public int freq() throws IOException {
    assert iterating();
    return in.freq();
  }

  @Override
  public int docID() {
    return in.docID();
  }

  @Override
  public int nextDoc() throws IOException {
    return docsEnumIn.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    return docsEnumIn.advance(target);
  }

  @Override
  public long cost() {
    return in.cost();
  }

  @Override
  public String toString() {
    return "AssertingScorer(" + in + ")";
  }
}

