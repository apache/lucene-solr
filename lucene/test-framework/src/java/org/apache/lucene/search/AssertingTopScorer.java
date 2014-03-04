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

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.util.VirtualMethod;

/** Wraps a Scorer with additional checks */
public class AssertingTopScorer extends TopScorer {

  private static final VirtualMethod<TopScorer> SCORE_COLLECTOR = new VirtualMethod<TopScorer>(TopScorer.class, "score", Collector.class);
  private static final VirtualMethod<TopScorer> SCORE_COLLECTOR_RANGE = new VirtualMethod<TopScorer>(TopScorer.class, "score", Collector.class, int.class);

  // we need to track scorers using a weak hash map because otherwise we
  // could loose references because of eg.
  // AssertingScorer.score(Collector) which needs to delegate to work correctly
  private static Map<TopScorer, WeakReference<AssertingTopScorer>> ASSERTING_INSTANCES = Collections.synchronizedMap(new WeakHashMap<TopScorer, WeakReference<AssertingTopScorer>>());

  public static TopScorer wrap(Random random, TopScorer other) {
    if (other == null || other instanceof AssertingTopScorer) {
      return other;
    }
    final AssertingTopScorer assertScorer = new AssertingTopScorer(random, other);
    ASSERTING_INSTANCES.put(other, new WeakReference<AssertingTopScorer>(assertScorer));
    return assertScorer;
  }

  public static boolean shouldWrap(TopScorer inScorer) {
    return SCORE_COLLECTOR.isOverriddenAsOf(inScorer.getClass()) || SCORE_COLLECTOR_RANGE.isOverriddenAsOf(inScorer.getClass());
  }

  final Random random;
  final TopScorer in;

  private AssertingTopScorer(Random random, TopScorer in) {
    this.random = random;
    this.in = in;
  }

  public TopScorer getIn() {
    return in;
  }

  @Override
  public void score(Collector collector) throws IOException {
    if (random.nextBoolean()) {
      try {
        final boolean remaining = in.score(collector, DocsEnum.NO_MORE_DOCS);
        assert !remaining;
      } catch (UnsupportedOperationException e) {
        in.score(collector);
      }
    } else {
      in.score(collector);
    }
  }

  @Override
  public boolean score(Collector collector, int max) throws IOException {
    return in.score(collector, max);
  }

  @Override
  public String toString() {
    return "AssertingTopScorer(" + in + ")";
  }
}
