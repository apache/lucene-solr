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
public class AssertingBulkScorer extends BulkScorer {

  private static final VirtualMethod<BulkScorer> SCORE_COLLECTOR = new VirtualMethod<>(BulkScorer.class, "score", Collector.class);
  private static final VirtualMethod<BulkScorer> SCORE_COLLECTOR_RANGE = new VirtualMethod<>(BulkScorer.class, "score", Collector.class, int.class);

  public static BulkScorer wrap(Random random, BulkScorer other) {
    if (other == null || other instanceof AssertingBulkScorer) {
      return other;
    }
    return new AssertingBulkScorer(random, other);
  }

  public static boolean shouldWrap(BulkScorer inScorer) {
    return SCORE_COLLECTOR.isOverriddenAsOf(inScorer.getClass()) || SCORE_COLLECTOR_RANGE.isOverriddenAsOf(inScorer.getClass());
  }

  final Random random;
  final BulkScorer in;

  private AssertingBulkScorer(Random random, BulkScorer in) {
    this.random = random;
    this.in = in;
  }

  public BulkScorer getIn() {
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
    return "AssertingBulkScorer(" + in + ")";
  }

}
