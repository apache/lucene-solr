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
import java.util.Random;

/** A crazy {@link BulkScorer} that wraps another {@link BulkScorer}
 *  but shuffles the order of the collected documents. */
public class AssertingBulkOutOfOrderScorer extends BulkScorer {

  final BulkScorer in;
  final Random random;

  public AssertingBulkOutOfOrderScorer(Random random, BulkScorer in) {
    this.in = in;
    this.random = random;
  }

  @Override
  public boolean score(Collector collector, int max) throws IOException {
    final RandomOrderCollector randomCollector = new RandomOrderCollector(random, collector);
    final boolean remaining = in.score(randomCollector, max);
    randomCollector.flush();
    return remaining;
  }

  @Override
  public void score(Collector collector) throws IOException {
    final RandomOrderCollector randomCollector = new RandomOrderCollector(random, collector);
    in.score(randomCollector);
    randomCollector.flush();
  }

  @Override
  public String toString() {
    return "AssertingBulkOutOfOrderScorer(" + in + ")";
  }
}
