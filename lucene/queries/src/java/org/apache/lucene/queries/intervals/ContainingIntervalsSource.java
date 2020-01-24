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

package org.apache.lucene.queries.intervals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

class ContainingIntervalsSource extends ConjunctionIntervalsSource {

  private final IntervalsSource big;
  private final IntervalsSource small;

  static IntervalsSource build(IntervalsSource big, IntervalsSource small) {
    return Intervals.or(Disjunctions.pullUp(big, s -> new ContainingIntervalsSource(s, small)));
  }

  private ContainingIntervalsSource(IntervalsSource big, IntervalsSource small) {
    super(Arrays.asList(big, small), false);
    this.big = big;
    this.small = small;
  }

  @Override
  protected IntervalIterator combine(List<IntervalIterator> iterators) {
    assert iterators.size() == 2;
    IntervalIterator a = iterators.get(0);
    IntervalIterator b = iterators.get(1);
    return new FilteringIntervalIterator(a, b) {
      @Override
      public int nextInterval() throws IOException {
        if (bpos == false)
          return IntervalIterator.NO_MORE_INTERVALS;
        while (a.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          while (b.start() < a.start() && b.end() < a.end()) {
            if (b.nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
              return IntervalIterator.NO_MORE_INTERVALS;
          }
          if (a.start() <= b.start() && a.end() >= b.end())
            return a.start();
        }
        return IntervalIterator.NO_MORE_INTERVALS;
      }
    };
  }

  @Override
  public int minExtent() {
    return big.minExtent();
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Disjunctions.pullUp(big, s -> new ContainingIntervalsSource(s, small));
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.subSources);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ContainingIntervalsSource == false) return false;
    ConjunctionIntervalsSource o = (ContainingIntervalsSource) other;
    return Objects.equals(this.subSources, o.subSources);
  }

  @Override
  public String toString() {
    return "CONTAINING(" + big + "," + small + ")";
  }
}
