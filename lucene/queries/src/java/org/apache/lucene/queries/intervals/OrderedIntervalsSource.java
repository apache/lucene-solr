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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class OrderedIntervalsSource extends ConjunctionIntervalsSource {

  static IntervalsSource build(List<IntervalsSource> sources) {
    if (sources.size() == 1) {
      return sources.get(0);
    }
    return new OrderedIntervalsSource(flatten(sources));
  }

  private static List<IntervalsSource> flatten(List<IntervalsSource> sources) {
    List<IntervalsSource> flattened = new ArrayList<>();
    for (IntervalsSource s : sources) {
      if (s instanceof OrderedIntervalsSource) {
        flattened.addAll(((OrderedIntervalsSource)s).subSources);
      }
      else {
        flattened.add(s);
      }
    }
    return flattened;
  }

  private OrderedIntervalsSource(List<IntervalsSource> sources) {
    super(sources, true);
  }

  @Override
  protected IntervalIterator combine(List<IntervalIterator> iterators) {
    return new OrderedIntervalIterator(iterators);
  }

  @Override
  public int minExtent() {
    int minExtent = 0;
    for (IntervalsSource subSource : subSources) {
      minExtent += subSource.minExtent();
    }
    return minExtent;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Disjunctions.pullUp(subSources, OrderedIntervalsSource::new);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(subSources);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof OrderedIntervalsSource == false) return false;
    OrderedIntervalsSource s = (OrderedIntervalsSource) other;
    return Objects.equals(subSources, s.subSources);
  }

  @Override
  public String toString() {
    return "ORDERED(" + subSources.stream().map(IntervalsSource::toString).collect(Collectors.joining(",")) + ")";
  }

  private static class OrderedIntervalIterator extends ConjunctionIntervalIterator {

    int start = -1, end = -1, i;
    int firstEnd;

    private OrderedIntervalIterator(List<IntervalIterator> subIntervals) {
      super(subIntervals);
    }

    @Override
    public int start() {
      return start;
    }

    @Override
    public int end() {
      return end;
    }

    @Override
    public int nextInterval() throws IOException {
      start = end = IntervalIterator.NO_MORE_INTERVALS;
      int b = Integer.MAX_VALUE;
      i = 1;
      while (true) {
        while (true) {
          if (subIterators.get(i - 1).end() >= b)
            return start;
          if (i == subIterators.size() || subIterators.get(i).start() > subIterators.get(i - 1).end())
            break;
          do {
            if (subIterators.get(i).end() >= b || subIterators.get(i).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
              return start;
          }
          while (subIterators.get(i).start() <= subIterators.get(i - 1).end());
          i++;
        }
        start = subIterators.get(0).start();
        if (start == NO_MORE_INTERVALS) {
          return end = NO_MORE_INTERVALS;
        }
        firstEnd = subIterators.get(0).end();
        end = subIterators.get(subIterators.size() - 1).end();
        b = subIterators.get(subIterators.size() - 1).start();
        i = 1;
        if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
          return start;
      }
    }

    @Override
    public int gaps() {
      int gaps = subIterators.get(1).start() - firstEnd - 1;
      for (int i = 2; i < subIterators.size(); i++) {
        gaps += (subIterators.get(i).start() - subIterators.get(i - 1).end() - 1);
      }
      return gaps;
    }

    @Override
    protected void reset() throws IOException {
      subIterators.get(0).nextInterval();
      i = 1;
      start = end = firstEnd = -1;
    }
  }
}
