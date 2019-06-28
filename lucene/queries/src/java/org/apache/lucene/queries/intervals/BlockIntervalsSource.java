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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class BlockIntervalsSource extends ConjunctionIntervalsSource {

  static IntervalsSource build(List<IntervalsSource> subSources) {
    if (subSources.size() == 1) {
      return subSources.get(0);
    }
    return Intervals.or(Disjunctions.pullUp(subSources, BlockIntervalsSource::new));
  }

  private static List<IntervalsSource> flatten(List<IntervalsSource> sources) {
    List<IntervalsSource> flattened = new ArrayList<>();
    for (IntervalsSource s : sources) {
      if (s instanceof BlockIntervalsSource) {
        flattened.addAll(((BlockIntervalsSource)s).subSources);
      }
      else {
        flattened.add(s);
      }
    }
    return flattened;
  }

  private BlockIntervalsSource(List<IntervalsSource> sources) {
    super(flatten(sources), true);
  }

  @Override
  protected IntervalIterator combine(List<IntervalIterator> iterators) {
    return new BlockIntervalIterator(iterators);
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
    return Collections.singletonList(this);   // Disjunctions already pulled up in build()
  }

  @Override
  public int hashCode() {
    return Objects.hash(subSources);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof BlockIntervalsSource == false) return false;
    BlockIntervalsSource b = (BlockIntervalsSource) other;
    return Objects.equals(this.subSources, b.subSources);
  }

  @Override
  public String toString() {
    return "BLOCK(" + subSources.stream().map(IntervalsSource::toString).collect(Collectors.joining(",")) + ")";
  }

  private static class BlockIntervalIterator extends ConjunctionIntervalIterator {

    int start = -1, end = -1;

    BlockIntervalIterator(List<IntervalIterator> subIterators) {
      super(subIterators);
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
    public int gaps() {
      return 0;
    }

    @Override
    public int nextInterval() throws IOException {
      if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
        return start = end = IntervalIterator.NO_MORE_INTERVALS;
      int i = 1;
      while (i < subIterators.size()) {
        while (subIterators.get(i).start() <= subIterators.get(i - 1).end()) {
          if (subIterators.get(i).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
            return start = end = IntervalIterator.NO_MORE_INTERVALS;
        }
        if (subIterators.get(i).start() == subIterators.get(i - 1).end() + 1) {
          i = i + 1;
        }
        else {
          if (subIterators.get(0).nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
            return start = end = IntervalIterator.NO_MORE_INTERVALS;
          i = 1;
        }
      }
      start = subIterators.get(0).start();
      end = subIterators.get(subIterators.size() - 1).end();
      return start;
    }

    @Override
    protected void reset() {
      start = end = -1;
    }
  }
}
