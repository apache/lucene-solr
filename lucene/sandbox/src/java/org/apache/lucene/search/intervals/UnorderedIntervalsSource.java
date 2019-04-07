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

package org.apache.lucene.search.intervals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.util.PriorityQueue;

class UnorderedIntervalsSource extends ConjunctionIntervalsSource {

  static IntervalsSource build(List<IntervalsSource> sources, boolean allowOverlaps) {
    if (sources.size() == 1) {
      return sources.get(0);
    }
    return new UnorderedIntervalsSource(flatten(sources, allowOverlaps), allowOverlaps);
  }

  private static List<IntervalsSource> flatten(List<IntervalsSource> sources, boolean allowOverlaps) {
    List<IntervalsSource> flattened = new ArrayList<>();
    for (IntervalsSource s : sources) {
      if (s instanceof UnorderedIntervalsSource && ((UnorderedIntervalsSource)s).allowOverlaps == allowOverlaps) {
        flattened.addAll(((UnorderedIntervalsSource)s).subSources);
      }
      else {
        flattened.add(s);
      }
    }
    return flattened;
  }

  private final boolean allowOverlaps;

  private UnorderedIntervalsSource(List<IntervalsSource> sources, boolean allowOverlaps) {
    super(sources, true);
    this.allowOverlaps = allowOverlaps;
  }

  @Override
  protected IntervalIterator combine(List<IntervalIterator> iterators) {
    return new UnorderedIntervalIterator(iterators, allowOverlaps);
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
    return Disjunctions.pullUp(subSources, ss -> new UnorderedIntervalsSource(ss, allowOverlaps));
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.subSources, this.allowOverlaps);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof UnorderedIntervalsSource == false) return false;
    UnorderedIntervalsSource o = (UnorderedIntervalsSource) other;
    return Objects.equals(this.subSources, o.subSources) &&
        Objects.equals(this.allowOverlaps, o.allowOverlaps);
  }

  @Override
  public String toString() {
    return (allowOverlaps ? "UNORDERED(" : "UNORDERED_NO_OVERLAPS(") +
        subSources.stream().map(IntervalsSource::toString).collect(Collectors.joining(",")) + ")";
  }

  private static class UnorderedIntervalIterator extends ConjunctionIntervalIterator {

    private final PriorityQueue<IntervalIterator> queue;
    private final IntervalIterator[] subIterators;
    private final int[] innerPositions;
    private final boolean allowOverlaps;

    int start = -1, end = -1, firstEnd, queueEnd;

    UnorderedIntervalIterator(List<IntervalIterator> subIterators, boolean allowOverlaps) {
      super(subIterators);
      this.queue = new PriorityQueue<IntervalIterator>(subIterators.size()) {
        @Override
        protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
          return a.start() < b.start() || (a.start() == b.start() && a.end() >= b.end());
        }
      };
      this.subIterators = new IntervalIterator[subIterators.size()];
      this.innerPositions = new int[subIterators.size() * 2];
      this.allowOverlaps = allowOverlaps;

      for (int i = 0; i < subIterators.size(); i++) {
        this.subIterators[i] = subIterators.get(i);
      }
    }

    @Override
    public int start() {
      return start;
    }

    @Override
    public int end() {
      return end;
    }

    void updateRightExtreme(IntervalIterator it) {
      int itEnd = it.end();
      if (itEnd > queueEnd) {
        queueEnd = itEnd;
      }
    }

    @Override
    public int nextInterval() throws IOException {
      // first, find a matching interval
      while (this.queue.size() == subIterators.length && queue.top().start() == start) {
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          if (allowOverlaps == false) {
            while (hasOverlaps(it)) {
              if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS)
                return start = end = IntervalIterator.NO_MORE_INTERVALS;
            }
          }
          queue.add(it);
          updateRightExtreme(it);
        }
      }
      if (this.queue.size() < subIterators.length)
        return start = end = IntervalIterator.NO_MORE_INTERVALS;
      // then, minimize it
      do {
        start = queue.top().start();
        firstEnd = queue.top().end();
        end = queueEnd;
        if (queue.top().end() == end)
          return start;
        IntervalIterator it = queue.pop();
        if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          if (allowOverlaps == false) {
            while (hasOverlaps(it)) {
              if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
                return start;
              }
            }
          }
          queue.add(it);
          updateRightExtreme(it);
        }
      } while (this.queue.size() == subIterators.length && end == queueEnd);
      return start;
    }

    @Override
    public int gaps() {
      for (int i = 0; i < subIterators.length; i++) {
        if (subIterators[i].end() > end) {
          innerPositions[i * 2] = start;
          innerPositions[i * 2 + 1] = firstEnd;
        }
        else {
          innerPositions[i * 2] = subIterators[i].start();
          innerPositions[i * 2 + 1] = subIterators[i].end();
        }
      }
      Arrays.sort(innerPositions);
      int gaps = 0;
      for (int i = 1; i < subIterators.length; i++) {
        gaps += (innerPositions[i * 2] - innerPositions[i * 2 - 1] - 1);
      }
      return gaps;
    }

    @Override
    protected void reset() throws IOException {
      queueEnd = start = end = -1;
      this.queue.clear();
      loop: for (IntervalIterator it : subIterators) {
        if (it.nextInterval() == NO_MORE_INTERVALS) {
          break;
        }
        if (allowOverlaps == false) {
          while (hasOverlaps(it)) {
            if (it.nextInterval() == NO_MORE_INTERVALS) {
              break loop;
            }
          }
        }
        queue.add(it);
        updateRightExtreme(it);
      }
    }

    private boolean hasOverlaps(IntervalIterator candidate) {
      for (IntervalIterator it : queue) {
        if (it.start() < candidate.start()) {
          if (it.end() >= candidate.start()) {
            return true;
          }
          continue;
        }
        if (it.start() == candidate.start()) {
          return true;
        }
        if (it.start() <= candidate.end()) {
          return true;
        }
      }
      return false;
    }

  }
}
