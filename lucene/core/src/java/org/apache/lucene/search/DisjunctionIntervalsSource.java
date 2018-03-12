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
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.PriorityQueue;

class DisjunctionIntervalsSource extends IntervalsSource {

  final List<IntervalsSource> subSources;

  public DisjunctionIntervalsSource(List<IntervalsSource> subSources) {
    this.subSources = subSources;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    List<IntervalIterator> subIterators = new ArrayList<>();
    for (IntervalsSource subSource : subSources) {
      IntervalIterator it = subSource.intervals(field, ctx);
      if (it != null) {
        subIterators.add(it);
      }
    }
    if (subIterators.size() == 0)
      return null;
    return new DisjunctionIntervalIterator(subIterators);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DisjunctionIntervalsSource that = (DisjunctionIntervalsSource) o;
    return Objects.equals(subSources, that.subSources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subSources);
  }

  @Override
  public String toString() {
    return subSources.stream().map(Object::toString).collect(Collectors.joining(",", "or(", ")"));
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    for (IntervalsSource source : subSources) {
      source.extractTerms(field, terms);
    }
  }

  private static class DisjunctionIntervalIterator extends IntervalIterator {

    final PriorityQueue<IntervalIterator> intervalQueue;
    final DisiPriorityQueue disiQueue;
    final List<IntervalIterator> iterators;
    final float matchCost;

    IntervalIterator current = EMPTY;

    DisjunctionIntervalIterator(List<IntervalIterator> iterators) {
      super(buildApproximation(iterators));
      this.disiQueue = ((DisjunctionDISIApproximation)approximation).subIterators;
      this.iterators = iterators;
      this.intervalQueue = new PriorityQueue<IntervalIterator>(iterators.size()) {
        @Override
        protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
          // This is different to the Vigna paper, because we're interested in matching rather
          // than in minimizing intervals, so a wider interval should sort before its prefixes
          return a.start() < b.start() || (a.start() == b.start() && a.end() > b.end());
          //return a.end() < b.end() || (a.end() == b.end() && a.start() >= b.start());
        }
      };
      float costsum = 0;
      for (IntervalIterator it : iterators) {
        costsum += it.cost();
      }
      this.matchCost = costsum;
    }

    private static DocIdSetIterator buildApproximation(List<IntervalIterator> iterators) {
      DisiPriorityQueue disiQueue = new DisiPriorityQueue(iterators.size());
      for (IntervalIterator it : iterators) {
        disiQueue.add(new DisiWrapper(it));
      }
      return new DisjunctionDISIApproximation(disiQueue);
    }

    @Override
    public float matchCost() {
      return matchCost;
    }

    @Override
    public int start() {
      return current.start();
    }

    @Override
    public int end() {
      return current.end();
    }

    @Override
    protected void reset() throws IOException {
      intervalQueue.clear();
      for (DisiWrapper dw = disiQueue.topList(); dw != null; dw = dw.next) {
        dw.intervals.nextInterval();
        intervalQueue.add(dw.intervals);
      }
      current = EMPTY;
    }

    @Override
    public int nextInterval() throws IOException {
      if (current == EMPTY) {
        if (intervalQueue.size() > 0) {
          current = intervalQueue.top();
        }
        return current.start();
      }
      int start = current.start(), end = current.end();
      while (intervalQueue.size() > 0 && contains(intervalQueue.top(), start, end)) {
        IntervalIterator it = intervalQueue.pop();
        if (it != null && it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS) {
          intervalQueue.add(it);
        }
      }
      if (intervalQueue.size() == 0) {
        current = EMPTY;
        return IntervalIterator.NO_MORE_INTERVALS;
      }
      current = intervalQueue.top();
      return current.start();
    }

    private boolean contains(IntervalIterator it, int start, int end) {
      return start >= it.start() && start <= it.end() && end >= it.start() && end <= it.end();
    }

  }

  private static final IntervalIterator EMPTY = new IntervalIterator(DocIdSetIterator.empty()) {

    @Override
    public int start() {
      return -1;
    }

    @Override
    public int end() {
      return -1;
    }

    @Override
    public int nextInterval() {
      return NO_MORE_INTERVALS;
    }

    @Override
    public float matchCost() {
      return 0;
    }

    @Override
    protected void reset() throws IOException {

    }
  };

}
