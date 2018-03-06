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

  private static class DisjunctionIntervalIterator implements IntervalIterator {

    final PriorityQueue<IntervalIterator> intervalQueue;
    final DisiPriorityQueue disiQueue;
    final DisjunctionDISIApproximation approximation;
    final List<IntervalIterator> iterators;
    final float matchCost;

    IntervalIterator current;

    DisjunctionIntervalIterator(List<IntervalIterator> iterators) {
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
      this.disiQueue = new DisiPriorityQueue(iterators.size());
      float costsum = 0;
      for (IntervalIterator it : iterators) {
        this.disiQueue.add(new DisiWrapper(it));
        costsum += it.cost();
      }
      this.matchCost = costsum;
      this.approximation = new DisjunctionDISIApproximation(this.disiQueue);
    }

    @Override
    public DocIdSetIterator approximation() {
      return approximation;
    }

    @Override
    public float cost() {
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
    public int innerWidth() {
      return current.innerWidth();
    }

    @Override
    public boolean advanceTo(int doc) throws IOException {
      intervalQueue.clear();
      int approxDoc = this.approximation.docID();
      if (approxDoc > doc || (approxDoc != doc && this.approximation.advance(doc) != doc)) {
        return false;
      }
      for (DisiWrapper dw = disiQueue.topList(); dw != null; dw = dw.next) {
        IntervalIterator it = dw.intervals;
        if (it.advanceTo(doc)) {
          it.nextInterval();
          intervalQueue.add(it);
        }
      }
      current = UNPOSITIONED;
      return intervalQueue.size() > 0;
    }

    @Override
    public int nextInterval() throws IOException {
      if (current == UNPOSITIONED) {
        current = intervalQueue.top();
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

    @Override
    public String toString() {
      return approximation.docID() + ":[" + start() + "->" + end() + "]";
    }

    private boolean contains(IntervalIterator it, int start, int end) {
      return start >= it.start() && start <= it.end() && end >= it.start() && end <= it.end();
    }

  }

  private static final IntervalIterator EMPTY = new IntervalIterator() {
    @Override
    public DocIdSetIterator approximation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceTo(int doc) throws IOException {
      return false;
    }

    @Override
    public int start() {
      return NO_MORE_INTERVALS;
    }

    @Override
    public int end() {
      return NO_MORE_INTERVALS;
    }

    @Override
    public int innerWidth() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextInterval() throws IOException {
      return NO_MORE_INTERVALS;
    }

    @Override
    public float cost() {
      return 0;
    }
  };

  private static final IntervalIterator UNPOSITIONED = new IntervalIterator() {
    @Override
    public DocIdSetIterator approximation() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceTo(int doc) throws IOException {
      return false;
    }

    @Override
    public int start() {
      return -1;
    }

    @Override
    public int end() {
      return -1;
    }

    @Override
    public int innerWidth() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextInterval() throws IOException {
      return NO_MORE_INTERVALS;
    }

    @Override
    public float cost() {
      return 0;
    }
  };
}
