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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
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
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<MatchesIterator> subMatches = new ArrayList<>();
    for (IntervalsSource subSource : subSources) {
      MatchesIterator mi = subSource.matches(field, ctx, doc);
      if (mi != null) {
        subMatches.add(mi);
      }
    }
    return MatchesUtils.disjunction(subMatches);
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
  public void visit(String field, QueryVisitor visitor) {
    Query parent = new IntervalQuery(field, this);
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.SHOULD, parent);
    for (IntervalsSource source : subSources) {
      source.visit(field, v);
    }
  }

  @Override
  public int minExtent() {
    int minExtent = subSources.get(0).minExtent();
    for (int i = 1; i < subSources.size(); i++) {
      minExtent = Math.min(minExtent, subSources.get(i).minExtent());
    }
    return minExtent;
  }

  static class DisjunctionIntervalIterator extends IntervalIterator {

    final DocIdSetIterator approximation;
    final PriorityQueue<IntervalIterator> intervalQueue;
    final DisiPriorityQueue disiQueue;
    final List<IntervalIterator> iterators;
    final float matchCost;

    IntervalIterator current = EMPTY;

    DisjunctionIntervalIterator(List<IntervalIterator> iterators) {
      this.disiQueue = new DisiPriorityQueue(iterators.size());
      for (IntervalIterator it : iterators) {
        disiQueue.add(new DisiWrapper(it));
      }
      this.approximation = new DisjunctionDISIApproximation(disiQueue);
      this.iterators = iterators;
      this.intervalQueue = new PriorityQueue<IntervalIterator>(iterators.size()) {
        @Override
        protected boolean lessThan(IntervalIterator a, IntervalIterator b) {
          return a.end() < b.end() || (a.end() == b.end() && a.start() >= b.start());
        }
      };
      float costsum = 0;
      for (IntervalIterator it : iterators) {
        costsum += it.cost();
      }
      this.matchCost = costsum;
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
    public int gaps() {
      return current.gaps();
    }

    private void reset() throws IOException {
      intervalQueue.clear();
      for (DisiWrapper dw = disiQueue.topList(); dw != null; dw = dw.next) {
        dw.intervals.nextInterval();
        intervalQueue.add(dw.intervals);
      }
      current = EMPTY;
    }

    @Override
    public int nextInterval() throws IOException {
      if (current == EMPTY || current == EXHAUSTED) {
        if (intervalQueue.size() > 0) {
          current = intervalQueue.top();
        }
        return current.start();
      }
      int start = current.start(), end = current.end();
      while (intervalQueue.size() > 0 && contains(intervalQueue.top(), start, end)) {
        IntervalIterator it = intervalQueue.pop();
        if (it != null && it.nextInterval() != NO_MORE_INTERVALS) {
          intervalQueue.add(it);
        }
      }
      if (intervalQueue.size() == 0) {
        current = EXHAUSTED;
        return NO_MORE_INTERVALS;
      }
      current = intervalQueue.top();
      return current.start();
    }

    private boolean contains(IntervalIterator it, int start, int end) {
      return start >= it.start() && start <= it.end() && end >= it.start() && end <= it.end();
    }

    @Override
    public int docID() {
      return approximation.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int doc = approximation.nextDoc();
      reset();
      return doc;
    }

    @Override
    public int advance(int target) throws IOException {
      int doc = approximation.advance(target);
      reset();
      return doc;
    }

    @Override
    public long cost() {
      return approximation.cost();
    }
  }

  private static final IntervalIterator EMPTY = new IntervalIterator() {

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
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
    public int gaps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextInterval() {
      return NO_MORE_INTERVALS;
    }

    @Override
    public float matchCost() {
      return 0;
    }
  };

  private static final IntervalIterator EXHAUSTED = new IntervalIterator() {

    @Override
    public int docID() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextDoc() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int advance(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
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
    public int gaps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextInterval() {
      return NO_MORE_INTERVALS;
    }

    @Override
    public float matchCost() {
      return 0;
    }
  };

}
