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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.PriorityQueue;

class DisjunctionIntervalsSource extends IntervalsSource {

  final Collection<IntervalsSource> subSources;
  final boolean pullUpDisjunctions;

  static IntervalsSource create(Collection<IntervalsSource> subSources, boolean pullUpDisjunctions) {
    subSources = simplify(subSources);
    if (subSources.size() == 1) {
      return subSources.iterator().next();
    }
    return new DisjunctionIntervalsSource(subSources, pullUpDisjunctions);
  }

  private DisjunctionIntervalsSource(Collection<IntervalsSource> subSources, boolean pullUpDisjunctions) {
    this.subSources = simplify(subSources);
    this.pullUpDisjunctions = pullUpDisjunctions;
  }

  private static Collection<IntervalsSource> simplify(Collection<IntervalsSource> sources) {
    Set<IntervalsSource> simplified = new HashSet<>();
    for (IntervalsSource source : sources) {
      if (source instanceof DisjunctionIntervalsSource) {
        simplified.addAll(source.pullUpDisjunctions());
      }
      else {
        simplified.add(source);
      }
    }
    return simplified;
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
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<IntervalMatchesIterator> subMatches = new ArrayList<>();
    for (IntervalsSource subSource : subSources) {
      IntervalMatchesIterator mi = subSource.matches(field, ctx, doc);
      if (mi != null) {
        subMatches.add(mi);
      }
    }
    if (subMatches.size() == 0) {
      return null;
    }
    DisjunctionIntervalIterator it = new DisjunctionIntervalIterator(
        subMatches.stream().map(m -> IntervalMatches.wrapMatches(m, doc)).collect(Collectors.toList())
    );
    if (it.advance(doc) != doc) {
      return null;
    }
    return new DisjunctionMatchesIterator(it, subMatches);
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
    return subSources.stream()
        .map(Object::toString)
        .sorted()
        .collect(Collectors.joining(",", "or(", ")"));
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
    int minExtent = Integer.MAX_VALUE;
    for (IntervalsSource subSource : subSources) {
      minExtent = Math.min(minExtent, subSource.minExtent());
    }
    return minExtent;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    if (pullUpDisjunctions) {
      return subSources;
    }
    return Collections.singletonList(this);
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

    int currentOrd() {
      assert current != EMPTY && current != EXHAUSTED;
      for (int i = 0; i < iterators.size(); i++) {
        if (iterators.get(i) == current) {
          return i;
        }
      }
      throw new IllegalStateException();
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

  private static class DisjunctionMatchesIterator implements IntervalMatchesIterator {

    final DisjunctionIntervalIterator it;
    final List<IntervalMatchesIterator> subs;

    private DisjunctionMatchesIterator(DisjunctionIntervalIterator it, List<IntervalMatchesIterator> subs) {
      this.it = it;
      this.subs = subs;
    }

    @Override
    public boolean next() throws IOException {
      return it.nextInterval() != IntervalIterator.NO_MORE_INTERVALS;
    }

    @Override
    public int startPosition() {
      return it.start();
    }

    @Override
    public int endPosition() {
      return it.end();
    }

    @Override
    public int startOffset() throws IOException {
      int ord = it.currentOrd();
      return subs.get(ord).startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      int ord = it.currentOrd();
      return subs.get(ord).endOffset();
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
      int ord = it.currentOrd();
      return subs.get(ord).getSubMatches();
    }

    @Override
    public Query getQuery() {
      int ord = it.currentOrd();
      return subs.get(ord).getQuery();
    }

    @Override
    public int gaps() {
      return it.gaps();
    }

    @Override
    public int width() {
      return it.width();
    }
  }

}
