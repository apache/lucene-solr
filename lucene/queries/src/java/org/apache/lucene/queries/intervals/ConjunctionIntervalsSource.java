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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

abstract class ConjunctionIntervalsSource extends IntervalsSource {

  protected final List<IntervalsSource> subSources;
  protected final boolean isMinimizing;

  protected ConjunctionIntervalsSource(List<IntervalsSource> subSources, boolean isMinimizing) {
    assert subSources.size() > 1;
    this.subSources = subSources;
    this.isMinimizing = isMinimizing;
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    Query parent = new IntervalQuery(field, this);
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, parent);
    for (IntervalsSource source : subSources) {
      source.visit(field, v);
    }
  }

  @Override
  public final IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    List<IntervalIterator> subIntervals = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalIterator it = source.intervals(field, ctx);
      if (it == null)
        return null;
      subIntervals.add(it);
    }
    return combine(subIntervals);
  }

  protected abstract IntervalIterator combine(List<IntervalIterator> iterators);

  @Override
  public final IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<IntervalMatchesIterator> subs = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalMatchesIterator mi = source.matches(field, ctx, doc);
      if (mi == null) {
        return null;
      }
      if (isMinimizing) {
        mi = new CachingMatchesIterator(mi);
      }
      subs.add(mi);
    }
    IntervalIterator it = combine(subs.stream().map(m -> IntervalMatches.wrapMatches(m, doc)).collect(Collectors.toList()));
    if (it.advance(doc) != doc) {
      return null;
    }
    if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
      return null;
    }
    return isMinimizing ? new MinimizingConjunctionMatchesIterator(it, subs) : new ConjunctionMatchesIterator(it, subs);
  }

  private static class ConjunctionMatchesIterator implements IntervalMatchesIterator {

    final IntervalIterator iterator;
    final List<IntervalMatchesIterator> subs;
    boolean cached = true;

    private ConjunctionMatchesIterator(IntervalIterator iterator, List<IntervalMatchesIterator> subs) {
      this.iterator = iterator;
      this.subs = subs;
    }

    @Override
    public boolean next() throws IOException {
      if (cached) {
        cached = false;
        return true;
      }
      return iterator.nextInterval() != IntervalIterator.NO_MORE_INTERVALS;
    }

    @Override
    public int startPosition() {
      return iterator.start();
    }

    @Override
    public int endPosition() {
      return iterator.end();
    }

    @Override
    public int startOffset() throws IOException {
      int start = Integer.MAX_VALUE;
      for (MatchesIterator s : subs) {
        start = Math.min(start, s.startOffset());
      }
      return start;
    }

    @Override
    public int endOffset() throws IOException {
      int end = -1;
      for (MatchesIterator s : subs) {
        end = Math.max(end, s.endOffset());
      }
      return end;
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
      List<MatchesIterator> subMatches = new ArrayList<>();
      for (MatchesIterator mi : subs) {
        MatchesIterator sub = mi.getSubMatches();
        if (sub == null) {
          sub = new SingletonMatchesIterator(mi);
        }
        subMatches.add(sub);
      }
      return MatchesUtils.disjunction(subMatches);
    }

    @Override
    public Query getQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int gaps() {
      return iterator.gaps();
    }

    @Override
    public int width() {
      return iterator.width();
    }
  }

  static class SingletonMatchesIterator extends FilterMatchesIterator {

    boolean exhausted = false;

    SingletonMatchesIterator(MatchesIterator in) {
      super(in);
    }

    @Override
    public boolean next() {
      if (exhausted) {
        return false;
      }
      return exhausted = true;
    }
  }

}
