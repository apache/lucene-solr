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
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;

class ConjunctionIntervalsSource extends IntervalsSource {

  protected final List<IntervalsSource> subSources;
  protected final IntervalFunction function;

  ConjunctionIntervalsSource(List<IntervalsSource> subSources, IntervalFunction function) {
    this.subSources = subSources;
    this.function = function;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConjunctionIntervalsSource that = (ConjunctionIntervalsSource) o;
    return Objects.equals(subSources, that.subSources) &&
        Objects.equals(function, that.function);
  }

  @Override
  public String toString() {
    return function + subSources.stream().map(Object::toString).collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    for (IntervalsSource source : subSources) {
      source.extractTerms(field, terms);
    }
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    List<IntervalIterator> subIntervals = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalIterator it = source.intervals(field, ctx);
      if (it == null)
        return null;
      subIntervals.add(it);
    }
    return function.apply(subIntervals);
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<MatchesIterator> subs = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      MatchesIterator mi = source.matches(field, ctx, doc);
      if (mi == null) {
        return null;
      }
      subs.add(mi);
    }
    IntervalIterator it = function.apply(subs.stream().map(m -> IntervalMatches.wrapMatches(m, doc)).collect(Collectors.toList()));
    if (it.advance(doc) != doc) {
      return null;
    }
    if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
      return null;
    }
    return new ConjunctionMatchesIterator(it, subs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subSources, function);
  }

  private static class ConjunctionMatchesIterator implements MatchesIterator {

    final IntervalIterator iterator;
    final List<MatchesIterator> subs;
    boolean cached = true;

    private ConjunctionMatchesIterator(IntervalIterator iterator, List<MatchesIterator> subs) {
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
  }

  private static class SingletonMatchesIterator extends FilterMatchesIterator {

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
