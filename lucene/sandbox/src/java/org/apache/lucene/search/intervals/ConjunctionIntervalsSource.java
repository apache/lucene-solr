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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

class ConjunctionIntervalsSource extends IntervalsSource {

  protected final List<IntervalsSource> subSources;
  protected final IntervalFunction function;

  ConjunctionIntervalsSource(List<IntervalsSource> subSources, IntervalFunction function) {
    assert subSources.size() > 1;
    this.subSources = rewrite(subSources, function);
    this.function = function;
  }

  private static List<IntervalsSource> rewrite(List<IntervalsSource> subSources, IntervalFunction function) {
    List<IntervalsSource> flattenedSources = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      if (source instanceof ConjunctionIntervalsSource) {
        IntervalFunction wrappedFunction = ((ConjunctionIntervalsSource)source).function;
        if (wrappedFunction == function) {
          flattenedSources.addAll(((ConjunctionIntervalsSource)source).subSources);
          continue;
        }
      }
      flattenedSources.add(source);
    }
    return rewriteDisjunctions(flattenedSources, function);
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
  public void visit(String field, QueryVisitor visitor) {
    Query parent = new IntervalQuery(field, this);
    QueryVisitor v = visitor.getSubVisitor(BooleanClause.Occur.MUST, parent);
    for (IntervalsSource source : subSources) {
      source.visit(field, v);
    }
  }

  @Override
  public int minExtent() {
    int minExtent = 0;
    for (IntervalsSource source : subSources) {
      minExtent += source.minExtent();
    }
    return minExtent;
  }

  @Override
  public Collection<IntervalsSource> getDisjunctions() {
    if (function.isFiltering() == false) {
      return Collections.singleton(this);
    }
    assert subSources.size() == 2;
    Collection<IntervalsSource> inner = subSources.get(0).getDisjunctions();
    if (inner.size() == 1) {
      return Collections.singleton(this);
    }
    IntervalsSource filter = subSources.get(1);
    return inner.stream().map(s -> new ConjunctionIntervalsSource(Arrays.asList(s, filter), function)).collect(Collectors.toSet());
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
      if (function.isMinimizing()) {
        mi = new CachingMatchesIterator(mi);
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
    return function.isMinimizing() ? new MinimizingConjunctionMatchesIterator(it, subs) : new ConjunctionMatchesIterator(it, subs);
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

  private static class Clauses {

    final Set<IntervalsSource> singletons = new HashSet<>();
    final Set<IntervalsSource> nonSingletons = new HashSet<>();

    Clauses(IntervalsSource source) {
      for (IntervalsSource s : source.getDisjunctions()) {
        if (s.minExtent() == 1) {
          singletons.add(s);
        }
        else {
          nonSingletons.add(s);
        }
      }
    }

    boolean hasNonSingletons() {
      if (singletons.size() == 0) {
        return nonSingletons.size() > 1;
      }
      return nonSingletons.size() > 0;
    }

    IntervalsSource rewrite(IntervalFunction function, IntervalsSource next) {
      List<IntervalsSource> out = new ArrayList<>();
      if (singletons.size() > 0) {
        out.add(new ConjunctionIntervalsSource(Arrays.asList(Intervals.or(singletons.toArray(new IntervalsSource[0])), next), function));
      }
      for (IntervalsSource source : nonSingletons) {
        out.add(new ConjunctionIntervalsSource(Arrays.asList(source, next), function));
      }
      return Intervals.or(out.toArray(new IntervalsSource[0]));
    }

  }

  private static List<IntervalsSource> rewriteDisjunctions(List<IntervalsSource> sources, IntervalFunction function) {
    if (function.rewriteDisjunctions() == false) {
      return sources;
    }
    // Go backwards through the list, from n - 1 to 0
    // If n is a disjunction, then collect term counts for each of its subsources
    // if we have a mixture of single-term and multiple-term subsources, then we need to rewrite
    // - each subsource with multiple terms becomes function(subsource, subsource(n + 1))
    // - collect all single term subsources together and create function(or(singletons), subsource(n + 1))
    // - replace this subsource with disjunction of created functions
    // - remove subsource(n + 1)

    for (int i = sources.size() - 2; i >= 0; i--) {
      Clauses clauses = new Clauses(sources.get(i));
      if (clauses.hasNonSingletons()) {
        sources.set(i, clauses.rewrite(function, sources.get(i + 1)));
        sources.remove(i + 1);
        // if we've rewritten stuff and there are still trailing sources, then we back
        // up to see if we need to rewrite again
        if (i < sources.size() - 1) {
          i++;
        }
      }
    }
    return sources;
  }

}
