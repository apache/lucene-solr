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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;

class MultiTermIntervalsSource extends IntervalsSource {

  private final CompiledAutomaton automaton;
  private final int maxExpansions;
  private final String pattern;

  MultiTermIntervalsSource(CompiledAutomaton automaton, int maxExpansions, String pattern) {
    this.automaton = automaton;
    if (maxExpansions > IndexSearcher.getMaxClauseCount()) {
      throw new IllegalArgumentException("maxExpansions [" + maxExpansions
          + "] cannot be greater than BooleanQuery.getMaxClauseCount [" + IndexSearcher.getMaxClauseCount() + "]");
    }
    this.maxExpansions = maxExpansions;
    this.pattern = pattern;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null) {
      return null;
    }
    List<IntervalIterator> subSources = new ArrayList<>();
    TermsEnum te = automaton.getTermsEnum(terms);
    BytesRef term;
    int count = 0;
    while ((term = te.next()) != null) {
      subSources.add(TermIntervalsSource.intervals(term, te));
      if (++count > maxExpansions) {
        throw new IllegalStateException("Automaton [" + this.pattern + "] expanded to too many terms (limit " + maxExpansions + ")");
      }
    }
    if (subSources.size() == 0) {
      return null;
    }
    return new DisjunctionIntervalsSource.DisjunctionIntervalIterator(subSources);
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    Terms terms = ctx.reader().terms(field);
    if (terms == null) {
      return null;
    }
    List<MatchesIterator> subMatches = new ArrayList<>();
    TermsEnum te = automaton.getTermsEnum(terms);
    BytesRef term;
    int count = 0;
    while ((term = te.next()) != null) {
      MatchesIterator mi = TermIntervalsSource.matches(te, doc, field);
      if (mi != null) {
        subMatches.add(mi);
        if (count++ > maxExpansions) {
          throw new IllegalStateException("Automaton " + term + " expanded to too many terms (limit " + maxExpansions + ")");
        }
      }
    }
    return MatchesUtils.disjunction(subMatches);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    visitor.visitLeaf(new IntervalQuery(field, this));
  }

  @Override
  public int minExtent() {
    return 1;
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singleton(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MultiTermIntervalsSource that = (MultiTermIntervalsSource) o;
    return maxExpansions == that.maxExpansions &&
        Objects.equals(automaton, that.automaton) &&
        Objects.equals(pattern, that.pattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(automaton, maxExpansions, pattern);
  }

  @Override
  public String toString() {
    return "MultiTerm(" + pattern + ")";
  }
}
