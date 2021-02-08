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

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;

/**
 * Constructor functions for {@link IntervalsSource} types
 *
 * These sources implement minimum-interval algorithms taken from the paper
 * <a href="http://vigna.di.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics.pdf">
 * Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics</a>
 *
 * By default, sources that are sensitive to internal gaps (e.g. PHRASE and MAXGAPS) will
 * rewrite their sub-sources so that disjunctions of different lengths are pulled up
 * to the top of the interval tree.  For example, PHRASE(or(PHRASE("a", "b", "c"), "b"), "c")
 * will automatically rewrite itself to OR(PHRASE("a", "b", "c", "c"), PHRASE("b", "c"))
 * to ensure that documents containing "b c" are matched.  This can lead to less efficient
 * queries, as more terms need to be loaded (for example, the "c" iterator above is loaded
 * twice), so if you care more about speed than about accuracy you can use the
 * {@link #or(boolean, IntervalsSource...)} factory method to prevent rewriting.
 */
public final class Intervals {

  private Intervals() {}

  /**
   * Return an {@link IntervalsSource} exposing intervals for a term
   */
  public static IntervalsSource term(BytesRef term) {
    return new TermIntervalsSource(term);
  }

  /**
   * Return an {@link IntervalsSource} exposing intervals for a term
   */
  public static IntervalsSource term(String term) {
    return new TermIntervalsSource(new BytesRef(term));
  }

  /**
   * Return an {@link IntervalsSource} exposing intervals for a term, filtered by the value
   * of the term's payload at each position
   */
  public static IntervalsSource term(String term, Predicate<BytesRef> payloadFilter) {
    return term(new BytesRef(term), payloadFilter);
  }

  /**
   * Return an {@link IntervalsSource} exposing intervals for a term, filtered by the value
   * of the term's payload at each position
   */
  public static IntervalsSource term(BytesRef term, Predicate<BytesRef> payloadFilter) {
    return new PayloadFilteredTermIntervalsSource(term, payloadFilter);
  }

  /**
   * Return an {@link IntervalsSource} exposing intervals for a phrase consisting of a list of terms
   */
  public static IntervalsSource phrase(String... terms) {
    if (terms.length == 1) {
      return Intervals.term(terms[0]);
    }
    IntervalsSource[] sources = new IntervalsSource[terms.length];
    int i = 0;
    for (String term : terms) {
      sources[i] = term(term);
      i++;
    }
    return phrase(sources);
  }

  /**
   * Return an {@link IntervalsSource} exposing intervals for a phrase consisting of a list of IntervalsSources
   */
  public static IntervalsSource phrase(IntervalsSource... subSources) {
    return BlockIntervalsSource.build(Arrays.asList(subSources));
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of a set of sub-sources
   *
   * Automatically rewrites if wrapped by an interval source that is sensitive to
   * internal gaps
   */
  public static IntervalsSource or(IntervalsSource... subSources) {
    return or(true, Arrays.asList(subSources));
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of a set of sub-sources
   *
   * @param rewrite      if {@code false}, do not rewrite intervals that are sensitive to
   *                     internal gaps; this may run more efficiently, but can miss valid
   *                     hits due to minimization
   * @param subSources   the sources to combine
   */
  public static IntervalsSource or(boolean rewrite, IntervalsSource... subSources) {
    return or(rewrite, Arrays.asList(subSources));
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of a set of sub-sources
   */
  public static IntervalsSource or(List<IntervalsSource> subSources) {
    return or(true, subSources);
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of a set of sub-sources
   *
   * @param rewrite      if {@code false}, do not rewrite intervals that are sensitive to
   *                     internal gaps; this may run more efficiently, but can miss valid
   *                     hits due to minimization
   * @param subSources   the sources to combine
   */
  public static IntervalsSource or(boolean rewrite, List<IntervalsSource> subSources) {
    return DisjunctionIntervalsSource.create(subSources, rewrite);
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of all terms that begin with a prefix
   *
   * @throws IllegalStateException if the prefix expands to more than 128 terms
   */
  public static IntervalsSource prefix(BytesRef prefix) {
    return prefix(prefix, 128);
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that begin with a prefix
   *
   * WARNING: Setting {@code maxExpansions} to higher than the default value of 128
   * can be both slow and memory-intensive
   *
   * @param prefix        the prefix to expand
   * @param maxExpansions the maximum number of terms to expand to
   *
   * @throws IllegalStateException if the prefix expands to more than {@code maxExpansions} terms
   */
  public static IntervalsSource prefix(BytesRef prefix, int maxExpansions) {
    CompiledAutomaton ca = new CompiledAutomaton(PrefixQuery.toAutomaton(prefix));
    return new MultiTermIntervalsSource(ca, maxExpansions, prefix.utf8ToString() + "*");
  }

  /**
   * Return an {@link IntervalsSource} over the disjunction of all terms that match a wildcard glob
   *
   * @throws IllegalStateException if the wildcard glob expands to more than 128 terms
   *
   * @see WildcardQuery for glob format
   */
  public static IntervalsSource wildcard(BytesRef wildcard) {
    return wildcard(wildcard, 128);
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that match a wildcard glob
   *
   * WARNING: Setting {@code maxExpansions} to higher than the default value of 128
   * can be both slow and memory-intensive
   *
   * @param wildcard the glob to expand
   * @param maxExpansions the maximum number of terms to expand to
   *
   * @throws IllegalStateException if the wildcard glob expands to more than {@code maxExpansions} terms
   *
   * @see WildcardQuery for glob format
   */
  public static IntervalsSource wildcard(BytesRef wildcard, int maxExpansions) {
    CompiledAutomaton ca = new CompiledAutomaton(WildcardQuery.toAutomaton(new Term("", wildcard)));
    return new MultiTermIntervalsSource(ca, maxExpansions, wildcard.utf8ToString());
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that's accepted by the given automaton 
   *
   * @param automaton accepts terms for to expand to
   * @param pattern string representation of the given automaton, mostly used in exception messages  
   *
   * @throws IllegalStateException if the automaton accepts more than 128 terms
   *
   * @deprecated use {@link #multiterm(CompiledAutomaton, String)}
   */
  @Deprecated
  public static IntervalsSource multiterm(Automaton automaton, String pattern) {
    return multiterm(automaton, 128, pattern);
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that's accepted by the given automaton 
   *
   * WARNING: Setting {@code maxExpansions} to higher than the default value of 128
   * can be both slow and memory-intensive
   *
   * @param automaton accepts terms for to expand to
   * @param maxExpansions the maximum number of terms to expand to
   * @param pattern string representation of the given automaton, mostly used in exception messages  
   *
   * @throws IllegalStateException if the automaton accepts more than {@code maxExpansions} terms
   *
   * @deprecated use {@link #multiterm(CompiledAutomaton, int, String)}
   */
  @Deprecated
  public static IntervalsSource multiterm(Automaton automaton, int maxExpansions, String pattern) {
    CompiledAutomaton ca = new CompiledAutomaton(automaton);
    return new MultiTermIntervalsSource(ca, maxExpansions, pattern);
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that's accepted by the given automaton
   *
   * @param ca      an automaton accepting matching terms
   * @param pattern string representation of the given automaton, mostly used in exception messages
   *
   * @throws IllegalStateException if the automaton accepts more than 128 terms
   */
  public static IntervalsSource multiterm(CompiledAutomaton ca, String pattern) {
    return multiterm(ca, 128, pattern);
  }

  /**
   * Expert: Return an {@link IntervalsSource} over the disjunction of all terms that's accepted by the given automaton
   *
   * WARNING: Setting {@code maxExpansions} to higher than the default value of 128
   * can be both slow and memory-intensive
   *
   * @param ca            an automaton accepting matching terms
   * @param maxExpansions the maximum number of terms to expand to
   * @param pattern string representation of the given automaton, mostly used in exception messages
   *
   * @throws IllegalStateException if the automaton accepts more than {@code maxExpansions} terms
   */
  public static IntervalsSource multiterm(CompiledAutomaton ca, int maxExpansions, String pattern) {
    return new MultiTermIntervalsSource(ca, maxExpansions, pattern);
  }
  
  /**
   * Create an {@link IntervalsSource} that filters a sub-source by the width of its intervals
   * @param width       the maximum width of intervals in the sub-source to filter
   * @param subSource   the sub-source to filter
   */
  public static IntervalsSource maxwidth(int width, IntervalsSource subSource) {
    return FilteredIntervalsSource.maxWidth(subSource, width);
  }

  /**
   * Create an {@link IntervalsSource} that filters a sub-source by its gaps
   * @param gaps        the maximum number of gaps in the sub-source to filter
   * @param subSource   the sub-source to filter
   */
  public static IntervalsSource maxgaps(int gaps, IntervalsSource subSource) {
    return FilteredIntervalsSource.maxGaps(subSource, gaps);
  }

  /**
   * Create an {@link IntervalsSource} that wraps another source, extending its
   * intervals by a number of positions before and after.
   *
   * This can be useful for adding defined gaps in a block query; for example,
   * to find 'a b [2 arbitrary terms] c', you can call:
   * <pre>
   *   Intervals.phrase(Intervals.term("a"), Intervals.extend(Intervals.term("b"), 0, 2), Intervals.term("c"));
   * </pre>
   *
   * Note that calling {@link IntervalIterator#gaps()} on iterators returned by this source
   * delegates directly to the wrapped iterator, and does not include the extensions.
   *
   * @param source the source to extend
   * @param before how many positions to extend before the delegated interval
   * @param after  how many positions to extend after the delegated interval
   */
  public static IntervalsSource extend(IntervalsSource source, int before, int after) {
    return new ExtendedIntervalsSource(source, before, after);
  }

  /**
   * Create an ordered {@link IntervalsSource}
   *
   * Returns intervals in which the subsources all appear in the given order
   *
   * @param subSources  an ordered set of {@link IntervalsSource} objects
   */
  public static IntervalsSource ordered(IntervalsSource... subSources) {
    return OrderedIntervalsSource.build(Arrays.asList(subSources));
  }

  /**
   * Create an unordered {@link IntervalsSource}
   *
   * Returns intervals in which all the subsources appear.  The subsources may overlap
   *
   * @param subSources  an unordered set of {@link IntervalsSource}s
   */
  public static IntervalsSource unordered(IntervalsSource... subSources) {
    return UnorderedIntervalsSource.build(Arrays.asList(subSources));
  }

  /**
   * Create an unordered {@link IntervalsSource} allowing no overlaps between subsources
   *
   * Returns intervals in which both the subsources appear and do not overlap.
   */
  public static IntervalsSource unorderedNoOverlaps(IntervalsSource a, IntervalsSource b) {
    return Intervals.or(Intervals.ordered(a, b), Intervals.ordered(b, a));
  }

  /**
   * Create an {@link IntervalsSource} that always returns intervals from a specific field
   *
   * This is useful for comparing intervals across multiple fields, for example fields that
   * have been analyzed differently, allowing you to search for stemmed terms near unstemmed
   * terms, etc.
   */
  public static IntervalsSource fixField(String field, IntervalsSource source) {
    return new FixedFieldIntervalsSource(field, source);
  }

  /**
   * Create a non-overlapping IntervalsSource
   *
   * Returns intervals of the minuend that do not overlap with intervals from the subtrahend

   * @param minuend     the {@link IntervalsSource} to filter
   * @param subtrahend  the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource nonOverlapping(IntervalsSource minuend, IntervalsSource subtrahend) {
    return new NonOverlappingIntervalsSource(minuend, subtrahend);
  }

  /**
   * Returns intervals from a source that overlap with intervals from another source
   * @param source      the source to filter
   * @param reference   the source to filter by
   */
  public static IntervalsSource overlapping(IntervalsSource source, IntervalsSource reference) {
    return new OverlappingIntervalsSource(source, reference);
  }

  /**
   * Create a not-within {@link IntervalsSource}
   *
   * Returns intervals of the minuend that do not appear within a set number of positions of
   * intervals from the subtrahend query
   *
   * @param minuend     the {@link IntervalsSource} to filter
   * @param positions   the minimum distance that intervals from the minuend may occur from intervals
   *                    of the subtrahend
   * @param subtrahend  the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource notWithin(IntervalsSource minuend, int positions, IntervalsSource subtrahend) {
    return new NonOverlappingIntervalsSource(minuend, Intervals.extend(subtrahend, positions, positions));
  }

  /**
   * Returns intervals of the source that appear within a set number of positions of intervals from
   * the reference
   *
   * @param source    the {@link IntervalsSource} to filter
   * @param positions the maximum distance that intervals of the source may occur from intervals of the reference
   * @param reference the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource within(IntervalsSource source, int positions, IntervalsSource reference) {
    return containedBy(source, Intervals.extend(reference, positions, positions));
  }

  /**
   * Create a not-containing {@link IntervalsSource}
   *
   * Returns intervals from the minuend that do not contain intervals of the subtrahend
   *
   * @param minuend     the {@link IntervalsSource} to filter
   * @param subtrahend  the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource notContaining(IntervalsSource minuend, IntervalsSource subtrahend) {
    return NotContainingIntervalsSource.build(minuend, subtrahend);
  }

  /**
   * Create a containing {@link IntervalsSource}
   *
   * Returns intervals from the big source that contain one or more intervals from
   * the small source
   *
   * @param big     the {@link IntervalsSource} to filter
   * @param small   the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource containing(IntervalsSource big, IntervalsSource small) {
    return ContainingIntervalsSource.build(big, small);
  }

  /**
   * Create a not-contained-by {@link IntervalsSource}
   *
   * Returns intervals from the small {@link IntervalsSource} that do not appear within
   * intervals from the big {@link IntervalsSource}.
   *
   * @param small   the {@link IntervalsSource} to filter
   * @param big     the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource notContainedBy(IntervalsSource small, IntervalsSource big) {
    return NotContainedByIntervalsSource.build(small, big);
  }

  /**
   * Create a contained-by {@link IntervalsSource}
   *
   * Returns intervals from the small query that appear within intervals of the big query
   *
   * @param small     the {@link IntervalsSource} to filter
   * @param big       the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource containedBy(IntervalsSource small, IntervalsSource big) {
    return ContainedByIntervalsSource.build(small, big);
  }

  /**
   * Return intervals that span combinations of intervals from {@code minShouldMatch} of the sources
   */
  public static IntervalsSource atLeast(int minShouldMatch, IntervalsSource... sources) {
    if (minShouldMatch == sources.length) {
      return unordered(sources);
    }
    if (minShouldMatch > sources.length) {
      return new NoMatchIntervalsSource(
          "Too few sources to match minimum of ["
              + minShouldMatch
              + "]: "
              + Arrays.toString(sources));
    }
    return new MinimumShouldMatchIntervalsSource(sources, minShouldMatch);
  }

  /**
   * Returns intervals from the source that appear before intervals from the reference
   */
  public static IntervalsSource before(IntervalsSource source, IntervalsSource reference) {
    return ContainedByIntervalsSource.build(source,
        Intervals.extend(new OffsetIntervalsSource(reference, true), Integer.MAX_VALUE, 0));
  }

  /**
   * Returns intervals from the source that appear after intervals from the reference
   */
  public static IntervalsSource after(IntervalsSource source, IntervalsSource reference) {
    return ContainedByIntervalsSource.build(source,
        Intervals.extend(new OffsetIntervalsSource(reference, false), 0, Integer.MAX_VALUE));
  }

}
