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

import java.util.Arrays;

import org.apache.lucene.util.BytesRef;

/**
 * Constructor functions for interval-based queries
 *
 * These queries use {@link IntervalFunction} or {@link DifferenceIntervalFunction}
 * classes, implementing minimum-interval algorithms taken from the paper
 * <a href="http://vigna.di.unimi.it/ftp/papers/EfficientAlgorithmsMinimalIntervalSemantics.pdf">
 * Efficient Optimally Lazy Algorithms for Minimal-Interval Semantics</a>
 */
public final class Intervals {

  private Intervals() {}

  public static IntervalsSource term(BytesRef term) {
    return new TermIntervalsSource(term);
  }

  public static IntervalsSource term(String term) {
    return new TermIntervalsSource(new BytesRef(term));
  }

  public static IntervalsSource phrase(String... terms) {
    IntervalsSource[] sources = new IntervalsSource[terms.length];
    int i = 0;
    for (String term : terms) {
      sources[i] = term(term);
      i++;
    }
    return orderedNear(0, sources);
  }

  public static IntervalsSource or(IntervalsSource... subSources) {
    if (subSources.length == 1)
      return subSources[0];
    return new DisjunctionIntervalsSource(Arrays.asList(subSources));
  }

  /**
   * Create an ordered {@link IntervalsSource} with a maximum width
   *
   * Returns intervals in which the subsources all appear in the given order, and
   * in which the width of the interval over which the subsources appear is less than
   * the defined width
   *
   * @param width       the maximum width of subquery-spanning intervals that will match
   * @param subSources  an ordered set of {@link IntervalsSource} objects
   */
  public static IntervalsSource orderedNear(int width, IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), new IntervalFunction.OrderedNearFunction(0, width));
  }

  /**
   * Create an ordered {@link IntervalsSource} with a defined width range
   *
   * Returns intervals in which the subsources all appear in the given order, and in
   * which the width of the interval over which the subsources appear is between the
   * minimum and maximum defined widths
   *
   * @param minWidth    the minimum width of subquery-spanning intervals that will match
   * @param maxWidth    the maximum width of subquery-spanning intervals that will match
   * @param subSources  an ordered set of {@link IntervalsSource} objects
   */
  public static IntervalsSource orderedNear(int minWidth, int maxWidth, IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), new IntervalFunction.OrderedNearFunction(minWidth, maxWidth));
  }

  /**
   * Create an ordered {@link IntervalsSource} with an unbounded width range
   *
   * Returns intervals in which the subsources all appear in the given order
   *
   * @param subSources  an ordered set of {@link IntervalsSource} objects
   */
  public static IntervalsSource ordered(IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), IntervalFunction.ORDERED);
  }

  /**
   * Create an unordered {@link IntervalsSource} with a maximum width
   *
   * Returns intervals in which the subsources all appear in any order, and in which
   * the width of the interval over which the subsources appear is less than the
   * defined width
   *
   * @param width       the maximum width of subquery-spanning intervals that will match
   * @param subSources  an unordered set of queries
   */
  public static IntervalsSource unorderedNear(int width, IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), new IntervalFunction.UnorderedNearFunction(0, width));
  }

  /**
   * Create an unordered {@link IntervalsSource} with a defined width range
   *
   * Returns intervals in which the subsources all appear in any order, and in which
   * the width of the interval over which the subsources appear is between the minimum
   * and maximum defined widths
   *
   * @param minWidth    the minimum width of subquery-spanning intervals that will match
   * @param maxWidth    the maximum width of subquery-spanning intervals that will match
   * @param subSources  an unordered set of subsources
   */
  public static IntervalsSource unorderedNear(int minWidth, int maxWidth, IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), new IntervalFunction.UnorderedNearFunction(minWidth, maxWidth));
  }

  /**
   * Create an unordered {@link IntervalsSource} with an unbounded width range
   *
   * Returns intervals in which all the subsources appear.
   *
   * @param subSources  an unordered set of queries
   */
  public static IntervalsSource unordered(IntervalsSource... subSources) {
    return new ConjunctionIntervalsSource(Arrays.asList(subSources), IntervalFunction.UNORDERED);
  }

  /**
   * Create a non-overlapping IntervalsSource
   *
   * Returns intervals of the minuend that do not overlap with intervals from the subtrahend

   * @param minuend     the {@link IntervalsSource} to filter
   * @param subtrahend  the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource nonOverlapping(IntervalsSource minuend, IntervalsSource subtrahend) {
    return new DifferenceIntervalsSource(minuend, subtrahend, DifferenceIntervalFunction.NON_OVERLAPPING);
  }

  /**
   * Create a not-within {@link IntervalsSource}
   *
   * Returns intervals of the minuend that do not appear within a set number of positions of
   * intervals from the subtrahend query
   *
   * @param minuend     the {@link IntervalsSource} to filter
   * @param positions   the maximum distance that intervals from the minuend may occur from intervals
   *                    of the subtrahend
   * @param subtrahend  the {@link IntervalsSource} to filter by
   */
  public static IntervalsSource notWithin(IntervalsSource minuend, int positions, IntervalsSource subtrahend) {
    return new DifferenceIntervalsSource(minuend, subtrahend, new DifferenceIntervalFunction.NotWithinFunction(positions));
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
    return new DifferenceIntervalsSource(minuend, subtrahend, DifferenceIntervalFunction.NOT_CONTAINING);
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
    return new ConjunctionIntervalsSource(Arrays.asList(big, small), IntervalFunction.CONTAINING);
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
    return new DifferenceIntervalsSource(small, big, DifferenceIntervalFunction.NOT_CONTAINED_BY);
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
    return new ConjunctionIntervalsSource(Arrays.asList(small, big), IntervalFunction.CONTAINED_BY);
  }

  // TODO: beforeQuery, afterQuery, arbitrary IntervalFunctions

}
