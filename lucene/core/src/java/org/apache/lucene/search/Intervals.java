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

  /**
   * Create an ordered query with a maximum width
   *
   * Matches documents in which the subqueries all match in the given order, and
   * in which the width of the interval over which the queries match is less than
   * the defined width
   *
   * @param field       the field to query
   * @param width       the maximum width of subquery-spanning intervals that will match
   * @param subQueries  an ordered set of subqueries
   */
  public static Query orderedQuery(String field, int width, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.OrderedNearFunction(0, width));
  }

  /**
   * Create an ordered query with a defined width range
   *
   * Matches documents in which the subqueries all match in the given order, and in
   * which the width of the interval over which the queries match is between the
   * minimum and maximum defined widths
   *
   * @param field       the field to query
   * @param minWidth    the minimum width of subquery-spanning intervals that will match
   * @param maxWidth    the maximum width of subquery-spanning intervals that will match
   * @param subQueries  an ordered set of subqueries
   */
  public static Query orderedQuery(String field, int minWidth, int maxWidth, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.OrderedNearFunction(minWidth, maxWidth));
  }

  /**
   * Create an ordered query with an unbounded width range
   *
   * Matches documents in which the subqueries all match in the given order
   *
   * @param field       the field to query
   * @param subQueries  an ordered set of subqueries
   */
  public static Query orderedQuery(String field, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), IntervalFunction.ORDERED);
  }

  /**
   * Create an unordered query with a maximum width
   *
   * Matches documents in which the subqueries all match in any order, and in which
   * the width of the interval over which the queries match is less than the
   * defined width
   *
   * @param field       the field to query
   * @param width       the maximum width of subquery-spanning intervals that will match
   * @param subQueries  an unordered set of queries
   */
  public static Query unorderedQuery(String field, int width, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.UnorderedNearFunction(0, width));
  }

  /**
   * Create an unordered query with a defined width range
   *
   * Matches documents in which the subqueries all match in any order, and in which
   * the width of the interval over which the queries match is between the minimum
   * and maximum defined widths
   *
   * @param field       the field to query
   * @param minWidth    the minimum width of subquery-spanning intervals that will match
   * @param maxWidth    the maximum width of subquery-spanning intervals that will match
   * @param subQueries  an unordered set of queries
   */
  public static Query unorderedQuery(String field, int minWidth, int maxWidth, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.UnorderedNearFunction(minWidth, maxWidth));
  }

  /**
   * Create an unordered query with an unbounded width range
   *
   * Matches documents in which all the subqueries match.  This is essence a pure conjunction
   * query, but it will expose iterators over those conjunctions that may then be further
   * nested in other interval queries
   *
   * @param field       the field to query
   * @param subQueries  an unordered set of queries
   */
  public static Query unorderedQuery(String field, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), IntervalFunction.UNORDERED);
  }

  /**
   * Create a non-overlapping query
   *
   * Matches documents that match the minuend query, except when the intervals of the minuend
   * query overlap with intervals from the subtrahend query
   *
   * Exposes matching intervals from the minuend
   *
   * @param field       the field to query
   * @param minuend     the query to filter
   * @param subtrahend  the query to filter by
   */
  public static Query nonOverlappingQuery(String field, Query minuend, Query subtrahend) {
    return new DifferenceIntervalQuery(field, minuend, subtrahend, DifferenceIntervalFunction.NON_OVERLAPPING);
  }

  /**
   * Create a not-within query
   *
   * Matches documents that match the minuend query, except when the intervals of the minuend
   * query appear within a set number of positions of intervals from the subtrahend query
   *
   * Exposes matching intervals from the minuend
   *
   * @param field       the field to query
   * @param minuend     the query to filter
   * @param positions   the maximum distance that intervals from the minuend may occur from intervals
   *                    of the subtrahend
   * @param subtrahend  the query to filter by
   */
  public static Query notWithinQuery(String field, Query minuend, int positions, Query subtrahend) {
    return new DifferenceIntervalQuery(field, minuend, subtrahend, new DifferenceIntervalFunction.NotWithinFunction(positions));
  }

  /**
   * Create a not-containing query
   *
   * Matches documents that match the minuend query, except when the intervals of the minuend
   * query are contained within an interval of the subtrahend query
   *
   * Exposes matching intervals from the minuend
   *
   * @param field       the field to query
   * @param minuend     the query to filter
   * @param subtrahend  the query to filter by
   */
  public static Query notContainingQuery(String field, Query minuend, Query subtrahend) {
    return new DifferenceIntervalQuery(field, minuend, subtrahend, DifferenceIntervalFunction.NOT_CONTAINING);
  }

  /**
   * Create a containing query
   *
   * Matches documents where intervals of the big query contain one or more intervals from
   * the small query
   *
   * Exposes matching intervals from the big query
   *
   * @param field   the field to query
   * @param big     the query to filter
   * @param small   the query to filter by
   */
  public static Query containingQuery(String field, Query big, Query small) {
    return new IntervalQuery(field, Arrays.asList(big, small), IntervalFunction.CONTAINING);
  }

  /**
   * Create a not-contained-by query
   *
   * Matches documents that match the small query, except when the intervals of the small
   * query are contained within an interval of the big query
   *
   * Exposes matching intervals from the small query
   *
   * @param field   the field to query
   * @param small   the query to filter
   * @param big     the query to filter by
   */
  public static Query notContainedByQuery(String field, Query small, Query big) {
    return new DifferenceIntervalQuery(field, small, big, DifferenceIntervalFunction.NOT_CONTAINED_BY);
  }

  /**
   * Create a contained-by query
   *
   * Matches documents where intervals of the small query occur within intervals of the big query
   *
   * Exposes matching intervals from the small query
   *
   * @param field     the field to query
   * @param small     the query to filter
   * @param big       the query to filter by
   */
  public static Query containedByQuery(String field, Query small, Query big) {
    return new IntervalQuery(field, Arrays.asList(small, big), IntervalFunction.CONTAINED_BY);
  }

  // TODO: beforeQuery, afterQuery, arbitrary IntervalFunctions

}
