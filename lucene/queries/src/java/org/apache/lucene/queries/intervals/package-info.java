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

/**
 * <h2>Intervals queries</h2>
 *
 * This package contains experimental classes to search over intervals within fields
 *
 * <h2>IntervalsSource</h2>
 *
 * The {@link org.apache.lucene.queries.intervals.IntervalsSource} class can be used to construct proximity
 * relationships between terms and intervals.  They can be built using static methods
 * in the {@link org.apache.lucene.queries.intervals.Intervals} class
 *
 * <h3>Basic intervals</h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#term(String)} &mdash; Represents a single term</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#phrase(java.lang.String...)} &mdash; Represents a phrase</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#ordered(IntervalsSource...)}
 *        &mdash; Represents an interval over an ordered set of terms or intervals</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#unordered(IntervalsSource...)}
 *        &mdash; Represents an interval over an unordered set of terms or intervals</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#or(IntervalsSource...)}
 *        &mdash; Represents the disjunction of a set of terms or intervals</li>
 * </ul>
 *
 * <h3>Filters</h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#maxwidth(int, IntervalsSource)}
 *          &mdash; Filters out intervals that are larger than a set width</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#maxgaps(int, IntervalsSource)}
 *          &mdash; Filters out intervals that have more than a set number of gaps between their constituent sub-intervals</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#containedBy(IntervalsSource, IntervalsSource)}
 *          &mdash; Returns intervals that are contained by another interval</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#notContainedBy(IntervalsSource, IntervalsSource)}
 *          &mdash; Returns intervals that are *not* contained by another interval</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#containing(IntervalsSource, IntervalsSource)}
 *          &mdash; Returns intervals that contain another interval</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#notContaining(IntervalsSource, IntervalsSource)}
 *          &mdash; Returns intervals that do not contain another interval</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#nonOverlapping(IntervalsSource, IntervalsSource)}
 *          &mdash; Returns intervals that do not overlap with another interval</li>
 *   <li>{@link org.apache.lucene.queries.intervals.Intervals#notWithin(IntervalsSource, int, IntervalsSource)}
 *          &mdash; Returns intervals that do not appear within a set number of positions of another interval</li>
 * </ul>
 *
 * <h2>IntervalQuery</h2>
 *
 * An {@link org.apache.lucene.queries.intervals.IntervalQuery} takes a field name and an {@link org.apache.lucene.queries.intervals.IntervalsSource},
 * and matches all documents that contain intervals defined by the source in that field.
 */
package org.apache.lucene.queries.intervals;