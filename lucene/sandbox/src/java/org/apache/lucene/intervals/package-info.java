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

package org.apache.lucene.intervals;

/**
 * <h2>Intervals queries</h2>
 *
 * This package contains experimental classes to search over intervals within fields
 *
 * <h2>IntervalsSource</h2>
 *
 * The {@link org.apache.lucene.intervals.IntervalsSource} class can be used to construct proximity
 * relationships between terms and intervals.  They can be built using static methods
 * in the {@link org.apache.lucene.intervals.Intervals} class
 *
 * <h3>Basic intervals</h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.intervals.Intervals#term(String)} &mdash; Represents a single term</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#phrase(java.lang.String...)} &mdash; Represents a phrase</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#ordered(org.apache.lucene.intervals.IntervalsSource...)}
 *        &mdash; Represents an interval over an ordered set of terms or intervals</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#unordered(org.apache.lucene.intervals.IntervalsSource...)}
 *        &mdash; Represents an interval over an unordered set of terms or intervals</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#or(org.apache.lucene.intervals.IntervalsSource...)}
 *        &mdash; Represents the disjunction of a set of terms or intervals</li>
 * </ul>
 *
 * <h3>Filters</h3>
 *
 * <ul>
 *   <li>{@link org.apache.lucene.intervals.Intervals#maxwidth(int, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Filters out intervals that are larger than a set width</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#containedBy(org.apache.lucene.intervals.IntervalsSource, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that are contained by another interval</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#notContainedBy(org.apache.lucene.intervals.IntervalsSource, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that are *not* contained by another interval</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#containing(org.apache.lucene.intervals.IntervalsSource, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that contain another interval</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#notContaining(org.apache.lucene.intervals.IntervalsSource, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that do not contain another interval</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#nonOverlapping(org.apache.lucene.intervals.IntervalsSource, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that do not overlap with another interval</li>
 *   <li>{@link org.apache.lucene.intervals.Intervals#notWithin(org.apache.lucene.intervals.IntervalsSource, int, org.apache.lucene.intervals.IntervalsSource)}
 *          &mdash; Returns intervals that do not appear within a set number of positions of another interval</li>
 * </ul>
 *
 * <h2>IntervalQuery</h2>
 *
 * An {@link org.apache.lucene.intervals.IntervalQuery} takes a field name and an {@link org.apache.lucene.intervals.IntervalsSource},
 * and matches all documents that contain intervals defined by the source in that field.
 */