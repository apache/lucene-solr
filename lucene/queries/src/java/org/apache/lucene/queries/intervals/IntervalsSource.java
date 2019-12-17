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
import java.util.Collection;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.QueryVisitor;

/**
 * A helper class for {@link IntervalQuery} that provides an {@link IntervalIterator}
 * for a given field and segment
 *
 * Static constructor functions for various different sources can be found in the
 * {@link Intervals} class
 */
public abstract class IntervalsSource {

  /**
   * Create an {@link IntervalIterator} exposing the minimum intervals defined by this {@link IntervalsSource}
   *
   * Returns {@code null} if no intervals for this field exist in this segment
   *
   * @param field the field to read positions from
   * @param ctx   the context for which to return the iterator
   */
  public abstract IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException;

  /**
   * Return a {@link MatchesIterator} over the intervals defined by this {@link IntervalsSource} for a
   * given document and field
   *
   * Returns {@code null} if no intervals exist in the given document and field
   *
   * @param field the field to read positions from
   * @param ctx   the document's context
   * @param doc   the document to return matches for
   */
  public abstract IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException;

  /**
   * Expert: visit the tree of sources
   */
  public abstract void visit(String field, QueryVisitor visitor);

  /**
   * Return the minimum possible width of an interval returned by this source
   */
  public abstract int minExtent();

  /**
   * Expert: return the set of disjunctions that make up this IntervalsSource
   *
   * Most implementations can return {@code Collections.singleton(this)}
   */
  public abstract Collection<IntervalsSource> pullUpDisjunctions();

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object other);

  @Override
  public abstract String toString();

}
