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
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.QueryVisitor;

/** An IntervalsSource that filters the intervals from another IntervalsSource */
public abstract class FilteredIntervalsSource extends IntervalsSource {

  public static IntervalsSource maxGaps(IntervalsSource in, int maxGaps) {
    return Intervals.or(
        in.pullUpDisjunctions().stream()
            .map(s -> new MaxGaps(s, maxGaps))
            .collect(Collectors.toList()));
  }

  private static class MaxGaps extends FilteredIntervalsSource {

    private final int maxGaps;

    MaxGaps(IntervalsSource in, int maxGaps) {
      super("MAXGAPS/" + maxGaps, in);
      this.maxGaps = maxGaps;
    }

    @Override
    protected boolean accept(IntervalIterator it) {
      return it.gaps() <= maxGaps;
    }
  }

  public static IntervalsSource maxWidth(IntervalsSource in, int maxWidth) {
    return new MaxWidth(in, maxWidth);
  }

  private static class MaxWidth extends FilteredIntervalsSource {

    private final int maxWidth;

    MaxWidth(IntervalsSource in, int maxWidth) {
      super("MAXWIDTH/" + maxWidth, in);
      this.maxWidth = maxWidth;
    }

    @Override
    protected boolean accept(IntervalIterator it) {
      return (it.end() - it.start()) + 1 <= maxWidth;
    }

    @Override
    public Collection<IntervalsSource> pullUpDisjunctions() {
      return Disjunctions.pullUp(in, s -> new MaxWidth(s, maxWidth));
    }
  }

  private final String name;
  protected final IntervalsSource in;

  /**
   * Create a new FilteredIntervalsSource
   *
   * @param name the name of the filter
   * @param in the source to filter
   */
  public FilteredIntervalsSource(String name, IntervalsSource in) {
    this.name = name;
    this.in = in;
  }

  /** @return {@code false} if the current interval should be filtered out */
  protected abstract boolean accept(IntervalIterator it);

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator i = in.intervals(field, ctx);
    if (i == null) {
      return null;
    }
    return new IntervalFilter(i) {
      @Override
      protected boolean accept() {
        return FilteredIntervalsSource.this.accept(in);
      }
    };
  }

  @Override
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc)
      throws IOException {
    IntervalMatchesIterator mi = in.matches(field, ctx, doc);
    if (mi == null) {
      return null;
    }
    IntervalIterator filtered =
        new IntervalFilter(IntervalMatches.wrapMatches(mi, doc)) {
          @Override
          protected boolean accept() {
            return FilteredIntervalsSource.this.accept(in);
          }
        };
    return IntervalMatches.asMatches(filtered, mi, doc);
  }

  @Override
  public int minExtent() {
    return in.minExtent();
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singletonList(this);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    in.visit(field, visitor);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || o instanceof FilteredIntervalsSource == false) return false;
    FilteredIntervalsSource that = (FilteredIntervalsSource) o;
    return Objects.equals(name, that.name) && Objects.equals(in, that.in);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, in);
  }

  @Override
  public String toString() {
    return name + "(" + in + ")";
  }
}
