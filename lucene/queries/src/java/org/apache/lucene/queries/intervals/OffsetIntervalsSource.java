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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.QueryVisitor;

/**
 * Tracks a reference intervals source, and produces a pseudo-interval that appears either one
 * position before or one position after each interval from the reference
 */
class OffsetIntervalsSource extends IntervalsSource {

  private final IntervalsSource in;
  private final boolean before;

  OffsetIntervalsSource(IntervalsSource in, boolean before) {
    this.in = in;
    this.before = before;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator it = in.intervals(field, ctx);
    if (it == null) {
      return null;
    }
    return offset(it);
  }

  private IntervalIterator offset(IntervalIterator it) {
    if (before) {
      return new OffsetIntervalIterator(it) {
        @Override
        public int start() {
          int pos = in.start();
          if (pos == -1) {
            return -1;
          }
          if (pos == NO_MORE_INTERVALS) {
            return NO_MORE_INTERVALS;
          }
          return Math.max(0, pos - 1);
        }
      };
    } else {
      return new OffsetIntervalIterator(it) {
        @Override
        public int start() {
          int pos = in.end() + 1;
          if (pos == 0) {
            return -1;
          }
          if (pos < 0) { // overflow
            return Integer.MAX_VALUE;
          }
          if (pos == Integer.MAX_VALUE) {
            return Integer.MAX_VALUE - 1;
          }
          return pos;
        }
      };
    }
  }

  private abstract static class OffsetIntervalIterator extends IntervalIterator {

    final IntervalIterator in;

    OffsetIntervalIterator(IntervalIterator in) {
      this.in = in;
    }

    @Override
    public int end() {
      return start();
    }

    @Override
    public int gaps() {
      return 0;
    }

    @Override
    public int nextInterval() throws IOException {
      in.nextInterval();
      return start();
    }

    @Override
    public float matchCost() {
      return in.matchCost();
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return in.advance(target);
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  @Override
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc)
      throws IOException {
    IntervalMatchesIterator mi = in.matches(field, ctx, doc);
    if (mi == null) {
      return null;
    }
    return IntervalMatches.asMatches(offset(IntervalMatches.wrapMatches(mi, doc)), mi, doc);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    in.visit(
        field, visitor.getSubVisitor(BooleanClause.Occur.MUST, new IntervalQuery(field, this)));
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
    OffsetIntervalsSource that = (OffsetIntervalsSource) o;
    return before == that.before && Objects.equals(in, that.in);
  }

  @Override
  public int hashCode() {
    return Objects.hash(in, before);
  }

  @Override
  public String toString() {
    if (before) {
      return ("PRECEDING(" + in + ")");
    }
    return ("FOLLOWING(" + in + ")");
  }
}
