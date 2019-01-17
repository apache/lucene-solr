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
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchesIterator;

class ExtendedIntervalsSource extends IntervalsSource {

  final IntervalsSource source;
  private final int before;
  private final int after;

  ExtendedIntervalsSource(IntervalsSource source, int before, int after) {
    this.source = source;
    this.before = before;
    this.after = after;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator in = source.intervals(field, ctx);
    if (in == null) {
      return null;
    }
    return new ExtendedIntervalIterator(in, before, after);
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    MatchesIterator in = source.matches(field, ctx, doc);
    if (in == null) {
      return null;
    }
    IntervalIterator wrapped = new ExtendedIntervalIterator(IntervalMatches.wrapMatches(in, doc), before, after);
    return IntervalMatches.asMatches(wrapped, in, doc);
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    source.extractTerms(field, terms);
  }

  @Override
  public int minExtent() {
    int minExtent = before + source.minExtent() + after;
    if (minExtent < 0) {
      return Integer.MAX_VALUE;
    }
    return minExtent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ExtendedIntervalsSource that = (ExtendedIntervalsSource) o;
    return before == that.before &&
        after == that.after &&
        Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(source, before, after);
  }

  @Override
  public String toString() {
    return "EXTEND(" + source + "," + before + "," + after + ")";
  }
}
