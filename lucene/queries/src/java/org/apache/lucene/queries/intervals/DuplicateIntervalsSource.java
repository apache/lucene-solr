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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

class DuplicateIntervalsSource extends IntervalsSource {

  static IntervalsSource build(IntervalsSource in, int childCount) {
    if (childCount == 1) {
      return in;
    }
    assert childCount > 0;
    return new DuplicateIntervalsSource(in, childCount);
  }

  final IntervalsSource in;
  final int childCount;

  private DuplicateIntervalsSource(IntervalsSource in, int childCount) {
    this.in = in;
    this.childCount = childCount;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator it = in.intervals(field, ctx);
    if (it == null) {
      return null;
    }
    return new DuplicateIntervalIterator(it, childCount);
  }

  @Override
  public IntervalMatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<IntervalMatchesIterator> subs = new ArrayList<>();
    for (int i = 0; i < childCount; i++) {
      IntervalMatchesIterator mi = in.matches(field, ctx, doc);
      if (mi == null) {
        return null;
      }
      subs.add(mi);
    }
    return DuplicateMatchesIterator.build(subs);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    in.visit(field, visitor);
  }

  @Override
  public int minExtent() {
    return in.minExtent();
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singleton(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(in, childCount);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DuplicateIntervalsSource == false) return false;
    DuplicateIntervalsSource o = (DuplicateIntervalsSource) other;
    return Objects.equals(this.in, o.in) && Objects.equals(this.childCount, o.childCount);
  }

  @Override
  public String toString() {
    String s = in.toString();
    StringBuilder out = new StringBuilder(s);
    for (int i = 1; i < childCount; i++) {
      out.append(",").append(s);
    }
    return out.toString();
  }

  private static class DuplicateIntervalIterator extends IntervalIterator {

    private final IntervalIterator in;
    final int[] cache;
    final int cacheLength;
    int cacheBase;
    boolean started = false;
    boolean exhausted = false;

    private DuplicateIntervalIterator(IntervalIterator primary, int copies) {
      this.in = primary;
      this.cacheLength = copies;
      this.cache = new int[this.cacheLength * 2];
    }

    @Override
    public int start() {
      return exhausted ? NO_MORE_INTERVALS : cache[(cacheBase % cacheLength) * 2];
    }

    @Override
    public int end() {
      return exhausted ? NO_MORE_INTERVALS : cache[(((cacheBase + cacheLength - 1) % cacheLength) * 2) + 1];
    }

    @Override
    public int width() {
      int width = 0;
      for (int i = 0; i < cacheLength; i++) {
        int pos = (cacheBase + i) % cacheLength;
        width += cache[pos * 2] - cache[pos * 2 + 1] + 1;
      }
      return width;
    }

    @Override
    public int gaps() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextInterval() throws IOException {
      if (exhausted) {
        return NO_MORE_INTERVALS;
      }
      if (started == false) {
        for (int i = 0; i < cacheLength; i++) {
          if (cacheNextInterval(i) == NO_MORE_INTERVALS) {
            return NO_MORE_INTERVALS;
          }
        }
        cacheBase = 0;
        started = true;
        return start();
      }
      else {
        int insert = (cacheBase + cacheLength) % cacheLength;
        cacheBase = (cacheBase + 1) % cacheLength;
        return cacheNextInterval(insert);
      }
    }

    private int cacheNextInterval(int linePos) throws IOException {
      if (in.nextInterval() == NO_MORE_INTERVALS) {
        exhausted = true;
        return NO_MORE_INTERVALS;
      }
      cache[linePos * 2] = in.start();
      cache[linePos * 2 + 1] = in.end();
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
      started = exhausted = false;
      Arrays.fill(cache, -1);
      return in.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      started = exhausted = false;
      Arrays.fill(cache, -1);
      return in.advance(target);
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  private static class DuplicateMatchesIterator implements IntervalMatchesIterator {

    List<IntervalMatchesIterator> subs;
    boolean cached = false;

    static IntervalMatchesIterator build(List<IntervalMatchesIterator> subs) throws IOException {
      int count = subs.size();
      while (count > 0) {
        for (int i = 0; i < count; i++) {
          if (subs.get(count - 1).next() == false) {
            return null;
          }
        }
        count--;
      }
      return new DuplicateMatchesIterator(subs);
    }

    private DuplicateMatchesIterator(List<IntervalMatchesIterator> subs) throws IOException {
      this.subs = subs;
    }

    @Override
    public boolean next() throws IOException {
      if (cached == false) {
        return cached = true;
      }
      if (subs.get(subs.size() - 1).next() == false) {
        return false;
      }
      for (int i = 0; i < subs.size() - 1; i++) {
        subs.get(i).next();
      }
      return true;
    }

    @Override
    public int startPosition() {
      return subs.get(0).startPosition();
    }

    @Override
    public int endPosition() {
      return subs.get(subs.size() - 1).endPosition();
    }

    @Override
    public int startOffset() throws IOException {
      return subs.get(0).startOffset();
    }

    @Override
    public int endOffset() throws IOException {
      return subs.get(subs.size() - 1).endOffset();
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
      List<MatchesIterator> subMatches = new ArrayList<>();
      for (MatchesIterator mi : subs) {
        MatchesIterator sub = mi.getSubMatches();
        if (sub == null) {
          sub = new ConjunctionIntervalsSource.SingletonMatchesIterator(mi);
        }
        subMatches.add(sub);
      }
      return MatchesUtils.disjunction(subMatches);
    }

    @Override
    public Query getQuery() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int gaps() {
      return 0;
    }

    @Override
    public int width() {
      int width = endPosition() - startPosition() + 1;
      for (MatchesIterator mi : subs) {
        width = width - (mi.endPosition() - mi.startPosition() + 1);
      }
      return width;
    }
  }
}
