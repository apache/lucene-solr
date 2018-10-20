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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;

/**
 * A ConjunctionIntervalsSource that attempts to minimize its internal intervals by
 * eagerly advancing its first subinterval
 *
 * Uses caching to expose matches after its first subinterval has been moved on
 */
class MinimizingConjunctionIntervalsSource extends ConjunctionIntervalsSource {

  MinimizingConjunctionIntervalsSource(List<IntervalsSource> subSources, IntervalFunction function) {
    super(subSources, function);
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    List<CacheingMatchesIterator> subs = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      MatchesIterator mi = source.matches(field, ctx, doc);
      if (mi == null) {
        return null;
      }
      subs.add(new CacheingMatchesIterator(mi));
    }
    IntervalIterator it = function.apply(subs.stream().map(m -> IntervalMatches.wrapMatches(m, doc)).collect(Collectors.toList()));
    if (it.advance(doc) != doc) {
      return null;
    }
    if (it.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
      return null;
    }
    return new ConjunctionMatchesIterator(it, subs);
  }

  private static class ConjunctionMatchesIterator implements MatchesIterator {

    final IntervalIterator iterator;
    final List<CacheingMatchesIterator> subs;
    boolean cached = true;

    private ConjunctionMatchesIterator(IntervalIterator iterator, List<CacheingMatchesIterator> subs) {
      this.iterator = iterator;
      this.subs = subs;
    }

    @Override
    public boolean next() throws IOException {
      if (cached) {
        cached = false;
        return true;
      }
      return iterator.nextInterval() != IntervalIterator.NO_MORE_INTERVALS;
    }

    @Override
    public int startPosition() {
      return iterator.start();
    }

    @Override
    public int endPosition() {
      return iterator.end();
    }

    @Override
    public int startOffset() throws IOException {
      int start = Integer.MAX_VALUE;
      int endPos = endPosition();
      for (CacheingMatchesIterator s : subs) {
        start = Math.min(start, s.startOffset(endPos));
      }
      return start;
    }

    @Override
    public int endOffset() throws IOException {
      int end = 0;
      int endPos = endPosition();
      for (CacheingMatchesIterator s : subs) {
        end = Math.max(end, s.endOffset(endPos));
      }
      return end;
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
      List<MatchesIterator> mis = new ArrayList<>();
      int endPos = endPosition();
      for (CacheingMatchesIterator s : subs) {
        mis.add(s.getSubMatches(endPos));
      }
      return MatchesUtils.disjunction(mis);
    }

    @Override
    public Query getQuery() {
      return null;
    }
  }

  private static class CacheingMatchesIterator extends FilterMatchesIterator {

    boolean positioned = false;
    int posAndOffsets[] = new int[16];
    int count = 0;

    CacheingMatchesIterator(MatchesIterator in) {
      super(in);
    }

    private void cache() throws IOException {
      count = 0;
      MatchesIterator mi = in.getSubMatches();
      if (mi == null) {
        count = 1;
        posAndOffsets[0] = in.startPosition();
        posAndOffsets[1] = in.endPosition();
        posAndOffsets[2] = in.startOffset();
        posAndOffsets[3] = in.endOffset();
      }
      else {
        while (mi.next()) {
          if (count * 4 >= posAndOffsets.length) {
            posAndOffsets = ArrayUtil.grow(posAndOffsets, (count + 1) * 4);
          }
          posAndOffsets[count * 4] = mi.startPosition();
          posAndOffsets[count * 4 + 1] = mi.endPosition();
          posAndOffsets[count * 4 + 2] = mi.startOffset();
          posAndOffsets[count * 4 + 3] = mi.endOffset();
          count++;
        }
      }
    }

    @Override
    public boolean next() throws IOException {
      if (positioned == false) {
        positioned = true;
      }
      else {
        cache();
      }
      return in.next();
    }

    int startOffset(int endPos) throws IOException {
      if (endPosition() <= endPos) {
        return in.startOffset();
      }
      return posAndOffsets[2];
    }

    int endOffset(int endPos) throws IOException {
      if (endPosition() <= endPos) {
        return in.endOffset();
      }
      return posAndOffsets[count * 4 + 3];
    }

    MatchesIterator getSubMatches(int endPos) throws IOException {
      if (endPosition() <= endPos) {
        cache();
      }
      return new MatchesIterator() {

        int upto = -1;

        @Override
        public boolean next() {
          upto++;
          return upto < count;
        }

        @Override
        public int startPosition() {
          return posAndOffsets[upto * 4];
        }

        @Override
        public int endPosition() {
          return posAndOffsets[upto * 4 + 1];
        }

        @Override
        public int startOffset() {
          return posAndOffsets[upto * 4 + 2];
        }

        @Override
        public int endOffset() {
          return posAndOffsets[upto * 4 + 3];
        }

        @Override
        public MatchesIterator getSubMatches() {
          return null;
        }

        @Override
        public Query getQuery() {
          throw new UnsupportedOperationException();
        }
      };
    }

  }
}
