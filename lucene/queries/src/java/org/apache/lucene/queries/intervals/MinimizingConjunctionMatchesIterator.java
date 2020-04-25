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
import java.util.List;

import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.MatchesUtils;
import org.apache.lucene.search.Query;

class MinimizingConjunctionMatchesIterator implements IntervalMatchesIterator {

  final IntervalIterator iterator;
  private final List<CachingMatchesIterator> subs = new ArrayList<>();
  private boolean cached = true;

  MinimizingConjunctionMatchesIterator(IntervalIterator iterator, List<IntervalMatchesIterator> subs) {
    this.iterator = iterator;
    for (MatchesIterator mi : subs) {
      assert mi instanceof CachingMatchesIterator;
      this.subs.add((CachingMatchesIterator)mi);
    }
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
    for (CachingMatchesIterator s : subs) {
      start = Math.min(start, s.startOffset(endPos));
    }
    return start;
  }

  @Override
  public int endOffset() throws IOException {
    int end = 0;
    int endPos = endPosition();
    for (CachingMatchesIterator s : subs) {
      end = Math.max(end, s.endOffset(endPos));
    }
    return end;
  }

  @Override
  public int gaps() {
    return iterator.gaps();
  }

  @Override
  public int width() {
    return iterator.width();
  }

  @Override
  public MatchesIterator getSubMatches() throws IOException {
    List<MatchesIterator> mis = new ArrayList<>();
    int endPos = endPosition();
    for (CachingMatchesIterator s : subs) {
      mis.add(s.getSubMatches(endPos));
    }
    return MatchesUtils.disjunction(mis);
  }

  @Override
  public Query getQuery() {
    return null;
  }
}
