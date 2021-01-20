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
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;

final class IntervalMatches {

  static IntervalMatchesIterator asMatches(
      IntervalIterator iterator, IntervalMatchesIterator source, int doc) throws IOException {
    if (source == null) {
      return null;
    }
    if (iterator.advance(doc) != doc) {
      return null;
    }
    if (iterator.nextInterval() == IntervalIterator.NO_MORE_INTERVALS) {
      return null;
    }
    return new IntervalMatchesIterator() {

      boolean cached = true;

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
        return source.startOffset();
      }

      @Override
      public int endOffset() throws IOException {
        return source.endOffset();
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
        return source.getSubMatches();
      }

      @Override
      public Query getQuery() {
        return source.getQuery();
      }
    };
  }

  enum State {
    UNPOSITIONED,
    ITERATING,
    NO_MORE_INTERVALS,
    EXHAUSTED
  }

  static IntervalIterator wrapMatches(IntervalMatchesIterator mi, int doc) {
    return new IntervalIterator() {

      State state = State.UNPOSITIONED;

      @Override
      public int start() {
        if (state == State.NO_MORE_INTERVALS) {
          return NO_MORE_INTERVALS;
        }
        assert state == State.ITERATING;
        return mi.startPosition();
      }

      @Override
      public int end() {
        if (state == State.NO_MORE_INTERVALS) {
          return NO_MORE_INTERVALS;
        }
        assert state == State.ITERATING;
        return mi.endPosition();
      }

      @Override
      public int gaps() {
        assert state == State.ITERATING;
        return mi.gaps();
      }

      @Override
      public int width() {
        assert state == State.ITERATING;
        return mi.width();
      }

      @Override
      public int nextInterval() throws IOException {
        assert state == State.ITERATING;
        if (mi.next()) {
          return mi.startPosition();
        }
        state = State.NO_MORE_INTERVALS;
        return NO_MORE_INTERVALS;
      }

      @Override
      public float matchCost() {
        return 1;
      }

      @Override
      public int docID() {
        switch (state) {
          case UNPOSITIONED:
            return -1;
          case ITERATING:
          case NO_MORE_INTERVALS:
            return doc;
          case EXHAUSTED:
        }
        return NO_MORE_DOCS;
      }

      @Override
      public int nextDoc() {
        switch (state) {
          case UNPOSITIONED:
            state = State.ITERATING;
            return doc;
          case ITERATING:
          case NO_MORE_INTERVALS:
            state = State.EXHAUSTED;
            break;
          case EXHAUSTED:
        }
        return NO_MORE_DOCS;
      }

      @Override
      public int advance(int target) {
        if (target == doc) {
          state = State.ITERATING;
          return doc;
        }
        state = State.EXHAUSTED;
        return NO_MORE_DOCS;
      }

      @Override
      public long cost() {
        return 1;
      }
    };
  }
}
