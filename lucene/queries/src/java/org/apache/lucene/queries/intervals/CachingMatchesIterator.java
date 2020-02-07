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

import org.apache.lucene.search.FilterMatchesIterator;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;

class CachingMatchesIterator extends FilterMatchesIterator implements IntervalMatchesIterator {

  private boolean positioned = false;
  private int[] posAndOffsets = new int[4*4];
  private Query[] matchingQueries = new Query[4];
  private int count = 0; 

  CachingMatchesIterator(IntervalMatchesIterator in) {
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
      matchingQueries [0] = in.getQuery();
    }
    else {
      while (mi.next()) {
        if (count * 4 >= posAndOffsets.length) {
          posAndOffsets = ArrayUtil.grow(posAndOffsets, (count + 1) * 4);
          matchingQueries = ArrayUtil.grow(matchingQueries, (count + 1));
        }
        posAndOffsets[count * 4] = mi.startPosition();
        posAndOffsets[count * 4 + 1] = mi.endPosition();
        posAndOffsets[count * 4 + 2] = mi.startOffset();
        posAndOffsets[count * 4 + 3] = mi.endOffset();
        matchingQueries[count] = mi.getQuery();
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
        return matchingQueries[upto];
      }
    };
  }

  @Override
  public int gaps() {
    return ((IntervalMatchesIterator)in).gaps();
  }

  @Override
  public int width() {
    return ((IntervalMatchesIterator)in).width();
  }
}
