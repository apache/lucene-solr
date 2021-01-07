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

/**
 * Wraps an IntervalIterator and extends the bounds of its intervals
 *
 * <p>Useful for specifying gaps in an ordered iterator; if you want to match `a b [2 spaces] c`,
 * you can search for phrase(a, extended(b, 0, 2), c)
 *
 * <p>An interval with prefix bounds extended by n will skip over matches that appear in positions
 * lower than n
 */
class ExtendedIntervalIterator extends IntervalIterator {

  private final IntervalIterator in;
  private final int before;
  private final int after;

  private boolean positioned;

  /**
   * Create a new ExtendedIntervalIterator
   *
   * @param in the iterator to wrap
   * @param before the number of positions to extend before the delegated interval
   * @param after the number of positions to extend beyond the delegated interval
   */
  ExtendedIntervalIterator(IntervalIterator in, int before, int after) {
    this.in = in;
    this.before = before;
    this.after = after;
  }

  @Override
  public int start() {
    if (positioned == false) {
      return -1;
    }
    int start = in.start();
    if (start == NO_MORE_INTERVALS) {
      return NO_MORE_INTERVALS;
    }
    return Math.max(0, start - before);
  }

  @Override
  public int end() {
    if (positioned == false) {
      return -1;
    }
    int end = in.end();
    if (end == NO_MORE_INTERVALS) {
      return NO_MORE_INTERVALS;
    }
    end += after;
    if (end < 0 || end == NO_MORE_INTERVALS) {
      // overflow
      end = NO_MORE_INTERVALS - 1;
    }
    return end;
  }

  @Override
  public int gaps() {
    return in.gaps();
  }

  @Override
  public int nextInterval() throws IOException {
    positioned = true;
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
    positioned = false;
    return in.nextDoc();
  }

  @Override
  public int advance(int target) throws IOException {
    positioned = false;
    return in.advance(target);
  }

  @Override
  public long cost() {
    return in.cost();
  }
}
