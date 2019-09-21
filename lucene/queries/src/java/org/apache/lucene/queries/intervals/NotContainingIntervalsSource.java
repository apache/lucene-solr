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

class NotContainingIntervalsSource extends DifferenceIntervalsSource {

  static IntervalsSource build(IntervalsSource minuend, IntervalsSource subtrahend) {
    return Intervals.or(Disjunctions.pullUp(minuend, s -> new NotContainingIntervalsSource(s, subtrahend)));
  }

  private NotContainingIntervalsSource(IntervalsSource minuend, IntervalsSource subtrahend) {
    super(minuend, subtrahend);
  }

  @Override
  protected IntervalIterator combine(IntervalIterator minuend, IntervalIterator subtrahend) {
    return new NotContainingIterator(minuend, subtrahend);
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    return Collections.singletonList(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minuend, subtrahend);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof NotContainingIntervalsSource == false) return false;
    NotContainingIntervalsSource o = (NotContainingIntervalsSource) other;
    return Objects.equals(this.minuend, o.minuend) && Objects.equals(this.subtrahend, o.subtrahend);
  }

  @Override
  public String toString() {
    return "NOT_CONTAINING(" + minuend + "," + subtrahend + ")";
  }

  private static class NotContainingIterator extends RelativeIterator {

    private NotContainingIterator(IntervalIterator minuend, IntervalIterator subtrahend) {
      super(minuend, subtrahend);
    }

    @Override
    public int nextInterval() throws IOException {
      if (bpos == false)
        return a.nextInterval();
      while (a.nextInterval() != NO_MORE_INTERVALS) {
        while (b.start() < a.start() && b.end() < a.end()) {
          if (b.nextInterval() == NO_MORE_INTERVALS) {
            bpos = false;
            return a.start();
          }
        }
        if (b.start() > a.end())
          return a.start();
      }
      return NO_MORE_INTERVALS;
    }

  }
}
