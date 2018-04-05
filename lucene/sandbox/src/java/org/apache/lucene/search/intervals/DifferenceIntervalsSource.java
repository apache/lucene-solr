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

class DifferenceIntervalsSource extends IntervalsSource {

  final IntervalsSource minuend;
  final IntervalsSource subtrahend;
  final DifferenceIntervalFunction function;

  public DifferenceIntervalsSource(IntervalsSource minuend, IntervalsSource subtrahend, DifferenceIntervalFunction function) {
    this.minuend = minuend;
    this.subtrahend = subtrahend;
    this.function = function;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator minIt = minuend.intervals(field, ctx);
    if (minIt == null)
      return null;
    IntervalIterator subIt = subtrahend.intervals(field, ctx);
    if (subIt == null)
      return minIt;
    return function.apply(minIt, subIt);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DifferenceIntervalsSource that = (DifferenceIntervalsSource) o;
    return Objects.equals(minuend, that.minuend) &&
        Objects.equals(subtrahend, that.subtrahend) &&
        Objects.equals(function, that.function);
  }

  @Override
  public int hashCode() {
    return Objects.hash(minuend, subtrahend, function);
  }

  @Override
  public String toString() {
    return function + "(" + minuend + ", " + subtrahend + ")";
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    minuend.extractTerms(field, terms);
  }
}
