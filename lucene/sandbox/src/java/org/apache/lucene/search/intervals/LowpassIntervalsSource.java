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

class LowpassIntervalsSource extends IntervalsSource {

  final IntervalsSource in;
  private final int maxWidth;

  LowpassIntervalsSource(IntervalsSource in, int maxWidth) {
    this.in = in;
    this.maxWidth = maxWidth;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LowpassIntervalsSource that = (LowpassIntervalsSource) o;
    return maxWidth == that.maxWidth &&
        Objects.equals(in, that.in);
  }

  @Override
  public String toString() {
    return "MAXWIDTH/" + maxWidth + "(" + in + ")";
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    in.extractTerms(field, terms);
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    IntervalIterator i = in.intervals(field, ctx);
    if (i == null) {
      return null;
    }
    return new IntervalFilter(i) {
      @Override
      protected boolean accept() {
        return (i.end() - i.start()) + 1 <= maxWidth;
      }
    };
  }

  @Override
  public int hashCode() {
    return Objects.hash(in, maxWidth);
  }
}
