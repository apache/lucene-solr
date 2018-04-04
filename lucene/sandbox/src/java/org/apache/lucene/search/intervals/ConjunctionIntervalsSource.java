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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;

class ConjunctionIntervalsSource extends IntervalsSource {

  final List<IntervalsSource> subSources;
  final IntervalFunction function;

  ConjunctionIntervalsSource(List<IntervalsSource> subSources, IntervalFunction function) {
    this.subSources = subSources;
    this.function = function;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConjunctionIntervalsSource that = (ConjunctionIntervalsSource) o;
    return Objects.equals(subSources, that.subSources) &&
        Objects.equals(function, that.function);
  }

  @Override
  public String toString() {
    return function + subSources.stream().map(Object::toString).collect(Collectors.joining(",", "(", ")"));
  }

  @Override
  public void extractTerms(String field, Set<Term> terms) {
    for (IntervalsSource source : subSources) {
      source.extractTerms(field, terms);
    }
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    List<IntervalIterator> subIntervals = new ArrayList<>();
    for (IntervalsSource source : subSources) {
      IntervalIterator it = source.intervals(field, ctx);
      if (it == null)
        return null;
      subIntervals.add(it);
    }
    return function.apply(subIntervals);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subSources, function);
  }
}
