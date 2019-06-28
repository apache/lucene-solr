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
import java.util.stream.Collectors;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.QueryVisitor;

class FixedFieldIntervalsSource extends IntervalsSource {

  private final String field;
  private final IntervalsSource source;

  FixedFieldIntervalsSource(String field, IntervalsSource source) {
    this.field = field;
    this.source = source;
  }

  @Override
  public IntervalIterator intervals(String field, LeafReaderContext ctx) throws IOException {
    return source.intervals(this.field, ctx);
  }

  @Override
  public MatchesIterator matches(String field, LeafReaderContext ctx, int doc) throws IOException {
    return source.matches(this.field, ctx, doc);
  }

  @Override
  public void visit(String field, QueryVisitor visitor) {
    source.visit(this.field, visitor);
  }

  @Override
  public int minExtent() {
    return source.minExtent();
  }

  @Override
  public Collection<IntervalsSource> pullUpDisjunctions() {
    Collection<IntervalsSource> inner = source.pullUpDisjunctions();
    if (inner.size() == 1) {
      return Collections.singleton(this);
    }
    return inner.stream().map(s -> new FixedFieldIntervalsSource(field, s)).collect(Collectors.toSet());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FixedFieldIntervalsSource that = (FixedFieldIntervalsSource) o;
    return Objects.equals(field, that.field) &&
        Objects.equals(source, that.source);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, source);
  }

  @Override
  public String toString() {
    return "FIELD(" + field + "," + source + ")";
  }
}
