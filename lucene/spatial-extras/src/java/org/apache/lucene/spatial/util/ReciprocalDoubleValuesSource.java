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

package org.apache.lucene.spatial.util;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

/**
 * Transforms a DoubleValuesSource using the formula v = k / (v + k)
 */
public class ReciprocalDoubleValuesSource extends DoubleValuesSource {

  private final double distToEdge;
  private final DoubleValuesSource input;

  /**
   * Creates a ReciprocalDoubleValuesSource
   * @param distToEdge  the value k in v = k / (v + k)
   * @param input       the input DoubleValuesSource to transform
   */
  public ReciprocalDoubleValuesSource(double distToEdge, DoubleValuesSource input) {
    this.distToEdge = distToEdge;
    this.input = input;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    DoubleValues in = input.getValues(ctx, scores);
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return recip(in.doubleValue());
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return in.advanceExact(doc);
      }
    };
  }

  private double recip(double in) {
    return distToEdge / (in + distToEdge);
  }

  @Override
  public boolean needsScores() {
    return input.needsScores();
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return input.isCacheable(ctx);
  }

  @Override
  public Explanation explain(LeafReaderContext ctx, int docId, Explanation scoreExplanation) throws IOException {
    Explanation expl = input.explain(ctx, docId, scoreExplanation);
    return Explanation.match((float)recip(expl.getValue()),
        distToEdge + " / (v + " + distToEdge + "), computed from:", expl);
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return new ReciprocalDoubleValuesSource(distToEdge, input.rewrite(searcher));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ReciprocalDoubleValuesSource that = (ReciprocalDoubleValuesSource) o;
    return Double.compare(that.distToEdge, distToEdge) == 0 &&
        Objects.equals(input, that.input);
  }

  @Override
  public int hashCode() {
    return Objects.hash(distToEdge, input);
  }

  @Override
  public String toString() {
    return "recip(" + distToEdge + ", " + input.toString() + ")";
  }
}
