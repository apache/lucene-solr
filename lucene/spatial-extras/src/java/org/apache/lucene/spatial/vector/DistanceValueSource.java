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
package org.apache.lucene.spatial.vector;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;

/**
 * A DoubleValuesSource that returns the distance for a {@link PointVectorStrategy}.
 *
 * @lucene.internal
 */
public class DistanceValueSource extends DoubleValuesSource {

  private PointVectorStrategy strategy;
  private final Point from;
  private final double multiplier;
  private final double nullValue;

  /**
   * Constructor.
   */
  public DistanceValueSource(PointVectorStrategy strategy, Point from, double multiplier) {
    this.strategy = strategy;
    this.from = from;
    this.multiplier = multiplier;
    this.nullValue = 180 * multiplier;
  }

  /**
   * Returns the ValueSource description.
   */
  @Override
  public String toString() {
    return "DistanceValueSource("+strategy+", "+from+")";
  }

  /**
   * Returns the FunctionValues used by the function query.
   */
  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores) throws IOException {
    LeafReader reader = readerContext.reader();

    final NumericDocValues ptX = DocValues.getNumeric(reader, strategy.getFieldNameX());
    final NumericDocValues ptY = DocValues.getNumeric(reader, strategy.getFieldNameY());

    return DoubleValues.withDefault(new DoubleValues() {

      private final Point from = DistanceValueSource.this.from;
      private final DistanceCalculator calculator = strategy.getSpatialContext().getDistCalc();

      @Override
      public double doubleValue() throws IOException {
        double x = Double.longBitsToDouble(ptX.longValue());
        double y = Double.longBitsToDouble(ptY.longValue());
        return calculator.distance(from, x, y) * multiplier;
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return ptX.advanceExact(doc) && ptY.advanceExact(doc);
      }

    }, nullValue);
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return DocValues.isCacheable(ctx, strategy.getFieldNameX(), strategy.getFieldNameY());
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DistanceValueSource that = (DistanceValueSource) o;

    if (!from.equals(that.from)) return false;
    if (!strategy.equals(that.strategy)) return false;
    if (multiplier != that.multiplier) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return from.hashCode();
  }
}
