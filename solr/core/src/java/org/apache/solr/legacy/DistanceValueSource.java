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
package org.apache.solr.legacy;

import org.apache.lucene.index.NumericDocValues;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;

import java.io.IOException;
import java.util.Map;

/**
 * An implementation of the Lucene ValueSource model that returns the distance
 * for a {@link PointVectorStrategy}.
 *
 * @lucene.internal
 */
public class DistanceValueSource extends ValueSource {

  private PointVectorStrategy strategy;
  private final Point from;
  private final double multiplier;

  /**
   * Constructor.
   */
  public DistanceValueSource(PointVectorStrategy strategy, Point from, double multiplier) {
    this.strategy = strategy;
    this.from = from;
    this.multiplier = multiplier;
  }

  /**
   * Returns the ValueSource description.
   */
  @Override
  public String description() {
    return "DistanceValueSource("+strategy+", "+from+")";
  }

  /**
   * Returns the FunctionValues used by the function query.
   */
  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    LeafReader reader = readerContext.reader();

    final NumericDocValues ptX = DocValues.getNumeric(reader, strategy.getFieldNameX());
    final NumericDocValues ptY = DocValues.getNumeric(reader, strategy.getFieldNameY());

    return new FunctionValues() {

      private int lastDocID = -1;

      private final Point from = DistanceValueSource.this.from;
      private final DistanceCalculator calculator = strategy.getSpatialContext().getDistCalc();
      private final double nullValue =
          (strategy.getSpatialContext().isGeo() ? 180 * multiplier : Double.MAX_VALUE);

      private double getDocValue(NumericDocValues values, int doc) throws IOException {
        int curDocID = values.docID();
        if (doc > curDocID) {
          curDocID = values.advance(doc);
        }
        if (doc == curDocID) {
          return Double.longBitsToDouble(values.longValue());
        } else {
          return 0.0;
        }
      }

      @Override
      public float floatVal(int doc) throws IOException {
        return (float) doubleVal(doc);
      }

      @Override
      public double doubleVal(int doc) throws IOException {
        // make sure it has minX and area
        double x = getDocValue(ptX, doc);
        if (ptX.docID() == doc) {
          double y = getDocValue(ptY, doc);
          assert ptY.docID() == doc;
          return calculator.distance(from, x, y) * multiplier;
        }
        return nullValue;
      }

      @Override
      public String toString(int doc) throws IOException {
        return description() + "=" + floatVal(doc);
      }
    };
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
