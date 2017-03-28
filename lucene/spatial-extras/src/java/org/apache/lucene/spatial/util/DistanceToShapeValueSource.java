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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.spatial.ShapeValues;
import org.apache.lucene.spatial.ShapeValuesSource;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;

/**
 * The distance from a provided Point to a Point retrieved from an ShapeValuesSource. The distance
 * is calculated via a {@link org.locationtech.spatial4j.distance.DistanceCalculator}.
 *
 * @lucene.experimental
 */
public class DistanceToShapeValueSource extends DoubleValuesSource {

  private final ShapeValuesSource shapeValueSource;
  private final Point queryPoint;
  private final double multiplier;
  private final DistanceCalculator distCalc;

  //TODO if DoubleValues returns NaN; will things be ok?
  private final double nullValue;

  public DistanceToShapeValueSource(ShapeValuesSource shapeValueSource, Point queryPoint,
                                    double multiplier, SpatialContext ctx) {
    this.shapeValueSource = shapeValueSource;
    this.queryPoint = queryPoint;
    this.multiplier = multiplier;
    this.distCalc = ctx.getDistCalc();
    this.nullValue = (ctx.isGeo() ? 180 * multiplier : Double.MAX_VALUE);
  }

  @Override
  public String toString() {
    return "distance(" + queryPoint + " to " + shapeValueSource.toString() + ")*" + multiplier + ")";
  }

  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores) throws IOException {

    final ShapeValues shapeValues = shapeValueSource.getValues(readerContext);

    return DoubleValues.withDefault(new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return distCalc.distance(queryPoint, shapeValues.value().getCenter()) * multiplier;
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return shapeValues.advanceExact(doc);
      }
    }, nullValue);
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return shapeValueSource.isCacheable(ctx);
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DistanceToShapeValueSource that = (DistanceToShapeValueSource) o;

    if (!queryPoint.equals(that.queryPoint)) return false;
    if (Double.compare(that.multiplier, multiplier) != 0) return false;
    if (!shapeValueSource.equals(that.shapeValueSource)) return false;
    if (!distCalc.equals(that.distCalc)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result;
    long temp;
    result = shapeValueSource.hashCode();
    result = 31 * result + queryPoint.hashCode();
    temp = Double.doubleToLongBits(multiplier);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }
}
