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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * The distance from a provided Point to a Point retrieved from a ValueSource via
 * {@link org.apache.lucene.queries.function.FunctionValues#objectVal(int)}. The distance
 * is calculated via a {@link org.locationtech.spatial4j.distance.DistanceCalculator}.
 *
 * @lucene.experimental
 */
public class DistanceToShapeValueSource extends ValueSource {
  private final ValueSource shapeValueSource;
  private final Point queryPoint;
  private final double multiplier;
  private final DistanceCalculator distCalc;

  //TODO if FunctionValues returns NaN; will things be ok?
  private final double nullValue;//computed

  public DistanceToShapeValueSource(ValueSource shapeValueSource, Point queryPoint,
                                    double multiplier, SpatialContext ctx) {
    this.shapeValueSource = shapeValueSource;
    this.queryPoint = queryPoint;
    this.multiplier = multiplier;
    this.distCalc = ctx.getDistCalc();
    this.nullValue =
        (ctx.isGeo() ? 180 * multiplier : Double.MAX_VALUE);
  }

  @Override
  public String description() {
    return "distance(" + queryPoint + " to " + shapeValueSource.description() + ")*" + multiplier + ")";
  }

  @Override
  public void createWeight(Map context, IndexSearcher searcher) throws IOException {
    shapeValueSource.createWeight(context, searcher);
  }

  @Override
  public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
    final FunctionValues shapeValues = shapeValueSource.getValues(context, readerContext);

    return new DoubleDocValues(this) {
      @Override
      public double doubleVal(int doc) {
        Shape shape = (Shape) shapeValues.objectVal(doc);
        if (shape == null || shape.isEmpty())
          return nullValue;
        Point pt = shape.getCenter();
        return distCalc.distance(queryPoint, pt) * multiplier;
      }

      @Override
      public Explanation explain(int doc) {
        Explanation exp = super.explain(doc);
        List<Explanation> details = new ArrayList<>(Arrays.asList(exp.getDetails()));
        details.add(shapeValues.explain(doc));
        return Explanation.match(exp.getValue(), exp.getDescription(), details);
      }
    };
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
