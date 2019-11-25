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
package org.apache.lucene.spatial;

import org.apache.lucene.document.Field;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.util.ReciprocalDoubleValuesSource;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

/**
 * The SpatialStrategy encapsulates an approach to indexing and searching based
 * on shapes.
 * <p>
 * Different implementations will support different features. A strategy should
 * document these common elements:
 * <ul>
 *   <li>Can it index more than one shape per field?</li>
 *   <li>What types of shapes can be indexed?</li>
 *   <li>What types of query shapes can be used?</li>
 *   <li>What types of query operations are supported?
 *   This might vary per shape.</li>
 *   <li>Does it use some type of cache?  When?
 * </ul>
 * If a strategy only supports certain shapes at index or query time, then in
 * general it will throw an exception if given an incompatible one.  It will not
 * be coerced into compatibility.
 * <p>
 * Note that a SpatialStrategy is not involved with the Lucene stored field
 * values of shapes, which is immaterial to indexing and search.
 * <p>
 * Thread-safe.
 * <p>
 * This API is marked as experimental, however it is quite stable.
 *
 * @lucene.experimental
 */
public abstract class SpatialStrategy {

  protected final SpatialContext ctx;
  private final String fieldName;

  /**
   * Constructs the spatial strategy with its mandatory arguments.
   */
  public SpatialStrategy(SpatialContext ctx, String fieldName) {
    if (ctx == null)
      throw new IllegalArgumentException("ctx is required");
    this.ctx = ctx;
    if (fieldName == null || fieldName.length() == 0)
      throw new IllegalArgumentException("fieldName is required");
    this.fieldName = fieldName;
  }

  public SpatialContext getSpatialContext() {
    return ctx;
  }

  /**
   * The name of the field or the prefix of them if there are multiple
   * fields needed internally.
   * @return Not null.
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Returns the IndexableField(s) from the {@code shape} that are to be
   * added to the {@link org.apache.lucene.document.Document}.  These fields
   * are expected to be marked as indexed and not stored.
   * <p>
   * Note: If you want to <i>store</i> the shape as a string for retrieval in
   * search results, you could add it like this:
   * <pre>document.add(new StoredField(fieldName,ctx.toString(shape)));</pre>
   * The particular string representation used doesn't matter to the Strategy
   * since it doesn't use it.
   *
   * @return Not null nor will it have null elements.
   * @throws UnsupportedOperationException if given a shape incompatible with the strategy
   */
  public abstract Field[] createIndexableFields(Shape shape);

  /**
   * See {@link #makeDistanceValueSource(org.locationtech.spatial4j.shape.Point, double)} called with
   * a multiplier of 1.0 (i.e. units of degrees).
   */
  public DoubleValuesSource makeDistanceValueSource(Point queryPoint) {
    return makeDistanceValueSource(queryPoint, 1.0);
  }

  /**
   * Make a ValueSource returning the distance between the center of the
   * indexed shape and {@code queryPoint}.  If there are multiple indexed shapes
   * then the closest one is chosen. The result is multiplied by {@code multiplier}, which
   * conveniently is used to get the desired units.
   */
  public abstract DoubleValuesSource makeDistanceValueSource(Point queryPoint, double multiplier);

  /**
   * Make a Query based principally on {@link org.apache.lucene.spatial.query.SpatialOperation}
   * and {@link Shape} from the supplied {@code args}.  It should be constant scoring of 1.
   *
   * @throws UnsupportedOperationException If the strategy does not support the shape in {@code args}
   * @throws org.apache.lucene.spatial.query.UnsupportedSpatialOperation If the strategy does not support the {@link
   * org.apache.lucene.spatial.query.SpatialOperation} in {@code args}.
   */
  public abstract Query makeQuery(SpatialArgs args);

  /**
   * Returns a ValueSource with values ranging from 1 to 0, depending inversely
   * on the distance from {@link #makeDistanceValueSource(org.locationtech.spatial4j.shape.Point,double)}.
   * The formula is {@code c/(d + c)} where 'd' is the distance and 'c' is
   * one tenth the distance to the farthest edge from the center. Thus the
   * scores will be 1 for indexed points at the center of the query shape and as
   * low as ~0.1 at its furthest edges.
   */
  @SuppressWarnings("deprecation")
  public final DoubleValuesSource makeRecipDistanceValueSource(Shape queryShape) {
    Rectangle bbox = queryShape.getBoundingBox();
    double diagonalDist = ctx.getDistCalc().distance(
        ctx.makePoint(bbox.getMinX(), bbox.getMinY()), bbox.getMaxX(), bbox.getMaxY());
    double distToEdge = diagonalDist * 0.5;
    float c = (float)distToEdge * 0.1f;//one tenth
    DoubleValuesSource distance = makeDistanceValueSource(queryShape.getCenter(), 1.0);
    return new ReciprocalDoubleValuesSource(c, distance);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+" field:"+fieldName+" ctx="+ctx;
  }
}
