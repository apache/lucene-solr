package org.apache.lucene.spatial;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.document.Field;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.ReciprocalFloatFunction;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.query.SpatialArgs;

/**
 * The SpatialStrategy encapsulates an approach to indexing and searching based
 * on shapes.
 * <p/>
 * Different implementations will support different features. A strategy should
 * document these common elements:
 * <ul>
 *   <li>Can it index more than one shape per field?</li>
 *   <li>What types of shapes can be indexed?</li>
 *   <li>What types of query shapes can be used?</li>
 *   <li>What types of query operations are supported?
 *   This might vary per shape.</li>
 *   <li>Does it use the {@link org.apache.lucene.search.FieldCache},
 *   or some other type of cache?  When?
 * </ul>
 * If a strategy only supports certain shapes at index or query time, then in
 * general it will throw an exception if given an incompatible one.  It will not
 * be coerced into compatibility.
 * <p/>
 * Note that a SpatialStrategy is not involved with the Lucene stored field
 * values of shapes, which is immaterial to indexing & search.
 * <p/>
 * Thread-safe.
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
   * <p/>
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
   * Make a ValueSource returning the distance between the center of the
   * indexed shape and {@code queryPoint}.  If there are multiple indexed shapes
   * then the closest one is chosen.
   */
  public abstract ValueSource makeDistanceValueSource(Point queryPoint);

  /**
   * Make a Query based principally on {@link org.apache.lucene.spatial.query.SpatialOperation}
   * and {@link Shape} from the supplied {@code args}.
   * The default implementation is
   * <pre>return new ConstantScoreQuery(makeFilter(args));</pre>
   *
   * @throws UnsupportedOperationException If the strategy does not support the shape in {@code args}
   * @throws org.apache.lucene.spatial.query.UnsupportedSpatialOperation If the strategy does not support the {@link
   * org.apache.lucene.spatial.query.SpatialOperation} in {@code args}.
   */
  public Query makeQuery(SpatialArgs args) {
    return new ConstantScoreQuery(makeFilter(args));
  }

  /**
   * Make a Filter based principally on {@link org.apache.lucene.spatial.query.SpatialOperation}
   * and {@link Shape} from the supplied {@code args}.
   * <p />
   * If a subclasses implements
   * {@link #makeQuery(org.apache.lucene.spatial.query.SpatialArgs)}
   * then this method could be simply:
   * <pre>return new QueryWrapperFilter(makeQuery(args).getQuery());</pre>
   *
   * @throws UnsupportedOperationException If the strategy does not support the shape in {@code args}
   * @throws org.apache.lucene.spatial.query.UnsupportedSpatialOperation If the strategy does not support the {@link
   * org.apache.lucene.spatial.query.SpatialOperation} in {@code args}.
   */
  public abstract Filter makeFilter(SpatialArgs args);

  /**
   * Returns a ValueSource with values ranging from 1 to 0, depending inversely
   * on the distance from {@link #makeDistanceValueSource(com.spatial4j.core.shape.Point)}.
   * The formula is {@code c/(d + c)} where 'd' is the distance and 'c' is
   * one tenth the distance to the farthest edge from the center. Thus the
   * scores will be 1 for indexed points at the center of the query shape and as
   * low as ~0.1 at its furthest edges.
   */
  public final ValueSource makeRecipDistanceValueSource(Shape queryShape) {
    Rectangle bbox = queryShape.getBoundingBox();
    double diagonalDist = ctx.getDistCalc().distance(
        ctx.makePoint(bbox.getMinX(), bbox.getMinY()), bbox.getMaxX(), bbox.getMaxY());
    double distToEdge = diagonalDist * 0.5;
    float c = (float)distToEdge * 0.1f;//one tenth
    return new ReciprocalFloatFunction(makeDistanceValueSource(queryShape.getCenter()), 1f, c, c);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+" field:"+fieldName+" ctx="+ctx;
  }
}
