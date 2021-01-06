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
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.shape.Point;

/**
 * A DoubleValuesSource that returns the spatial distance between an input point and a document's
 * points in {@link ShapeFieldCacheProvider}. The shortest distance is returned if a document has
 * more than one point.
 *
 * @lucene.internal
 */
public class ShapeFieldCacheDistanceValueSource extends DoubleValuesSource {

  private final SpatialContext ctx;
  private final Point from;
  private final ShapeFieldCacheProvider<Point> provider;
  private final double multiplier;

  public ShapeFieldCacheDistanceValueSource(
      SpatialContext ctx, ShapeFieldCacheProvider<Point> provider, Point from, double multiplier) {
    this.ctx = ctx;
    this.from = from;
    this.provider = provider;
    this.multiplier = multiplier;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + provider + ", " + from + ")";
  }

  @Override
  public DoubleValues getValues(LeafReaderContext readerContext, DoubleValues scores)
      throws IOException {

    final double nullValue = (ctx.isGeo() ? 180 * multiplier : Double.MAX_VALUE);

    return DoubleValues.withDefault(
        new DoubleValues() {
          private final ShapeFieldCache<Point> cache = provider.getCache(readerContext.reader());
          private final Point from = ShapeFieldCacheDistanceValueSource.this.from;
          private final DistanceCalculator calculator = ctx.getDistCalc();

          private List<Point> currentVals;

          @Override
          public double doubleValue() throws IOException {
            double v = calculator.distance(from, currentVals.get(0));
            for (int i = 1; i < currentVals.size(); i++) {
              v = Math.min(v, calculator.distance(from, currentVals.get(i)));
            }
            return v * multiplier;
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            currentVals = cache.getShapes(doc);
            return currentVals != null;
          }
        },
        nullValue);
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher searcher) throws IOException {
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ShapeFieldCacheDistanceValueSource that = (ShapeFieldCacheDistanceValueSource) o;

    if (!ctx.equals(that.ctx)) return false;
    if (!from.equals(that.from)) return false;
    if (!provider.equals(that.provider)) return false;
    if (multiplier != that.multiplier) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return from.hashCode();
  }
}
