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
package org.apache.lucene.document;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;

import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed geo points that comply the given {@link QueryRelation} with the
 * specified array of {@link LatLonGeometry}.
 *
 * <p>The field must be indexed using one or more {@link LatLonPoint} added per document.
 */
final class LatLonPointQuery extends SpatialQuery {
  private final LatLonGeometry[] geometries;
  private final Component2D component2D;

  /**
   * Creates a query that matches all indexed shapes to the provided array of {@link LatLonGeometry}
   */
  LatLonPointQuery(String field, QueryRelation queryRelation, LatLonGeometry... geometries) {
    super(field, queryRelation);
    if (queryRelation == QueryRelation.WITHIN) {
      for (LatLonGeometry geometry : geometries) {
        if (geometry instanceof Line) {
          // TODO: line queries do not support within relations
          throw new IllegalArgumentException(
              "LatLonPointQuery does not support "
                  + QueryRelation.WITHIN
                  + " queries with line geometries");
        }
      }
    }
    if (queryRelation == ShapeField.QueryRelation.CONTAINS) {
      for (LatLonGeometry geometry : geometries) {
        if ((geometry instanceof Point) == false) {
          throw new IllegalArgumentException(
              "LatLonPointQuery does not support "
                  + ShapeField.QueryRelation.CONTAINS
                  + " queries with non-points geometries");
        }
      }
    }
    this.component2D = LatLonGeometry.create(geometries);
    this.geometries = geometries.clone();
  }

  @Override
  protected SpatialVisitor getSpatialVisitor() {
    final GeoEncodingUtils.Component2DPredicate component2DPredicate =
        GeoEncodingUtils.createComponentPredicate(component2D);
    // bounding box over all geometries, this can speed up tree intersection/cheaply improve
    // approximation for complex multi-geometries
    final byte[] minLat = new byte[Integer.BYTES];
    final byte[] maxLat = new byte[Integer.BYTES];
    final byte[] minLon = new byte[Integer.BYTES];
    final byte[] maxLon = new byte[Integer.BYTES];
    NumericUtils.intToSortableBytes(encodeLatitude(component2D.getMinY()), minLat, 0);
    NumericUtils.intToSortableBytes(encodeLatitude(component2D.getMaxY()), maxLat, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(component2D.getMinX()), minLon, 0);
    NumericUtils.intToSortableBytes(encodeLongitude(component2D.getMaxX()), maxLon, 0);

    return new SpatialVisitor() {
      @Override
      protected Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        if (FutureArrays.compareUnsigned(minPackedValue, 0, Integer.BYTES, maxLat, 0, Integer.BYTES) > 0
            || FutureArrays.compareUnsigned(maxPackedValue, 0, Integer.BYTES, minLat, 0, Integer.BYTES)
                < 0
            || FutureArrays.compareUnsigned(
                    minPackedValue,
                    Integer.BYTES,
                    Integer.BYTES + Integer.BYTES,
                    maxLon,
                    0,
                    Integer.BYTES)
                > 0
            || FutureArrays.compareUnsigned(
                    maxPackedValue,
                    Integer.BYTES,
                    Integer.BYTES + Integer.BYTES,
                    minLon,
                    0,
                    Integer.BYTES)
                < 0) {
          // outside of global bounding box range
          return Relation.CELL_OUTSIDE_QUERY;
        }

        double cellMinLat = decodeLatitude(minPackedValue, 0);
        double cellMinLon = decodeLongitude(minPackedValue, Integer.BYTES);
        double cellMaxLat = decodeLatitude(maxPackedValue, 0);
        double cellMaxLon = decodeLongitude(maxPackedValue, Integer.BYTES);

        return component2D.relate(cellMinLon, cellMaxLon, cellMinLat, cellMaxLat);
      }

      @Override
      protected Predicate<byte[]> intersects() {
        return packedValue ->
            component2DPredicate.test(
                NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES));
      }

      @Override
      protected Predicate<byte[]> within() {
        return packedValue ->
            component2DPredicate.test(
                NumericUtils.sortableBytesToInt(packedValue, 0),
                NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES));
      }

      @Override
      protected Function<byte[], Component2D.WithinRelation> contains() {
        return packedValue ->
            component2D.withinPoint(
                GeoEncodingUtils.decodeLongitude(
                    NumericUtils.sortableBytesToInt(packedValue, Integer.BYTES)),
                GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(packedValue, 0)));
      }
    };
  }

  @Override
  public String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName());
    sb.append(':');
    if (this.field.equals(field) == false) {
      sb.append(" field=");
      sb.append(this.field);
      sb.append(':');
    }
    sb.append("[");
    for (int i = 0; i < geometries.length; i++) {
      sb.append(geometries[i].toString());
      sb.append(',');
    }
    sb.append(']');
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(geometries, ((LatLonPointQuery) o).geometries);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(geometries);
    return hash;
  }
}
