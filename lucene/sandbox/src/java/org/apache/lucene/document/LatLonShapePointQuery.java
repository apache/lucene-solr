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

import java.util.Arrays;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Point2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapePointQuery extends ShapeQuery {
  final Component2D point2D;
  final double[][] points;

  public LatLonShapePointQuery(String field, ShapeField.QueryRelation queryRelation, double[][] points) {
    super(field, queryRelation);
    this.points = points;
    this.point2D = Point2D.create(points);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return point2D.relate(minLon, maxLon, minLat, maxLat);
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, ShapeField.QueryRelation queryRelation) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double alat = GeoEncodingUtils.decodeLatitude(scratchTriangle.aY);
    double alon = GeoEncodingUtils.decodeLongitude(scratchTriangle.aX);
    double blat = GeoEncodingUtils.decodeLatitude(scratchTriangle.bY);
    double blon = GeoEncodingUtils.decodeLongitude(scratchTriangle.bX);
    double clat = GeoEncodingUtils.decodeLatitude(scratchTriangle.cY);
    double clon = GeoEncodingUtils.decodeLongitude(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS:
        return point2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN:
        return point2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT:
        return point2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default:
        throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double alat = GeoEncodingUtils.decodeLatitude(scratchTriangle.aY);
    double alon = GeoEncodingUtils.decodeLongitude(scratchTriangle.aX);
    double blat = GeoEncodingUtils.decodeLatitude(scratchTriangle.bY);
    double blon = GeoEncodingUtils.decodeLongitude(scratchTriangle.bX);
    double clat = GeoEncodingUtils.decodeLatitude(scratchTriangle.cY);
    double clon = GeoEncodingUtils.decodeLongitude(scratchTriangle.cX);

    return point2D.withinTriangle(alon, alat, scratchTriangle.ab, blon, blat, scratchTriangle.bc, clon, clat, scratchTriangle.ca);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(points, ((LatLonShapePointQuery)o).points);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(points);
    return hash;
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
    sb.append("lat = " + points[0][0] + " , lon = " + points[0][1]);
    return sb.toString();
  }
}
