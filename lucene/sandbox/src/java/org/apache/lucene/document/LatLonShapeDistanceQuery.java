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

import org.apache.lucene.geo.Circle;
import org.apache.lucene.geo.Circle2D;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed shapes that intersect the specified distance query.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeDistanceQuery extends ShapeQuery {
  final Circle circle;
  final Component2D circle2D;

  public LatLonShapeDistanceQuery(String field, ShapeField.QueryRelation queryRelation, Circle circle) {
    super(field, queryRelation);
    this.circle = circle;
    this.circle2D = Circle2D.create(circle);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return circle2D.relate(minLon, maxLon, minLat, maxLat);
  }

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
      case INTERSECTS: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return circle2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
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

    return circle2D.withinTriangle(alon, alat, scratchTriangle.ab, blon, blat, scratchTriangle.bc, clon, clat, scratchTriangle.ca);
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
    sb.append(circle.toString());
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && circle.equals(((LatLonShapeDistanceQuery)o).circle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + circle.hashCode();
    return hash;
  }
}
