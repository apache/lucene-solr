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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed geo shapes that intersect the specified arbitrary.
 * <p>
 * Note:
 * <ul>
 *    <li>Dateline crossing is not yet supported. Polygons should be cut at the dateline and provided as a multipolygon query</li>
 * </ul>
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapePolygonQuery extends ShapeQuery {
  final Polygon[] polygons;
  final private Component2D poly2D;

  /**
   * Creates a query that matches all indexed shapes to the provided polygons
   */
  public LatLonShapePolygonQuery(String field, QueryRelation queryRelation, Polygon... polygons) {
    super(field, queryRelation);
    if (polygons == null) {
      throw new IllegalArgumentException("polygons must not be null");
    }
    if (polygons.length == 0) {
      throw new IllegalArgumentException("polygons must not be empty");
    }
    for (int i = 0; i < polygons.length; i++) {
      if (polygons[i] == null) {
        throw new IllegalArgumentException("polygon[" + i + "] must not be null");
      } else if (polygons[i].minLon > polygons[i].maxLon) {
        throw new IllegalArgumentException("LatLonShapePolygonQuery does not currently support querying across dateline.");
      }
    }
    this.polygons = polygons.clone();
    this.poly2D = Polygon2D.create(polygons);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {

    double minLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
    double minLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
    double maxLat = GeoEncodingUtils.decodeLatitude(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
    double maxLon = GeoEncodingUtils.decodeLongitude(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

    // check internal node against query
    return poly2D.relate(minLon, maxLon, minLat, maxLat);
  }

  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, QueryRelation queryRelation) {
    ShapeField.decodeTriangle(t, scratchTriangle);

    double alat = GeoEncodingUtils.decodeLatitude(scratchTriangle.aY);
    double alon = GeoEncodingUtils.decodeLongitude(scratchTriangle.aX);
    double blat = GeoEncodingUtils.decodeLatitude(scratchTriangle.bY);
    double blon = GeoEncodingUtils.decodeLongitude(scratchTriangle.bX);
    double clat = GeoEncodingUtils.decodeLatitude(scratchTriangle.cY);
    double clon = GeoEncodingUtils.decodeLongitude(scratchTriangle.cX);

    switch (queryRelation) {
      case INTERSECTS: return poly2D.relateTriangle(alon, alat, blon, blat, clon, clat) != Relation.CELL_OUTSIDE_QUERY;
      case WITHIN: return poly2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_INSIDE_QUERY;
      case DISJOINT: return poly2D.relateTriangle(alon, alat, blon, blat, clon, clat) == Relation.CELL_OUTSIDE_QUERY;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
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
    sb.append("Polygon(").append(polygons[0].toGeoJSON()).append(')');
    return sb.toString();
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(polygons, ((LatLonShapePolygonQuery)o).polygons);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(polygons);
    return hash;
  }
}