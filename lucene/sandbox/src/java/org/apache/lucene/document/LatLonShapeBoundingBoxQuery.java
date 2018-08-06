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

import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.geo.GeoEncodingUtils.decodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.decodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields(String, Polygon)} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapeBoundingBoxQuery extends LatLonShapeQuery {
  final byte[] bbox;
  final int minX;
  final int maxX;
  final int minY;
  final int maxY;

  public LatLonShapeBoundingBoxQuery(String field, LatLonShape.QueryRelation queryRelation, double minLat, double maxLat, double minLon, double maxLon) {
    super(field, queryRelation);
    if (minLon > maxLon) {
      throw new IllegalArgumentException("dateline crossing bounding box queries are not supported for [" + field + "]");
    }
    this.bbox = new byte[4 * LatLonPoint.BYTES];
    this.minX = encodeLongitudeCeil(minLon);
    this.maxX = encodeLongitude(maxLon);
    this.minY = encodeLatitudeCeil(minLat);
    this.maxY = encodeLatitude(maxLat);
    NumericUtils.intToSortableBytes(this.minY, this.bbox, 0);
    NumericUtils.intToSortableBytes(this.minX, this.bbox, LatLonPoint.BYTES);
    NumericUtils.intToSortableBytes(this.maxY, this.bbox, 2 * LatLonPoint.BYTES);
    NumericUtils.intToSortableBytes(this.maxX, this.bbox, 3 * LatLonPoint.BYTES);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    // check bounding box (DISJOINT)
    if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, 4 * LatLonPoint.BYTES) > 0 ||
        FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0 ||
        FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + LatLonPoint.BYTES, bbox, 2 * LatLonPoint.BYTES, 3 * LatLonPoint.BYTES) > 0 ||
        FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + LatLonPoint.BYTES, bbox, 0, LatLonPoint.BYTES) < 0) {
      return Relation.CELL_OUTSIDE_QUERY;
    }

    if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) > 0 &&
        FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, 4 * LatLonPoint.BYTES) < 0 &&
        FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + LatLonPoint.BYTES, bbox, 0, LatLonPoint.BYTES) > 0 &&
        FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + LatLonPoint.BYTES, bbox, 2 * LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0) {
      return Relation.CELL_INSIDE_QUERY;
    }
    return Relation.CELL_CROSSES_QUERY;
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t) {
    if (queryRelation == LatLonShape.QueryRelation.WITHIN) {
      return queryContains(t, 0) && queryContains(t, 1) && queryContains(t, 2);
    }
    return queryIntersects(t);
  }

  /** returns true if the query intersects the encoded triangle */
  protected boolean queryIntersects(byte[] t) {

    // 1. query contains any triangle points
    if (queryContains(t, 0) || queryContains(t, 1) || queryContains(t, 2)) {
      return true;
    }

    int aY = NumericUtils.sortableBytesToInt(t, 0);
    int aX = NumericUtils.sortableBytesToInt(t, LatLonPoint.BYTES);
    int bY = NumericUtils.sortableBytesToInt(t, 2 * LatLonPoint.BYTES);
    int bX = NumericUtils.sortableBytesToInt(t, 3 * LatLonPoint.BYTES);
    int cY = NumericUtils.sortableBytesToInt(t, 4 * LatLonPoint.BYTES);
    int cX = NumericUtils.sortableBytesToInt(t, 5 * LatLonPoint.BYTES);

    int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
    int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
    int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

    // 2. check bounding boxes are disjoint
    if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
      return false;
    }

    // 3. check triangle contains any query points
    if (Tessellator.pointInTriangle(minX, minY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(maxX, minY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(maxX, maxY, aX, aY, bX, bY, cX, cY)) {
      return true;
    } else if (Tessellator.pointInTriangle(minX, maxY, aX, aY, bX, bY, cX, cY)) {
      return true;
    }


    // 4. last ditch effort: check crossings
    if (queryIntersects(aX, aY, bX, bY, cX, cY)) {
      return true;
    }
    return false;
  }

  /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
  private boolean edgeIntersectsQuery(double ax, double ay, double bx, double by) {
    // top
    if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
        orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
      return true;
    }

    // right
    if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
        orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
      return true;
    }

    // bottom
    if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
        orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
      return true;
    }

    // left
    if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
        orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
      return true;
    }
    return false;
  }

  /** returns true if the query contains the triangle vertex */
  private boolean queryContains(byte[] t, int point) {
    final int yIdx = 2 * LatLonPoint.BYTES * point;
    final int xIdx = yIdx + LatLonPoint.BYTES;

    if (FutureArrays.compareUnsigned(t, yIdx, xIdx, bbox, 0, LatLonPoint.BYTES) < 0 ||                     //minY
        FutureArrays.compareUnsigned(t, yIdx, xIdx, bbox, 2 * LatLonPoint.BYTES, 3 * LatLonPoint.BYTES) > 0 ||  //maxY
        FutureArrays.compareUnsigned(t, xIdx, xIdx + LatLonPoint.BYTES, bbox, LatLonPoint.BYTES, 2 * LatLonPoint.BYTES) < 0 || // minX
        FutureArrays.compareUnsigned(t, xIdx, xIdx + LatLonPoint.BYTES, bbox, 3 * LatLonPoint.BYTES, bbox.length) > 0) {
      return false;
    }
    return true;
  }

  /** returns true if the query intersects the provided triangle (in encoded space) */
  private boolean queryIntersects(int ax, int ay, int bx, int by, int cx, int cy) {
    // check each edge of the triangle against the query
    if (edgeIntersectsQuery(ax, ay, bx, by) ||
        edgeIntersectsQuery(bx, by, cx, cy) ||
        edgeIntersectsQuery(cx, cy, ax, ay)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && Arrays.equals(bbox, ((LatLonShapeBoundingBoxQuery)o).bbox);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Arrays.hashCode(bbox);
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
    sb.append("Rectangle(lat=");
    sb.append(decodeLatitude(minY));
    sb.append(" TO ");
    sb.append(decodeLatitude(maxY));
    sb.append(" lon=");
    sb.append(decodeLongitude(minX));
    sb.append(" TO ");
    sb.append(decodeLongitude(maxX));
    sb.append(")");
    return sb.toString();
  }
}
