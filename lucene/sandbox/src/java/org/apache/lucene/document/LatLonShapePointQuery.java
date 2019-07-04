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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static java.lang.Integer.BYTES;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Finds all previously indexed shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class LatLonShapePointQuery extends LatLonShapeQuery {
  final double lat;
  final double lon;
  final int latEnc;
  final int lonEnc;
  final byte[] point;

  public LatLonShapePointQuery(String field, LatLonShape.QueryRelation queryRelation, double lat, double lon) {
    super(field, queryRelation);
    this.lat = lat;
    this.lon = lon;
    this.point = new byte[2 * LatLonShape.BYTES];
    this.lonEnc = GeoEncodingUtils.encodeLongitude(lon);
    this.latEnc = GeoEncodingUtils.encodeLatitude(lat);
    NumericUtils.intToSortableBytes(latEnc, this.point, 0);
    NumericUtils.intToSortableBytes(lonEnc, this.point, LatLonShape.BYTES);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, point,  BYTES, 2 * BYTES) > 0 ||
        Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, point, BYTES, 2 * BYTES) < 0 ||
        Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, point, 0, BYTES) > 0 ||
        Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, point, 0, BYTES) < 0) {
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
    return PointValues.Relation.CELL_CROSSES_QUERY;
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle, LatLonShape.QueryRelation queryRelation) {

    // decode indexed triangle
    LatLonShape.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle[0];
    int aX = scratchTriangle[1];
    int bY = scratchTriangle[2];
    int bX = scratchTriangle[3];
    int cY = scratchTriangle[4];
    int cX = scratchTriangle[5];

    if (queryRelation == LatLonShape.QueryRelation.WITHIN) {
       if (aY == bY && cY == aY && aX == bX && cX == aX) {
         return lonEnc == aX && latEnc == aY;
       }
      return false;
    }
    return pointInTriangle(lonEnc, latEnc, aX, aY, bX, bY, cX, cY);
  }

  //This should be moved when LatLonShape is moved from sandbox!
  /**
   * Compute whether the given x, y point is in a triangle; uses the winding order method */
  private static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
    double minX = StrictMath.min(ax, StrictMath.min(bx, cx));
    double minY = StrictMath.min(ay, StrictMath.min(by, cy));
    double maxX = StrictMath.max(ax, StrictMath.max(bx, cx));
    double maxY = StrictMath.max(ay, StrictMath.max(by, cy));
    //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
    //coplanar points that are not part of the triangle.
    if (x >= minX && x <= maxX && y >= minY && y <= maxY ) {
      int a = orient(x, y, ax, ay, bx, by);
      int b = orient(x, y, bx, by, cx, cy);
      if (a == 0 || b == 0 || a < 0 == b < 0) {
        int c = orient(x, y, cx, cy, ax, ay);
        return c == 0 || (c < 0 == (b < 0 || a < 0));
      }
      return false;
    } else {
      return false;
    }
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && lon == ((LatLonShapePointQuery)o).lon && lat == ((LatLonShapePointQuery)o).lat;
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + Double.hashCode(lat);
    hash = 31 * hash + Double.hashCode(lon);
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
    sb.append("lat = " + lat + " , lon = " + lon);
    return sb.toString();
  }
}
