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

package org.apache.lucene.geo;

import java.util.Arrays;
import java.util.Objects;

import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;

import static java.lang.Integer.BYTES;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * 2D rectangle implementation containing geo spatial logic.
 *
 * @lucene.internal
 */
public class Rectangle2D {
  protected final byte[] bbox;
  private final byte[] west;
  protected final int minX;
  protected final int maxX;
  protected final int minY;
  protected final int maxY;

  protected Rectangle2D(double minLat, double maxLat, double minLon, double maxLon) {
    this.bbox = new byte[4 * BYTES];
    int minXenc = encodeLongitudeCeil(minLon);
    int maxXenc = encodeLongitude(maxLon);
    int minYenc = encodeLatitudeCeil(minLat);
    int maxYenc = encodeLatitude(maxLat);
    if (minYenc > maxYenc) {
      minYenc = maxYenc;
    }
    this.minY = minYenc;
    this.maxY = maxYenc;

    if (minLon > maxLon == true) {
      // crossing dateline is split into east/west boxes
      this.west = new byte[4 * BYTES];
      this.minX = minXenc;
      this.maxX = maxXenc;
      encode(MIN_LON_ENCODED, this.maxX, this.minY, this.maxY, this.west);
      encode(this.minX, MAX_LON_ENCODED, this.minY, this.maxY, this.bbox);
    } else {
      // encodeLongitudeCeil may cause minX to be > maxX iff
      // the delta between the longitude < the encoding resolution
      if (minXenc > maxXenc) {
        minXenc = maxXenc;
      }
      this.west = null;
      this.minX = minXenc;
      this.maxX = maxXenc;
      encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
    }
  }

  protected Rectangle2D(int minX, int maxX, int minY, int maxY) {
    this.bbox = new byte[4 * BYTES];
    this.west = null;
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
  }

  /** Builds a Rectangle2D from rectangle */
  public static Rectangle2D create(Rectangle rectangle) {
    return new Rectangle2D(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
  }

  public boolean crossesDateline() {
    return minX > maxX;
  }

  /** Checks if the rectangle contains the provided point **/
  public boolean queryContainsPoint(int x, int y) {
    if (this.crossesDateline() == true) {
      return bboxContainsPoint(x, y, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || bboxContainsPoint(x, y, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return bboxContainsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY);
  }

  /** compare this to a provided range bounding box **/
  public Relation relateRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                  int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    Relation eastRelation = compareBBoxToRangeBBox(this.bbox,
        minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
      return compareBBoxToRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }
    return eastRelation;
  }

  /** intersects this to a provided range bounding box **/
  public Relation intersectRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                  int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    Relation eastRelation = intersectBBoxWithRangeBBox(this.bbox,
        minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
      return intersectBBoxWithRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }
    return eastRelation;
  }

  /** Checks if the rectangle intersects the provided triangle **/
  public boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
    // 1. query contains any triangle points
    if (queryContainsPoint(aX, aY) || queryContainsPoint(bX, bY) || queryContainsPoint(cX, cY)) {
      return true;
    }

    // compute bounding box of triangle
    int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
    int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
    int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
    int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

    // 2. check bounding boxes are disjoint
    if (this.crossesDateline() == true) {
      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          && boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, this.minX, MAX_LON_ENCODED, this.minY, this.maxY)) {
        return false;
      }
    } else if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
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

  /** Checks if the rectangle contains the provided triangle **/
  public boolean containsTriangle(int ax, int ay, int bx, int by, int cx, int cy) {
    if (this.crossesDateline() == true) {
      return bboxContainsTriangle(ax, ay, bx, by, cx, cy, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || bboxContainsTriangle(ax, ay, bx, by, cx, cy, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return bboxContainsTriangle(ax, ay, bx, by, cx, cy, minX, maxX, minY, maxY);
  }

  /**
   * static utility method to compare a bbox with a range of triangles (just the bbox of the triangle collection)
   **/
  private static Relation compareBBoxToRangeBBox(final byte[] bbox,
                                                 int minXOffset, int minYOffset, byte[] minTriangle,
                                                 int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    // check bounding box (DISJOINT)
    if (disjoint(bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }

    if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
        Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
        Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0 &&
        Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
      return Relation.CELL_INSIDE_QUERY;
    }

    return Relation.CELL_CROSSES_QUERY;
  }

  /**
   * static utility method to compare a bbox with a range of triangles (just the bbox of the triangle collection)
   * for intersection
   **/
  private static Relation intersectBBoxWithRangeBBox(final byte[] bbox,
                                                     int minXOffset, int minYOffset, byte[] minTriangle,
                                                     int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    // check bounding box (DISJOINT)
    if (disjoint(bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
      return Relation.CELL_OUTSIDE_QUERY;
    }

    if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
        Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0 ) {
      if (Arrays.compareUnsigned(maxTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }
      if (Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          Arrays.compareUnsigned(maxTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }
    }

    if (Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
        Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0 ) {
      if (Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          Arrays.compareUnsigned(minTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) >= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }
      if (Arrays.compareUnsigned(minTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }
    }

    return Relation.CELL_CROSSES_QUERY;
  }

  /**
   * static utility method to check a bbox is disjoint with a range of triangles
   **/
  private static boolean disjoint(final byte[] bbox,
                               int minXOffset, int minYOffset, byte[] minTriangle,
                               int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      return Arrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) > 0 ||
          Arrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) < 0 ||
          Arrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) > 0 ||
          Arrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) < 0;
  }

  /**
   * encodes a bounding box into the provided byte array
   */
  private static void encode(final int minX, final int maxX, final int minY, final int maxY, byte[] b) {
    if (b == null) {
      b = new byte[4 * BYTES];
    }
    NumericUtils.intToSortableBytes(minY, b, 0);
    NumericUtils.intToSortableBytes(minX, b, BYTES);
    NumericUtils.intToSortableBytes(maxY, b, 2 * BYTES);
    NumericUtils.intToSortableBytes(maxX, b, 3 * BYTES);
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

  /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
  private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
    if (this.crossesDateline() == true) {
      return edgeIntersectsBox(ax, ay, bx, by, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
          || edgeIntersectsBox(ax, ay, bx, by, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
    }
    return edgeIntersectsBox(ax, ay, bx, by, this.minX, this.maxX, this.minY, this.maxY);
  }

  /** static utility method to check if a bounding box contains a point */
  private static boolean bboxContainsPoint(int x, int y, int minX, int maxX, int minY, int maxY) {
    return (x < minX || x > maxX || y < minY || y > maxY) == false;
  }

  /** static utility method to check if a bounding box contains a triangle */
  private static boolean bboxContainsTriangle(int ax, int ay, int bx, int by, int cx, int cy,
                                             int minX, int maxX, int minY, int maxY) {
    return bboxContainsPoint(ax, ay, minX, maxX, minY, maxY)
        && bboxContainsPoint(bx, by, minX, maxX, minY, maxY)
        && bboxContainsPoint(cx, cy, minX, maxX, minY, maxY);
  }

  /** returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query */
  private static boolean edgeIntersectsBox(int ax, int ay, int bx, int by,
                                           int minX, int maxX, int minY, int maxY) {
    // shortcut: if edge is a point (occurs w/ Line shapes); simply check bbox w/ point
    if (ax == bx && ay == by) {
      return Rectangle.containsPoint(ay, ax, minY, maxY, minX, maxX);
    }

    // shortcut: check if either of the end points fall inside the box
    if (bboxContainsPoint(ax, ay, minX, maxX, minY, maxY) ||
        bboxContainsPoint(bx, by, minX, maxX, minY, maxY)) {
      return true;
    }

    // shortcut: check bboxes of edges are disjoint
    if (boxesAreDisjoint(Math.min(ax, bx), Math.max(ax, bx), Math.min(ay, by), Math.max(ay, by),
        minX, maxX, minY, maxY)) {
      return false;
    }

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

  /** utility method to check if two boxes are disjoint */
  private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                         final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
    return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof Rectangle2D)) return false;
    Rectangle2D that = (Rectangle2D) o;
    return minX == that.minX &&
        maxX == that.maxX &&
        minY == that.minY &&
        maxY == that.maxY &&
        Arrays.equals(bbox, that.bbox) &&
        Arrays.equals(west, that.west);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(minX, maxX, minY, maxY);
    result = 31 * result + Arrays.hashCode(bbox);
    result = 31 * result + Arrays.hashCode(west);
    return result;
  }
}
