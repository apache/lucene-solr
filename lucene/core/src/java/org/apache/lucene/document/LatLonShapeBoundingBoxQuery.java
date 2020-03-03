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

import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;
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
 * Finds all previously indexed geo shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 **/
final class LatLonShapeBoundingBoxQuery extends ShapeQuery {
  private final Rectangle rectangle;
  private final EncodedRectangle encodedRectangle;

  LatLonShapeBoundingBoxQuery(String field, QueryRelation queryRelation, Rectangle rectangle) {
    super(field, queryRelation);
    this.rectangle = rectangle;
    this.encodedRectangle = new EncodedRectangle(rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
  }

  @Override
  protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                            int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    if (queryRelation == QueryRelation.INTERSECTS || queryRelation == QueryRelation.DISJOINT) {
      return encodedRectangle.intersectRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
    }
    return encodedRectangle.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, ShapeField.DecodedTriangle scratchTriangle, QueryRelation queryRelation) {
    // decode indexed triangle
    ShapeField.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle.aY;
    int aX = scratchTriangle.aX;
    int bY = scratchTriangle.bY;
    int bX = scratchTriangle.bX;
    int cY = scratchTriangle.cY;
    int cX = scratchTriangle.cX;

    switch (queryRelation) {
      case INTERSECTS: return encodedRectangle.intersectsTriangle(aX, aY, bX, bY, cX, cY);
      case WITHIN: return encodedRectangle.containsTriangle(aX, aY, bX, bY, cX, cY);
      case DISJOINT: return encodedRectangle.intersectsTriangle(aX, aY, bX, bY, cX, cY) == false;
      default: throw new IllegalArgumentException("Unsupported query type :[" + queryRelation + "]");
    }
  }

  @Override
  protected Component2D.WithinRelation queryWithin(byte[] t, ShapeField.DecodedTriangle scratchTriangle) {
    // decode indexed triangle
    ShapeField.decodeTriangle(t, scratchTriangle);

    return encodedRectangle.withinTriangle(scratchTriangle.aX, scratchTriangle.aY, scratchTriangle.ab,
                                      scratchTriangle.bX, scratchTriangle.bY, scratchTriangle.bc,
                                      scratchTriangle.cX, scratchTriangle.cY, scratchTriangle.ca);
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle.equals(((LatLonShapeBoundingBoxQuery)o).rectangle);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle.hashCode();
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
    sb.append(rectangle.toString());
    return sb.toString();
  }

  /** Holds spatial logic for a bounding box that works in the encoded space */
  private static class EncodedRectangle {
    protected final byte[] bbox;
    private final byte[] west;
    protected final int minX;
    protected final int maxX;
    protected final int minY;
    protected final int maxY;

    EncodedRectangle(double minLat, double maxLat, double minLon, double maxLon) {
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

    private boolean crossesDateline() {
      return minX > maxX;
    }

    /**
     * Checks if the rectangle contains the provided point
     **/
    boolean queryContainsPoint(int x, int y) {
      if (this.crossesDateline() == true) {
        return bboxContainsPoint(x, y, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
            || bboxContainsPoint(x, y, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
      }
      return bboxContainsPoint(x, y, this.minX, this.maxX, this.minY, this.maxY);
    }

    /**
     * compare this to a provided range bounding box
     **/
   Relation relateRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                    int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      Relation eastRelation = compareBBoxToRangeBBox(this.bbox,
          minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return compareBBoxToRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * intersects this to a provided range bounding box
     **/
    Relation intersectRangeBBox(int minXOffset, int minYOffset, byte[] minTriangle,
                                       int maxXOffset, int maxYOffset, byte[] maxTriangle) {
      Relation eastRelation = intersectBBoxWithRangeBBox(this.bbox,
          minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return intersectBBoxWithRangeBBox(this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * Checks if the rectangle intersects the provided triangle
     **/
    boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
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

    /**
     * Returns the Within relation to the provided triangle
     */
    Component2D.WithinRelation withinTriangle(int ax, int ay, boolean ab, int bx, int by, boolean bc, int cx, int cy, boolean ca) {
      if (this.crossesDateline() == true) {
        throw new IllegalArgumentException("withinTriangle is not supported for rectangles crossing the date line");
      }
      // Short cut, lines and points cannot contain a bbox
      if ((ax == bx && ay == by) || (ax == cx && ay == cy) || (bx == cx && by == cy)) {
        return Component2D.WithinRelation.DISJOINT;
      }
      // Compute bounding box of triangle
      int tMinX = StrictMath.min(StrictMath.min(ax, bx), cx);
      int tMaxX = StrictMath.max(StrictMath.max(ax, bx), cx);
      int tMinY = StrictMath.min(StrictMath.min(ay, by), cy);
      int tMaxY = StrictMath.max(StrictMath.max(ay, by), cy);
      // Bounding boxes disjoint?
      if (boxesAreDisjoint(tMinX, tMaxX, tMinY, tMaxY, minX, maxX, minY, maxY)) {
        return Component2D.WithinRelation.DISJOINT;
      }
      // Points belong to the shape so if points are inside the rectangle then it cannot be within.
      if (bboxContainsPoint(ax, ay, minX, maxX, minY, maxY) ||
          bboxContainsPoint(bx, by, minX, maxX, minY, maxY) ||
          bboxContainsPoint(cx, cy, minX, maxX, minY, maxY)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }
      // If any of the edges intersects an edge belonging to the shape then it cannot be within.
      Component2D.WithinRelation relation = Component2D.WithinRelation.DISJOINT;
      if (edgeIntersectsBox(ax, ay, bx, by, minX, maxX, minY, maxY) == true) {
        if (ab == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      if (edgeIntersectsBox(bx, by, cx, cy, minX, maxX, minY, maxY) == true) {
        if (bc == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }

      if (edgeIntersectsBox(cx, cy, ax, ay, minX, maxX, minY, maxY) == true) {
        if (ca == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      // If any of the rectangle edges crosses a triangle edge that does not belong to the shape
      // then it is a candidate for within
      if (relation == Component2D.WithinRelation.CANDIDATE) {
        return Component2D.WithinRelation.CANDIDATE;
      }
      // Check if shape is within the triangle
      if (Tessellator.pointInTriangle(minX, minY, ax, ay, bx, by, cx, cy)) {
        return Component2D.WithinRelation.CANDIDATE;
      }
      return relation;
    }

    /**
     * Checks if the rectangle contains the provided triangle
     **/
    boolean containsTriangle(int ax, int ay, int bx, int by, int cx, int cy) {
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

      if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0 &&
          FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
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

      if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
          FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0) {
        if (FutureArrays.compareUnsigned(maxTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
            FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
            FutureArrays.compareUnsigned(maxTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      if (FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) <= 0 &&
          FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) <= 0) {
        if (FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
            FutureArrays.compareUnsigned(minTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (FutureArrays.compareUnsigned(minTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) >= 0 &&
            FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES) >= 0) {
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
      return FutureArrays.compareUnsigned(minTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES) > 0 ||
          FutureArrays.compareUnsigned(maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES) < 0 ||
          FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES) > 0 ||
          FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES) < 0;
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

    /**
     * returns true if the query intersects the provided triangle (in encoded space)
     */
    private boolean queryIntersects(int ax, int ay, int bx, int by, int cx, int cy) {
      // check each edge of the triangle against the query
      if (edgeIntersectsQuery(ax, ay, bx, by) ||
          edgeIntersectsQuery(bx, by, cx, cy) ||
          edgeIntersectsQuery(cx, cy, ax, ay)) {
        return true;
      }
      return false;
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
    private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
      if (this.crossesDateline() == true) {
        return edgeIntersectsBox(ax, ay, bx, by, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
            || edgeIntersectsBox(ax, ay, bx, by, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
      }
      return edgeIntersectsBox(ax, ay, bx, by, this.minX, this.maxX, this.minY, this.maxY);
    }

    /**
     * static utility method to check if a bounding box contains a point
     */
    private static boolean bboxContainsPoint(int x, int y, int minX, int maxX, int minY, int maxY) {
      return (x < minX || x > maxX || y < minY || y > maxY) == false;
    }

    /**
     * static utility method to check if a bounding box contains a triangle
     */
    private static boolean bboxContainsTriangle(int ax, int ay, int bx, int by, int cx, int cy,
                                                int minX, int maxX, int minY, int maxY) {
      return bboxContainsPoint(ax, ay, minX, maxX, minY, maxY)
          && bboxContainsPoint(bx, by, minX, maxX, minY, maxY)
          && bboxContainsPoint(cx, cy, minX, maxX, minY, maxY);
    }

    /**
     * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
     */
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

    /**
     * utility method to check if two boxes are disjoint
     */
    private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                            final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
      return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
    }
  }
}
