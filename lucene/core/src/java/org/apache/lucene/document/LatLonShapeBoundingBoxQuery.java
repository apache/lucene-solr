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

import static java.lang.Integer.BYTES;
import static org.apache.lucene.geo.GeoEncodingUtils.MAX_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.MIN_LON_ENCODED;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitudeCeil;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitudeCeil;

import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.document.ShapeField.QueryRelation;
import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.NumericUtils;

/**
 * Finds all previously indexed geo shapes that intersect the specified bounding box.
 *
 * <p>The field must be indexed using {@link
 * org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 */
final class LatLonShapeBoundingBoxQuery extends SpatialQuery {
  private final Rectangle rectangle;

  LatLonShapeBoundingBoxQuery(String field, QueryRelation queryRelation, Rectangle rectangle) {
    super(field, queryRelation);
    this.rectangle = rectangle;
  }

  @Override
  protected SpatialVisitor getSpatialVisitor() {
    final EncodedRectangle encodedRectangle =
            new EncodedRectangle(
                    rectangle.minLat, rectangle.maxLat, rectangle.minLon, rectangle.maxLon);
    return new SpatialVisitor() {

      @Override
      protected Relation relate(byte[] minTriangle, byte[] maxTriangle) {
        if (queryRelation == QueryRelation.INTERSECTS || queryRelation == QueryRelation.DISJOINT) {
          return encodedRectangle.intersectRangeBBox(
                  ShapeField.BYTES,
                  0,
                  minTriangle,
                  3 * ShapeField.BYTES,
                  2 * ShapeField.BYTES,
                  maxTriangle);
        }
        return encodedRectangle.relateRangeBBox(
                ShapeField.BYTES,
                0,
                minTriangle,
                3 * ShapeField.BYTES,
                2 * ShapeField.BYTES,
                maxTriangle);
      }

      @Override
      protected Predicate<byte[]> intersects() {
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
            {
              return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
            }
            case LINE:
            {
              int aY = scratchTriangle.aY;
              int aX = scratchTriangle.aX;
              int bY = scratchTriangle.bY;
              int bX = scratchTriangle.bX;
              return encodedRectangle.intersectsLine(aX, aY, bX, bY);
            }
            case TRIANGLE:
            {
              int aY = scratchTriangle.aY;
              int aX = scratchTriangle.aX;
              int bY = scratchTriangle.bY;
              int bX = scratchTriangle.bX;
              int cY = scratchTriangle.cY;
              int cX = scratchTriangle.cX;
              return encodedRectangle.intersectsTriangle(aX, aY, bX, bY, cX, cY);
            }
            default:
              throw new IllegalArgumentException(
                      "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }

      @Override
      protected Predicate<byte[]> within() {
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
            {
              return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY);
            }
            case LINE:
            {
              int aY = scratchTriangle.aY;
              int aX = scratchTriangle.aX;
              int bY = scratchTriangle.bY;
              int bX = scratchTriangle.bX;
              return encodedRectangle.containsLine(aX, aY, bX, bY);
            }
            case TRIANGLE:
            {
              int aY = scratchTriangle.aY;
              int aX = scratchTriangle.aX;
              int bY = scratchTriangle.bY;
              int bX = scratchTriangle.bX;
              int cY = scratchTriangle.cY;
              int cX = scratchTriangle.cX;
              return encodedRectangle.containsTriangle(aX, aY, bX, bY, cX, cY);
            }
            default:
              throw new IllegalArgumentException(
                      "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }

      @Override
      protected Function<byte[], Component2D.WithinRelation> contains() {
        if (encodedRectangle.crossesDateline()) {
          throw new IllegalArgumentException(
                  "withinTriangle is not supported for rectangles crossing the date line");
        }
        final ShapeField.DecodedTriangle scratchTriangle = new ShapeField.DecodedTriangle();
        return triangle -> {

          // decode indexed triangle
          ShapeField.decodeTriangle(triangle, scratchTriangle);

          switch (scratchTriangle.type) {
            case POINT:
            {
              return encodedRectangle.contains(scratchTriangle.aX, scratchTriangle.aY)
                      ? Component2D.WithinRelation.NOTWITHIN
                      : Component2D.WithinRelation.DISJOINT;
            }
            case LINE:
            {
              return encodedRectangle.withinLine(
                      scratchTriangle.aX,
                      scratchTriangle.aY,
                      scratchTriangle.ab,
                      scratchTriangle.bX,
                      scratchTriangle.bY);
            }
            case TRIANGLE:
            {
              return encodedRectangle.withinTriangle(
                      scratchTriangle.aX,
                      scratchTriangle.aY,
                      scratchTriangle.ab,
                      scratchTriangle.bX,
                      scratchTriangle.bY,
                      scratchTriangle.bc,
                      scratchTriangle.cX,
                      scratchTriangle.cY,
                      scratchTriangle.ca);
            }
            default:
              throw new IllegalArgumentException(
                      "Unsupported triangle type :[" + scratchTriangle.type + "]");
          }
        };
      }
    };
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle.equals(((LatLonShapeBoundingBoxQuery) o).rectangle);
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
    private final boolean crossesDateline;

    EncodedRectangle(double minLat, double maxLat, double minLon, double maxLon) {
      this.bbox = new byte[4 * BYTES];
      if (minLon == 180.0 && minLon > maxLon) {
        minLon = -180;
      }
      this.minX = encodeLongitudeCeil(minLon);
      this.maxX = encodeLongitude(maxLon);
      this.minY = encodeLatitudeCeil(minLat);
      this.maxY = encodeLatitude(maxLat);
      this.crossesDateline = minLon > maxLon;
      if (this.crossesDateline) {
        // crossing dateline is split into east/west boxes
        this.west = new byte[4 * BYTES];
        encode(MIN_LON_ENCODED, this.maxX, this.minY, this.maxY, this.west);
        encode(this.minX, MAX_LON_ENCODED, this.minY, this.maxY, this.bbox);
      } else {
        this.west = null;
        encode(this.minX, this.maxX, this.minY, this.maxY, bbox);
      }
    }

    /** encodes a bounding box into the provided byte array */
    private static void encode(
            final int minX, final int maxX, final int minY, final int maxY, byte[] b) {
      if (b == null) {
        b = new byte[4 * BYTES];
      }
      NumericUtils.intToSortableBytes(minY, b, 0);
      NumericUtils.intToSortableBytes(minX, b, BYTES);
      NumericUtils.intToSortableBytes(maxY, b, 2 * BYTES);
      NumericUtils.intToSortableBytes(maxX, b, 3 * BYTES);
    }

    private boolean crossesDateline() {
      return crossesDateline;
    }

    /** compare this to a provided range bounding box */
    Relation relateRangeBBox(
            int minXOffset,
            int minYOffset,
            byte[] minTriangle,
            int maxXOffset,
            int maxYOffset,
            byte[] maxTriangle) {
      Relation eastRelation =
              compareBBoxToRangeBBox(
                      this.bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return compareBBoxToRangeBBox(
                this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /** intersects this to a provided range bounding box */
    Relation intersectRangeBBox(
            int minXOffset,
            int minYOffset,
            byte[] minTriangle,
            int maxXOffset,
            int maxYOffset,
            byte[] maxTriangle) {
      Relation eastRelation =
              intersectBBoxWithRangeBBox(
                      this.bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      if (this.crossesDateline() && eastRelation == Relation.CELL_OUTSIDE_QUERY) {
        return intersectBBoxWithRangeBBox(
                this.west, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
      }
      return eastRelation;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the
     * triangle collection)
     */
    private static Relation compareBBoxToRangeBBox(
            final byte[] bbox,
            int minXOffset,
            int minYOffset,
            byte[] minTriangle,
            int maxXOffset,
            int maxYOffset,
            byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(
              bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (FutureArrays.compareUnsigned(
              minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES)
              >= 0
              && FutureArrays.compareUnsigned(
              maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES)
              <= 0
              && FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES)
              >= 0
              && FutureArrays.compareUnsigned(
              maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES)
              <= 0) {
        return Relation.CELL_INSIDE_QUERY;
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /**
     * static utility method to compare a bbox with a range of triangles (just the bbox of the
     * triangle collection) for intersection
     */
    private static Relation intersectBBoxWithRangeBBox(
            final byte[] bbox,
            int minXOffset,
            int minYOffset,
            byte[] minTriangle,
            int maxXOffset,
            int maxYOffset,
            byte[] maxTriangle) {
      // check bounding box (DISJOINT)
      if (disjoint(
              bbox, minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle)) {
        return Relation.CELL_OUTSIDE_QUERY;
      }

      if (FutureArrays.compareUnsigned(
              minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES)
              >= 0
              && FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES)
              >= 0) {
        if (FutureArrays.compareUnsigned(
                maxTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES)
                <= 0
                && FutureArrays.compareUnsigned(
                maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES)
                <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (FutureArrays.compareUnsigned(
                maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES)
                <= 0
                && FutureArrays.compareUnsigned(
                maxTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES)
                <= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      if (FutureArrays.compareUnsigned(
              maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES)
              <= 0
              && FutureArrays.compareUnsigned(
              maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES)
              <= 0) {
        if (FutureArrays.compareUnsigned(
                minTriangle, minXOffset, minXOffset + BYTES, bbox, BYTES, 2 * BYTES)
                >= 0
                && FutureArrays.compareUnsigned(minTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES)
                >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
        if (FutureArrays.compareUnsigned(
                minTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES)
                >= 0
                && FutureArrays.compareUnsigned(minTriangle, minYOffset, minYOffset + BYTES, bbox, 0, BYTES)
                >= 0) {
          return Relation.CELL_INSIDE_QUERY;
        }
      }

      return Relation.CELL_CROSSES_QUERY;
    }

    /** static utility method to check a bbox is disjoint with a range of triangles */
    private static boolean disjoint(
            final byte[] bbox,
            int minXOffset,
            int minYOffset,
            byte[] minTriangle,
            int maxXOffset,
            int maxYOffset,
            byte[] maxTriangle) {
      return FutureArrays.compareUnsigned(
              minTriangle, minXOffset, minXOffset + BYTES, bbox, 3 * BYTES, 4 * BYTES)
              > 0
              || FutureArrays.compareUnsigned(
              maxTriangle, maxXOffset, maxXOffset + BYTES, bbox, BYTES, 2 * BYTES)
              < 0
              || FutureArrays.compareUnsigned(
              minTriangle, minYOffset, minYOffset + BYTES, bbox, 2 * BYTES, 3 * BYTES)
              > 0
              || FutureArrays.compareUnsigned(maxTriangle, maxYOffset, maxYOffset + BYTES, bbox, 0, BYTES)
              < 0;
    }

    /** Checks if the rectangle contains the provided point */
    boolean contains(int x, int y) {
      if (y < minY || y > maxY) {
        return false;
      }
      if (crossesDateline()) {
        return (x > maxX && x < minX) == false;
      } else {
        return (x > maxX || x < minX) == false;
      }
    }

    /** Checks if the rectangle intersects the provided LINE */
    boolean intersectsLine(int aX, int aY, int bX, int bY) {
      if (contains(aX, aY) || contains(bX, bY)) {
        return true;
      }
      // check bounding boxes are disjoint
      if (StrictMath.max(aY, bY) < minY || StrictMath.min(aY, bY) > maxY) {
        return false;
      }
      if (crossesDateline) { // crosses dateline
        if (StrictMath.min(aX, bX) > maxX && StrictMath.max(aX, bX) < minX) {
          return false;
        }
      } else {
        if (StrictMath.min(aX, bX) > maxX || StrictMath.max(aX, bX) < minX) {
          return false;
        }
      }
      // expensive part
      return edgeIntersectsQuery(aX, aY, bX, bY);
    }

    /** Checks if the rectangle intersects the provided triangle */
    boolean intersectsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      // query contains any triangle points
      if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
        return true;
      }
      // check bounding box of triangle
      int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
      int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);
      // check bounding boxes are disjoint
      if (tMaxY < minY || tMinY > maxY) {
        return false;
      }
      int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
      int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
      if (crossesDateline) { // crosses dateline
        if (tMinX > maxX && tMaxX < minX) {
          return false;
        }
      } else {
        if (tMinX > maxX || tMaxX < minX) {
          return false;
        }
      }
      // expensive part
      return Component2D.pointInTriangle(
              tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)
              || edgeIntersectsQuery(aX, aY, bX, bY)
              || edgeIntersectsQuery(bX, bY, cX, cY)
              || edgeIntersectsQuery(cX, cY, aX, aY);
    }

    /** Checks if the rectangle contains the provided LINE */
    boolean containsLine(int aX, int aY, int bX, int bY) {
      if (aY < minY || bY < minY || aY > maxY || bY > maxY) {
        return false;
      }
      if (crossesDateline) { // crosses dateline
        return (aX >= minX && bX >= minX) || (aX <= maxX && bX <= maxX);
      } else {
        return aX >= minX && bX >= minX && aX <= maxX && bX <= maxX;
      }
    }

    /** Checks if the rectangle contains the provided triangle */
    boolean containsTriangle(int aX, int aY, int bX, int bY, int cX, int cY) {
      if (aY < minY || bY < minY || cY < minY || aY > maxY || bY > maxY || cY > maxY) {
        return false;
      }
      if (crossesDateline) { // crosses dateline
        return (aX >= minX && bX >= minX && cX >= minX) || (aX <= maxX && bX <= maxX && cX <= maxX);
      } else {
        return aX >= minX && bX >= minX && cX >= minX && aX <= maxX && bX <= maxX && cX <= maxX;
      }
    }

    /** Returns the Within relation to the provided triangle */
    Component2D.WithinRelation withinLine(int ax, int ay, boolean ab, int bx, int by) {
      if (ab == true && edgeIntersectsBox(ax, ay, bx, by, minX, maxX, minY, maxY) == true) {
        return Component2D.WithinRelation.NOTWITHIN;
      }
      return Component2D.WithinRelation.DISJOINT;
    }

    /** Returns the Within relation to the provided triangle */
    Component2D.WithinRelation withinTriangle(
            int aX, int aY, boolean ab, int bX, int bY, boolean bc, int cX, int cY, boolean ca) {
      // Points belong to the shape so if points are inside the rectangle then it cannot be within.
      if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
        return Component2D.WithinRelation.NOTWITHIN;
      }

      // Bounding boxes disjoint?
      int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
      int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);
      // check bounding boxes are disjoint
      if (tMaxY < minY || tMinY > maxY) {
        return Component2D.WithinRelation.DISJOINT;
      }
      int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
      int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
      if (crossesDateline) { // crosses dateline
        if (tMinX > maxX && tMaxX < minX) {
          return Component2D.WithinRelation.DISJOINT;
        }
      } else {
        if (tMinX > maxX || tMaxX < minX) {
          return Component2D.WithinRelation.DISJOINT;
        }
      }
      // If any of the edges intersects an edge belonging to the shape then it cannot be within.
      Component2D.WithinRelation relation = Component2D.WithinRelation.DISJOINT;
      if (edgeIntersectsBox(aX, aY, bX, bY, minX, maxX, minY, maxY) == true) {
        if (ab == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      if (edgeIntersectsBox(bX, bY, cX, cY, minX, maxX, minY, maxY) == true) {
        if (bc == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }

      if (edgeIntersectsBox(cX, cY, aX, aY, minX, maxX, minY, maxY) == true) {
        if (ca == true) {
          return Component2D.WithinRelation.NOTWITHIN;
        } else {
          relation = Component2D.WithinRelation.CANDIDATE;
        }
      }
      // Check if shape is within the triangle
      if (relation == Component2D.WithinRelation.CANDIDATE
              || Component2D.pointInTriangle(
              tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)) {
        return Component2D.WithinRelation.CANDIDATE;
      }
      return relation;
    }

    /** returns true if the edge (defined by (aX, aY) (bX, bY)) intersects the query */
    private boolean edgeIntersectsQuery(int aX, int aY, int bX, int bY) {
      if (crossesDateline) {
        return edgeIntersectsBox(aX, aY, bX, bY, MIN_LON_ENCODED, this.maxX, this.minY, this.maxY)
                || edgeIntersectsBox(aX, aY, bX, bY, this.minX, MAX_LON_ENCODED, this.minY, this.maxY);
      }
      return edgeIntersectsBox(aX, aY, bX, bY, this.minX, this.maxX, this.minY, this.maxY);
    }

    /** returns true if the edge (defined by (aX, aY) (bX, bY)) intersects the box */
    private static boolean edgeIntersectsBox(
            int aX, int aY, int bX, int bY, int minX, int maxX, int minY, int maxY) {
      if (Math.max(aX, bX) < minX
              || Math.min(aX, bX) > maxX
              || Math.min(aY, bY) > maxY
              || Math.max(aY, bY) < minY) {
        return false;
      }
      return GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, maxY, maxX, maxY)
              || // top
              GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, maxY, maxX, minY)
              || // bottom
              GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, maxX, minY, minX, minY)
              || // left
              GeoUtils.lineCrossesLineWithBoundary(aX, aY, bX, bY, minX, minY, minX, maxY); // right
    }
  }
}
