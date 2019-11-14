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

import java.util.Objects;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.geo.Tessellator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

/**
 * A base shape utility class used for both LatLon (spherical) and XY (cartesian) shape fields.
 * <p>
 * {@link Polygon}'s and {@link Line}'s are decomposed into a triangular mesh using the {@link Tessellator} utility class.
 * Each {@link Triangle} is encoded by this base class and indexed as a seven dimension multi-value field.
 * <p>
 * Finding all shapes that intersect a range (e.g., bounding box), or target shape, at search time is efficient.
 * <p>
 * This class defines the static methods for encoding the three vertices of a tessellated triangles as a seven dimension point.
 * The coordinates are converted from double precision values into 32 bit integers so they are sortable at index time.
 * <p>
 *
 * @lucene.experimental
 */
public final class ShapeField {
  /** vertex coordinates are encoded as 4 byte integers */
  static final int BYTES = Integer.BYTES;

  /** tessellated triangles are seven dimensions; the first four are the bounding box index dimensions */
  protected static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(7, 4, BYTES);
    TYPE.freeze();
  }

  // no instance:
  private ShapeField() {
  }

  /** polygons are decomposed into tessellated triangles using {@link org.apache.lucene.geo.Tessellator}
   * these triangles are encoded and inserted as separate indexed POINT fields
   */
  public static class Triangle extends Field {

    // constructor for points
    Triangle(String name, int aXencoded, int aYencoded) {
      super(name, TYPE);
      setTriangleValue(aXencoded, aYencoded);
    }

    // constructor for lines
    Triangle(String name, int aXencoded, int aYencoded, int bXencoded, int bYencoded) {
      super(name, TYPE);
      if (aXencoded == bXencoded && aYencoded == bYencoded) {
        setTriangleValue(aXencoded, aYencoded);
      } else {
        setTriangleValue(aXencoded, aYencoded,  bXencoded, bYencoded);
      }
    }

    // constructor for triangles
    Triangle(String name, Tessellator.Triangle t) {
      super(name, TYPE);
      int aX = t.getEncodedX(0);
      int aY = t.getEncodedY(0);
      int bX = t.getEncodedX(1);
      int bY = t.getEncodedY(1);
      int cX = t.getEncodedX(2);
      int cY = t.getEncodedY(2);
      setTriangleValue(aX, aY, t.isEdgefromPolygon(0),
          bX, bY, t.isEdgefromPolygon(1),
          cX, cY, t.isEdgefromPolygon(2));
    }

    /** sets the vertices of the triangle as integer encoded values */
    protected void setTriangleValue(int aX, int aY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }
      encodePoint(bytes, aY, aX);
    }

    /** sets the vertices of the triangle as integer encoded values */
    protected void setTriangleValue(int aX, int aY, int bX, int bY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }
      encodeLine(bytes, aY, aX, bY, bX);
    }

    /** sets the vertices of the triangle as integer encoded values */
    protected void setTriangleValue(int aX, int aY, boolean abFromShape, int bX, int bY, boolean bcFromShape, int cX, int cY, boolean caFromShape) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }
      encodeTriangle(bytes, aY, aX, abFromShape, bY, bX, bcFromShape, cY, cX, caFromShape);
    }
  }

  /** Query Relation Types **/
  public enum QueryRelation {
    INTERSECTS, WITHIN, DISJOINT
  }

  private static final int MINY_MINX_MAXY_MAXX_Y_X = 0;
  private static final int MINY_MINX_Y_X_MAXY_MAXX = 1;
  private static final int MAXY_MINX_Y_X_MINY_MAXX = 2;
  private static final int MAXY_MINX_MINY_MAXX_Y_X = 3;
  private static final int Y_MINX_MINY_X_MAXY_MAXX = 4;
  private static final int Y_MINX_MINY_MAXX_MAXY_X = 5;
  private static final int MAXY_MINX_MINY_X_Y_MAXX = 6;
  private static final int MINY_MINX_Y_MAXX_MAXY_X = 7;

  public static void encodePoint(byte[] bytes, int aY, int aX) {
    int bits =  MINY_MINX_MAXY_MAXX_Y_X;
    NumericUtils.intToSortableBytes(aY, bytes, 0);
    NumericUtils.intToSortableBytes(aX, bytes, BYTES);
    NumericUtils.intToSortableBytes(aY, bytes, 2 * BYTES);
    NumericUtils.intToSortableBytes(aX, bytes, 3 * BYTES);
    NumericUtils.intToSortableBytes(aY, bytes, 4 * BYTES);
    NumericUtils.intToSortableBytes(aX, bytes, 5 * BYTES);
    bits |= 1 << 3;
    bits |= 1 << 4;
    bits |= 1 << 5;
    bits |= 1 << 6; // type point
    NumericUtils.intToSortableBytes(bits, bytes, 6 * BYTES);
  }

  public static void encodeLine(byte[] bytes, int aY, int aX, int bY, int bX) {
    int bits;
    if (aX > bX) {
      if (aY > bY) {
        bits = MINY_MINX_MAXY_MAXX_Y_X;
        NumericUtils.intToSortableBytes(bY, bytes, 0);
        NumericUtils.intToSortableBytes(bX, bytes, BYTES);
        NumericUtils.intToSortableBytes(aY, bytes, 2 * BYTES);
        NumericUtils.intToSortableBytes(aX, bytes, 3 * BYTES);
        NumericUtils.intToSortableBytes(bY, bytes, 4 * BYTES);
        NumericUtils.intToSortableBytes(bX, bytes, 5 * BYTES);
      } else {
        bits = MAXY_MINX_MINY_MAXX_Y_X;
        NumericUtils.intToSortableBytes(aY, bytes, 0);
        NumericUtils.intToSortableBytes(bX, bytes, BYTES);
        NumericUtils.intToSortableBytes(bY, bytes, 2 * BYTES);
        NumericUtils.intToSortableBytes(aX, bytes, 3 * BYTES);
        NumericUtils.intToSortableBytes(aY, bytes, 4 * BYTES);
        NumericUtils.intToSortableBytes(aX, bytes, 5 * BYTES);
      }
    } else if (aY > bY){
      bits = MAXY_MINX_MINY_MAXX_Y_X;
      NumericUtils.intToSortableBytes(bY, bytes, 0);
      NumericUtils.intToSortableBytes(aX, bytes, BYTES);
      NumericUtils.intToSortableBytes(aY, bytes, 2 * BYTES);
      NumericUtils.intToSortableBytes(bX, bytes, 3 * BYTES);
      NumericUtils.intToSortableBytes(aY, bytes, 4 * BYTES);
      NumericUtils.intToSortableBytes(aX, bytes, 5 * BYTES);
    } else {
      bits = MINY_MINX_MAXY_MAXX_Y_X;
      NumericUtils.intToSortableBytes(aY, bytes, 0);
      NumericUtils.intToSortableBytes(aX, bytes, BYTES);
      NumericUtils.intToSortableBytes(bY, bytes, 2 * BYTES);
      NumericUtils.intToSortableBytes(bX, bytes, 3 * BYTES);
      NumericUtils.intToSortableBytes(aY, bytes, 4 * BYTES);
      NumericUtils.intToSortableBytes(aX, bytes, 5 * BYTES);
    }
    bits |= 1 << 3;
    bits |= 1 << 4;
    bits |= 1 << 5;
    bits |= 1 << 7; // type line
    NumericUtils.intToSortableBytes(bits, bytes, 6 * BYTES);
  }

  /**
   * A triangle is encoded using 6 points and an extra point with encoded information in three bits of how to reconstruct it.
   * Triangles are encoded with CCW orientation and might be rotated to limit the number of possible reconstructions to 2^3.
   * Reconstruction always happens from west to east.
   */
  public static void encodeTriangle(byte[] bytes, int aLat, int aLon, boolean abFromShape, int bLat, int bLon, boolean bcFromShape, int cLat, int cLon, boolean caFromShape) {
    assert bytes.length == 7 * BYTES;
    if (aLon == bLon && aLat == bLat) {
      if (aLon == cLon && aLat == cLat) {
        assert abFromShape || bcFromShape || caFromShape;
        encodePoint(bytes, aLat, aLon);
        return;
      } else {
        assert abFromShape || bcFromShape || caFromShape;
        encodeLine(bytes, aLat, aLon, cLat, cLon);
        return;
      }
    } else if (aLon == cLon && aLat == cLat) {
      assert abFromShape || bcFromShape || caFromShape;
      encodeLine(bytes, aLat, aLon, bLat, bLon);
      return;
    }
    int aX;
    int bX;
    int cX;
    int aY;
    int bY;
    int cY;
    boolean ab, bc, ca;
    // change orientation if CW
    if (GeoUtils.orient(aLon, aLat, bLon, bLat, cLon, cLat) == -1) {
      aX = cLon;
      bX = bLon;
      cX = aLon;
      aY = cLat;
      bY = bLat;
      cY = aLat;
      ab = bcFromShape;
      bc = abFromShape;
      ca = caFromShape;
    } else {
      aX = aLon;
      bX = bLon;
      cX = cLon;
      aY = aLat;
      bY = bLat;
      cY = cLat;
      ab = abFromShape;
      bc = bcFromShape;
      ca = caFromShape;
    }
    // rotate edges and place minX at the beginning
    if (bX < aX || cX < aX) {
      if (bX < cX) {
        int tempX = aX;
        int tempY = aY;
        boolean tempBool = ab;
        aX = bX;
        aY = bY;
        ab = bc;
        bX = cX;
        bY = cY;
        bc = ca;
        cX = tempX;
        cY = tempY;
        ca = tempBool;
      } else if (cX < aX) {
        int tempX = aX;
        int tempY = aY;
        boolean tempBool = ab;
        aX = cX;
        aY = cY;
        ab = ca;
        cX = bX;
        cY = bY;
        ca = bc;
        bX = tempX;
        bY = tempY;
        bc = tempBool;
      }
    } else if (aX == bX && aX == cX) {
      // degenerated case, all points with same longitude
      // we need to prevent that aX is in the middle (not part of the MBS)
      if (bY < aY || cY < aY) {
        if (bY < cY) {
          int tempX = aX;
          int tempY = aY;
          boolean tempBool = ab;
          aX = bX;
          aY = bY;
          ab = bc;
          bX = cX;
          bY = cY;
          bc = ca;
          cX = tempX;
          cY = tempY;
          ca = tempBool;
        } else if (cY < aY) {
          int tempX = aX;
          int tempY = aY;
          boolean tempBool = ab;
          aX = cX;
          aY = cY;
          ab = ca;
          cX = bX;
          cY = bY;
          ca = bc;
          bX = tempX;
          bY = tempY;
          bc = tempBool;
        }
      }
    }

    int minX = aX;
    int minY = StrictMath.min(aY, StrictMath.min(bY, cY));
    int maxX = StrictMath.max(aX, StrictMath.max(bX, cX));
    int maxY = StrictMath.max(aY, StrictMath.max(bY, cY));

    int bits, x, y;
    if (minY == aY) {
      if (maxY == bY && maxX == bX) {
        y = cY;
        x = cX;
        bits = MINY_MINX_MAXY_MAXX_Y_X;
      } else if (maxY == cY && maxX == cX) {
        y = bY;
        x = bX;
        bits = MINY_MINX_Y_X_MAXY_MAXX;
      } else {
        y = bY;
        x = cX;
        bits = MINY_MINX_Y_MAXX_MAXY_X;
      }
    } else if (maxY == aY) {
      if (minY == bY && maxX == bX) {
        y = cY;
        x = cX;
        bits = MAXY_MINX_MINY_MAXX_Y_X;
      } else if (minY == cY && maxX == cX) {
        y = bY;
        x = bX;
        bits = MAXY_MINX_Y_X_MINY_MAXX;
      } else {
        y = cY;
        x = bX;
        bits = MAXY_MINX_MINY_X_Y_MAXX;
      }
    }  else if (maxX == bX && minY == bY) {
      y = aY;
      x = cX;
      bits = Y_MINX_MINY_MAXX_MAXY_X;
    } else if (maxX == cX && maxY == cY) {
      y = aY;
      x = bX;
      bits = Y_MINX_MINY_X_MAXY_MAXX;
    } else {
      throw new IllegalArgumentException("Could not encode the provided triangle");
    }
    bits |= (ab) ? (1 << 3) : 0;
    bits |= (bc) ? (1 << 4) : 0;
    bits |= (ca) ? (1 << 5) : 0;
    // bits 6 & 7 means type is a triangle
    bits |= 1 << 6;
    bits |= 1 << 7;
    NumericUtils.intToSortableBytes(minY, bytes, 0);
    NumericUtils.intToSortableBytes(minX, bytes, BYTES);
    NumericUtils.intToSortableBytes(maxY, bytes, 2 * BYTES);
    NumericUtils.intToSortableBytes(maxX, bytes, 3 * BYTES);
    NumericUtils.intToSortableBytes(y, bytes, 4 * BYTES);
    NumericUtils.intToSortableBytes(x, bytes, 5 * BYTES);
    NumericUtils.intToSortableBytes(bits, bytes, 6 * BYTES);
  }

  /** Decode a triangle encoded by {@link ShapeField#encodeTriangle(byte[], int, int, boolean, int, int, boolean, int, int, boolean)}.
   */
  public static void decodeTriangle(byte[] t, DecodedTriangle triangle) {
    final int bits = NumericUtils.sortableBytesToInt(t, 6 * BYTES);
    final boolean bit6 = (bits & 1 << 6) == 1 << 6;
    final boolean bit7 = (bits & 1 << 7) == 1 << 7;
    if (bit6) {
      if (bit7) {
        triangle.type = DecodedTriangle.TYPE.TRIANGLE;
        decodeTriangle(t, triangle, bits);
      } else {
        triangle.type = DecodedTriangle.TYPE.POINT;
        decodePoint(t,  triangle);
      }
    } else if (bit7) {
      triangle.type = DecodedTriangle.TYPE.LINE;
      decodeLine(t, triangle, bits);
    } else {
      // for backwards compatibility
      triangle.type = DecodedTriangle.TYPE.UNKNOWN;
      decodeTriangle(t, triangle, bits);
    }
  }

  private static void decodePoint(final byte[] t, final DecodedTriangle triangle) {
    triangle.aY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
    triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
    triangle.bY = triangle.aY;
    triangle.bX = triangle.aX;
    triangle.cY = triangle.aY;
    triangle.cX = triangle.aX;
    triangle.minX = triangle.aX;
    triangle.maxX = triangle.aX;
    triangle.minY = triangle.aY;
    triangle.maxY = triangle.aY;
    triangle.ab = true;
    triangle.bc = true;
    triangle.ca = true;
  }

  private static void decodeLine(final byte[] t, final DecodedTriangle triangle, final int bits) {
    int tCode = (((1 << 3) - 1) & (bits >> 0));
    switch (tCode) {
      case MINY_MINX_MAXY_MAXX_Y_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = triangle.aY;
        triangle.cX = triangle.aX;
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.aY;
        triangle.maxY = triangle.bY;
        break;
      case MAXY_MINX_MINY_MAXX_Y_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = triangle.aY;
        triangle.cX = triangle.aX;
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.bY;
        triangle.maxY = triangle.aY;
        break;
      default:
        throw new IllegalArgumentException("Could not decode the provided line");
    }
    assert triangle.minX <= triangle.aX && triangle.minX <= triangle.bX && triangle.minX <= triangle.cX;
    assert triangle.minY <= triangle.aY && triangle.minY <= triangle.bY && triangle.minY <= triangle.cY;
    assert triangle.maxX >= triangle.aX && triangle.maxX >= triangle.bX && triangle.maxX >= triangle.cX;
    assert triangle.maxY >= triangle.aY && triangle.maxY >= triangle.bY && triangle.maxY >= triangle.cY;
    triangle.ab = (bits & 1 << 3) == 1 << 3;
    triangle.bc = (bits & 1 << 4) == 1 << 4;
    triangle.ca = (bits & 1 << 5) == 1 << 5;
  }

  private static void decodeTriangle(final byte[] t, final DecodedTriangle triangle, final int bits) {
    // extract the first three bits
    int tCode = (((1 << 3) - 1) & (bits >> 0));
    switch (tCode) {
      case MINY_MINX_MAXY_MAXX_Y_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.aY;
        triangle.maxY = triangle.bY;
        break;
      case MINY_MINX_Y_X_MAXY_MAXX:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.cX;
        triangle.minY = triangle.aY;
        triangle.maxY = triangle.cY;
        break;
      case MAXY_MINX_Y_X_MINY_MAXX:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.cX;
        triangle.minY = triangle.cY;
        triangle.maxY = triangle.aY;
        break;
      case MAXY_MINX_MINY_MAXX_Y_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.bY;
        triangle.maxY = triangle.aY;
        break;
      case Y_MINX_MINY_X_MAXY_MAXX:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.cX;
        triangle.minY = triangle.bY;
        triangle.maxY = triangle.cY;
        break;
      case Y_MINX_MINY_MAXX_MAXY_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.bY;
        triangle.maxY = triangle.cY;
        break;
      case MAXY_MINX_MINY_X_Y_MAXX:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.cX;
        triangle.minY = triangle.bY;
        triangle.maxY = triangle.aY;
        break;
      case MINY_MINX_Y_MAXX_MAXY_X:
        triangle.aY = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle.aX = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle.bY = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle.bX = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle.cY = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle.cX = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle.minX = triangle.aX;
        triangle.maxX = triangle.bX;
        triangle.minY = triangle.aY;
        triangle.maxY = triangle.cY;
        break;
      default:
        throw new IllegalArgumentException("Could not decode the provided triangle");
    }
    // Points of the decoded triangle must be co-planar or CCW oriented
    assert GeoUtils.orient(triangle.aX, triangle.aY, triangle.bX, triangle.bY, triangle.cX, triangle.cY) >= 0;
    assert triangle.minX <= triangle.aX && triangle.minX <= triangle.bX && triangle.minX <= triangle.cX;
    assert triangle.minY <= triangle.aY && triangle.minY <= triangle.bY && triangle.minY <= triangle.cY;
    assert triangle.maxX >= triangle.aX && triangle.maxX >= triangle.bX && triangle.maxX >= triangle.cX;
    assert triangle.maxY >= triangle.aY && triangle.maxY >= triangle.bY && triangle.maxY >= triangle.cY;
    triangle.ab = (bits & 1 << 3) == 1 << 3;
    triangle.bc = (bits & 1 << 4) == 1 << 4;
    triangle.ca = (bits & 1 << 5) == 1 << 5;
    if (triangle.type == DecodedTriangle.TYPE.UNKNOWN) {
      // Encoding does not contain type information
      resolveTriangleType(triangle);
    }
  }

  private static void resolveTriangleType(DecodedTriangle triangle) {
    if (triangle.aX == triangle.bX && triangle.aY == triangle.bY) {
      if (triangle.aX == triangle.cX && triangle.aY == triangle.cY) {
        triangle.type = DecodedTriangle.TYPE.POINT;
      } else {
        triangle.aX = triangle.cX;
        triangle.aY = triangle.cY;
        triangle.type = DecodedTriangle.TYPE.LINE;
      }
    } else if (triangle.aX == triangle.cX && triangle.aY == triangle.cY) {
      triangle.type = DecodedTriangle.TYPE.LINE;
    } else if (triangle.bX == triangle.cX && triangle.bY == triangle.cY) {
      triangle.cX = triangle.aX;
      triangle.cY = triangle.aY;
      triangle.type = DecodedTriangle.TYPE.LINE;
    } else {
      triangle.type = DecodedTriangle.TYPE.TRIANGLE;
    }
  }

  /**
   * Represents a encoded triangle using {@link ShapeField#decodeTriangle(byte[], DecodedTriangle)}.
   */
  public static class DecodedTriangle {
    /**
     * type of triangle:
     * <p><ul>
     * <li>POINT: all coordinates are equal
     * <li>LINE: first and third coordinates are equal
     * <li>TRIANGLE: all coordinates are different
     * <li>UNKNOWN: Stored triangle does not contain type information. The type is resolved during decoding.
     * </ul><p>
     */
    public enum TYPE {
      POINT, LINE, TRIANGLE, UNKNOWN
    }

    // Triangle vertices
    public int aX, aY, bX, bY, cX, cY;
    // Triangle bounding box
    public int minX, maxX, minY, maxY;
    // Represent if edges belongs to original shape
    public boolean ab, bc, ca;
    // Triangle type
    public TYPE type;

    public DecodedTriangle() {
    }

    @Override
    public int hashCode() {
      return Objects.hash(aX, aY, bX, bY, cX, cY, ab, bc, ca);
    }

    @Override
    public boolean equals(Object o) {
      DecodedTriangle other  = (DecodedTriangle) o;
      return aX == other.aX && bX == other.bX && cX == other.cX
          && aY == other.aY && bY == other.bY && cY == other.cY
          && ab == other.ab && bc == other.bc && ca == other.ca;
    }

    /** pretty print the triangle vertices */
    public String toString() {
      String result = aX + ", " + aY + " " +
          bX + ", " + bY + " " +
          cX + ", " + cY + " " + "[" + ab + "," +bc + "," + ca + "]";
      return result;
    }
  }
}
