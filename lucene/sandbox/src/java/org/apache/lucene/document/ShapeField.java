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

    Triangle(String name, int aXencoded, int aYencoded, int bXencoded, int bYencoded, int cXencoded, int cYencoded) {
      super(name, TYPE);
      setTriangleValue(aXencoded, aYencoded, bXencoded, bYencoded, cXencoded, cYencoded);
    }

    Triangle(String name, Tessellator.Triangle t) {
      super(name, TYPE);
      setTriangleValue(t.getEncodedX(0), t.getEncodedY(0), t.getEncodedX(1), t.getEncodedY(1), t.getEncodedX(2), t.getEncodedY(2));
    }

    /** sets the vertices of the triangle as integer encoded values */
    protected void setTriangleValue(int aX, int aY, int bX, int bY, int cX, int cY) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef) fieldsData).bytes;
      }
      encodeTriangle(bytes, aY, aX, bY, bX, cY, cX);
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

  /**
   * A triangle is encoded using 6 points and an extra point with encoded information in three bits of how to reconstruct it.
   * Triangles are encoded with CCW orientation and might be rotated to limit the number of possible reconstructions to 2^3.
   * Reconstruction always happens from west to east.
   */
  public static void encodeTriangle(byte[] bytes, int aLat, int aLon, int bLat, int bLon, int cLat, int cLon) {
    assert bytes.length == 7 * BYTES;
    int aX;
    int bX;
    int cX;
    int aY;
    int bY;
    int cY;
    //change orientation if CW
    if (GeoUtils.orient(aLon, aLat, bLon, bLat, cLon, cLat) == -1) {
      aX = cLon;
      bX = bLon;
      cX = aLon;
      aY = cLat;
      bY = bLat;
      cY = aLat;
    } else {
      aX = aLon;
      bX = bLon;
      cX = cLon;
      aY = aLat;
      bY = bLat;
      cY = cLat;
    }
    //rotate edges and place minX at the beginning
    if (bX < aX || cX < aX) {
      if (bX < cX) {
        int tempX = aX;
        int tempY = aY;
        aX = bX;
        aY = bY;
        bX = cX;
        bY = cY;
        cX = tempX;
        cY = tempY;
      } else if (cX < aX) {
        int tempX = aX;
        int tempY = aY;
        aX = cX;
        aY = cY;
        cX = bX;
        cY = bY;
        bX = tempX;
        bY = tempY;
      }
    } else if (aX == bX && aX == cX) {
      //degenerated case, all points with same longitude
      //we need to prevent that aX is in the middle (not part of the MBS)
      if (bY < aY || cY < aY) {
        if (bY < cY) {
          int tempX = aX;
          int tempY = aY;
          aX = bX;
          aY = bY;
          bX = cX;
          bY = cY;
          cX = tempX;
          cY = tempY;
        } else if (cY < aY) {
          int tempX = aX;
          int tempY = aY;
          aX = cX;
          aY = cY;
          cX = bX;
          cY = bY;
          bX = tempX;
          bY = tempY;
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
    NumericUtils.intToSortableBytes(minY, bytes, 0);
    NumericUtils.intToSortableBytes(minX, bytes, BYTES);
    NumericUtils.intToSortableBytes(maxY, bytes, 2 * BYTES);
    NumericUtils.intToSortableBytes(maxX, bytes, 3 * BYTES);
    NumericUtils.intToSortableBytes(y, bytes, 4 * BYTES);
    NumericUtils.intToSortableBytes(x, bytes, 5 * BYTES);
    NumericUtils.intToSortableBytes(bits, bytes, 6 * BYTES);
  }

  /**
   * Decode a triangle encoded by {@link ShapeField#encodeTriangle(byte[], int, int, int, int, int, int)}.
   */
  public static void decodeTriangle(byte[] t, int[] triangle) {
    assert triangle.length == 6;
    int bits = NumericUtils.sortableBytesToInt(t, 6 * BYTES);
    //extract the first three bits
    int tCode = (((1 << 3) - 1) & (bits >> 0));
    switch (tCode) {
      case MINY_MINX_MAXY_MAXX_Y_X:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        break;
      case MINY_MINX_Y_X_MAXY_MAXX:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        break;
      case MAXY_MINX_Y_X_MINY_MAXX:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        break;
      case MAXY_MINX_MINY_MAXX_Y_X:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        break;
      case Y_MINX_MINY_X_MAXY_MAXX:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        break;
      case Y_MINX_MINY_MAXX_MAXY_X:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        break;
      case MAXY_MINX_MINY_X_Y_MAXX:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        break;
      case MINY_MINX_Y_MAXX_MAXY_X:
        triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * BYTES);
        triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * BYTES);
        triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * BYTES);
        triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * BYTES);
        triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * BYTES);
        triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * BYTES);
        break;
      default:
        throw new IllegalArgumentException("Could not decode the provided triangle");
    }
    //Points of the decoded triangle must be co-planar or CCW oriented
    assert GeoUtils.orient(triangle[1], triangle[0], triangle[3], triangle[2], triangle[5], triangle[4]) >= 0;
  }
}
