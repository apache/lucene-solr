/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.lucene.document;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.util.NumericUtils;

class XYTriangle {
  private static final int MINY_MINX_MAXY_MAXX_Y_X = 0;
  private static final int MINY_MINX_Y_X_MAXY_MAXX = 1;
  private static final int MAXY_MINX_Y_X_MINY_MAXX = 2;
  private static final int MAXY_MINX_MINY_MAXX_Y_X = 3;
  private static final int Y_MINX_MINY_X_MAXY_MAXX = 4;
  private static final int Y_MINX_MINY_MAXX_MAXY_X = 5;
  private static final int MAXY_MINX_MINY_X_Y_MAXX = 6;
  private static final int MINY_MINX_Y_MAXX_MAXY_X = 7;

  static class IntegerEncoder {
    /**
     * A triangle is encoded using 6 points and an extra point with encoded information in three bits of how to reconstruct it.
     * Triangles are encoded with CCW orientation and might be rotated to limit the number of possible reconstructions to 2^3.
     * Reconstruction always happens from west to east.
     */
    public static void encode(byte[] bytes, int aLat, int aLon, int bLat, int bLon, int cLat, int cLon) {
      assert bytes.length == 7 * Integer.BYTES;
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
      } else if (maxX == bX && minY == bY) {
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
      NumericUtils.intToSortableBytes(minX, bytes, Integer.BYTES);
      NumericUtils.intToSortableBytes(maxY, bytes, 2 * Integer.BYTES);
      NumericUtils.intToSortableBytes(maxX, bytes, 3 * Integer.BYTES);
      NumericUtils.intToSortableBytes(y, bytes, 4 * Integer.BYTES);
      NumericUtils.intToSortableBytes(x, bytes, 5 * Integer.BYTES);
      NumericUtils.intToSortableBytes(bits, bytes, 6 * Integer.BYTES);
    }

    /**
     * Decode a triangle encoded by {@link LatLonShape#encodeTriangle(byte[], int, int, int, int, int, int)}.
     */
    public static void decode(byte[] t, int[] triangle) {
      assert triangle.length == 6;
      int bits = NumericUtils.sortableBytesToInt(t, 6 * Integer.BYTES);
      //extract the first three bits
      int tCode = (((1 << 3) - 1) & (bits >> 0));
      switch (tCode) {
        case MINY_MINX_MAXY_MAXX_Y_X:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          break;
        case MINY_MINX_Y_X_MAXY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          break;
        case MAXY_MINX_Y_X_MINY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          break;
        case MAXY_MINX_MINY_MAXX_Y_X:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          break;
        case Y_MINX_MINY_X_MAXY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          break;
        case Y_MINX_MINY_MAXX_MAXY_X:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          break;
        case MAXY_MINX_MINY_X_Y_MAXX:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          break;
        case MINY_MINX_Y_MAXX_MAXY_X:
          triangle[0] = NumericUtils.sortableBytesToInt(t, 0 * Integer.BYTES);
          triangle[1] = NumericUtils.sortableBytesToInt(t, 1 * Integer.BYTES);
          triangle[2] = NumericUtils.sortableBytesToInt(t, 4 * Integer.BYTES);
          triangle[3] = NumericUtils.sortableBytesToInt(t, 3 * Integer.BYTES);
          triangle[4] = NumericUtils.sortableBytesToInt(t, 2 * Integer.BYTES);
          triangle[5] = NumericUtils.sortableBytesToInt(t, 5 * Integer.BYTES);
          break;
        default:
          throw new IllegalArgumentException("Could not decode the provided triangle");
      }
      //Points of the decoded triangle must be co-planar or CCW oriented
      assert GeoUtils.orient(triangle[1], triangle[0], triangle[3], triangle[2], triangle[5], triangle[4]) >= 0;
    }
  }

  static class LongEncoder {
    /**
     * A triangle is encoded using 6 points and an extra point with encoded information in three bits of how to reconstruct it.
     * Triangles are encoded with CCW orientation and might be rotated to limit the number of possible reconstructions to 2^3.
     * Reconstruction always happens from west to east.
     */
    public static void encode(byte[] bytes, long aLat, long aLon, long bLat, long bLon, long cLat, long cLon) {
      assert bytes.length == 7 * Integer.BYTES;
      long aX;
      long bX;
      long cX;
      long aY;
      long bY;
      long cY;
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
          long tempX = aX;
          long tempY = aY;
          aX = bX;
          aY = bY;
          bX = cX;
          bY = cY;
          cX = tempX;
          cY = tempY;
        } else if (cX < aX) {
          long tempX = aX;
          long tempY = aY;
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
            long tempX = aX;
            long tempY = aY;
            aX = bX;
            aY = bY;
            bX = cX;
            bY = cY;
            cX = tempX;
            cY = tempY;
          } else if (cY < aY) {
            long tempX = aX;
            long tempY = aY;
            aX = cX;
            aY = cY;
            cX = bX;
            cY = bY;
            bX = tempX;
            bY = tempY;
          }
        }
      }

      long minX = aX;
      long minY = StrictMath.min(aY, StrictMath.min(bY, cY));
      long maxX = StrictMath.max(aX, StrictMath.max(bX, cX));
      long maxY = StrictMath.max(aY, StrictMath.max(bY, cY));

      long bits, x, y;
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
      } else if (maxX == bX && minY == bY) {
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
      NumericUtils.longToSortableBytes(minY, bytes, 0);
      NumericUtils.longToSortableBytes(minX, bytes, Long.BYTES);
      NumericUtils.longToSortableBytes(maxY, bytes, 2 * Long.BYTES);
      NumericUtils.longToSortableBytes(maxX, bytes, 3 * Long.BYTES);
      NumericUtils.longToSortableBytes(y, bytes, 4 * Long.BYTES);
      NumericUtils.longToSortableBytes(x, bytes, 5 * Long.BYTES);
      NumericUtils.longToSortableBytes(bits, bytes, 6 * Long.BYTES);
    }

    /**
     * Decode a triangle encoded by {@link LatLonShape#encodeTriangle(byte[], int, int, int, int, int, int)}.
     */
    public static void decode(byte[] t, long[] triangle) {
      assert triangle.length == 6;
      int bits = NumericUtils.sortableBytesToInt(t, 6 * Integer.BYTES);
      //extract the first three bits
      int tCode = (((1 << 3) - 1) & (bits >> 0));
      switch (tCode) {
        case MINY_MINX_MAXY_MAXX_Y_X:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          break;
        case MINY_MINX_Y_X_MAXY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          break;
        case MAXY_MINX_Y_X_MINY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          break;
        case MAXY_MINX_MINY_MAXX_Y_X:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          break;
        case Y_MINX_MINY_X_MAXY_MAXX:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          break;
        case Y_MINX_MINY_MAXX_MAXY_X:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          break;
        case MAXY_MINX_MINY_X_Y_MAXX:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          break;
        case MINY_MINX_Y_MAXX_MAXY_X:
          triangle[0] = NumericUtils.sortableBytesToLong(t, 0 * Long.BYTES);
          triangle[1] = NumericUtils.sortableBytesToLong(t, 1 * Long.BYTES);
          triangle[2] = NumericUtils.sortableBytesToLong(t, 4 * Long.BYTES);
          triangle[3] = NumericUtils.sortableBytesToLong(t, 3 * Long.BYTES);
          triangle[4] = NumericUtils.sortableBytesToLong(t, 2 * Long.BYTES);
          triangle[5] = NumericUtils.sortableBytesToLong(t, 5 * Long.BYTES);
          break;
        default:
          throw new IllegalArgumentException("Could not decode the provided triangle");
      }
      //Points of the decoded triangle must be co-planar or CCW oriented
      assert GeoUtils.orient(triangle[1], triangle[0], triangle[3], triangle[2], triangle[5], triangle[4]) >= 0;
    }
  }

}
