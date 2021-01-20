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

import static org.apache.lucene.geo.GeoUtils.MAX_LAT_INCL;
import static org.apache.lucene.geo.GeoUtils.MAX_LON_INCL;
import static org.apache.lucene.geo.GeoUtils.MIN_LAT_INCL;
import static org.apache.lucene.geo.GeoUtils.MIN_LON_INCL;
import static org.apache.lucene.geo.GeoUtils.checkLatitude;
import static org.apache.lucene.geo.GeoUtils.checkLongitude;

import java.util.function.Function;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.SloppyMath;

/**
 * reusable geopoint encoding methods
 *
 * @lucene.experimental
 */
public final class GeoEncodingUtils {
  /** number of bits used for quantizing latitude and longitude values */
  public static final short BITS = 32;

  private static final double LAT_SCALE = (0x1L << BITS) / 180.0D;
  private static final double LAT_DECODE = 1 / LAT_SCALE;
  private static final double LON_SCALE = (0x1L << BITS) / 360.0D;
  private static final double LON_DECODE = 1 / LON_SCALE;

  public static final int MIN_LON_ENCODED = encodeLongitude(MIN_LON_INCL);
  public static final int MAX_LON_ENCODED = encodeLongitude(MAX_LON_INCL);

  // No instance:
  private GeoEncodingUtils() {}

  /**
   * Quantizes double (64 bit) latitude into 32 bits (rounding down: in the direction of -90)
   *
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if latitude is out of bounds
   */
  public static int encodeLatitude(double latitude) {
    checkLatitude(latitude);
    // the maximum possible value cannot be encoded without overflow
    if (latitude == 90.0D) {
      latitude = Math.nextDown(latitude);
    }
    return (int) Math.floor(latitude / LAT_DECODE);
  }

  /**
   * Quantizes double (64 bit) latitude into 32 bits (rounding up: in the direction of +90)
   *
   * @param latitude latitude value: must be within standard +/-90 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if latitude is out of bounds
   */
  public static int encodeLatitudeCeil(double latitude) {
    GeoUtils.checkLatitude(latitude);
    // the maximum possible value cannot be encoded without overflow
    if (latitude == 90.0D) {
      latitude = Math.nextDown(latitude);
    }
    return (int) Math.ceil(latitude / LAT_DECODE);
  }

  /**
   * Quantizes double (64 bit) longitude into 32 bits (rounding down: in the direction of -180)
   *
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if longitude is out of bounds
   */
  public static int encodeLongitude(double longitude) {
    checkLongitude(longitude);
    // the maximum possible value cannot be encoded without overflow
    if (longitude == 180.0D) {
      longitude = Math.nextDown(longitude);
    }
    return (int) Math.floor(longitude / LON_DECODE);
  }

  /**
   * Quantizes double (64 bit) longitude into 32 bits (rounding up: in the direction of +180)
   *
   * @param longitude longitude value: must be within standard +/-180 coordinate bounds.
   * @return encoded value as a 32-bit {@code int}
   * @throws IllegalArgumentException if longitude is out of bounds
   */
  public static int encodeLongitudeCeil(double longitude) {
    GeoUtils.checkLongitude(longitude);
    // the maximum possible value cannot be encoded without overflow
    if (longitude == 180.0D) {
      longitude = Math.nextDown(longitude);
    }
    return (int) Math.ceil(longitude / LON_DECODE);
  }

  /**
   * Turns quantized value from {@link #encodeLatitude} back into a double.
   *
   * @param encoded encoded value: 32-bit quantized value.
   * @return decoded latitude value.
   */
  public static double decodeLatitude(int encoded) {
    double result = encoded * LAT_DECODE;
    assert result >= MIN_LAT_INCL && result < MAX_LAT_INCL;
    return result;
  }

  /**
   * Turns quantized value from byte array back into a double.
   *
   * @param src byte array containing 4 bytes to decode at {@code offset}
   * @param offset offset into {@code src} to decode from.
   * @return decoded latitude value.
   */
  public static double decodeLatitude(byte[] src, int offset) {
    return decodeLatitude(NumericUtils.sortableBytesToInt(src, offset));
  }

  /**
   * Turns quantized value from {@link #encodeLongitude} back into a double.
   *
   * @param encoded encoded value: 32-bit quantized value.
   * @return decoded longitude value.
   */
  public static double decodeLongitude(int encoded) {
    double result = encoded * LON_DECODE;
    assert result >= MIN_LON_INCL && result < MAX_LON_INCL;
    return result;
  }

  /**
   * Turns quantized value from byte array back into a double.
   *
   * @param src byte array containing 4 bytes to decode at {@code offset}
   * @param offset offset into {@code src} to decode from.
   * @return decoded longitude value.
   */
  public static double decodeLongitude(byte[] src, int offset) {
    return decodeLongitude(NumericUtils.sortableBytesToInt(src, offset));
  }

  /**
   * Create a predicate that checks whether points are within a distance of a given point. It works
   * by computing the bounding box around the circle that is defined by the given points/distance
   * and splitting it into between 1024 and 4096 smaller boxes (4096*0.75^2=2304 on average). Then
   * for each sub box, it computes the relation between this box and the distance query. Finally at
   * search time, it first computes the sub box that the point belongs to, most of the time, no
   * distance computation will need to be performed since all points from the sub box will either be
   * in or out of the circle.
   *
   * @lucene.internal
   */
  public static DistancePredicate createDistancePredicate(
      double lat, double lon, double radiusMeters) {
    final Rectangle boundingBox = Rectangle.fromPointDistance(lat, lon, radiusMeters);
    final double axisLat = Rectangle.axisLat(lat, radiusMeters);
    final double distanceSortKey = GeoUtils.distanceQuerySortKey(radiusMeters);
    final Function<Rectangle, Relation> boxToRelation =
        box ->
            GeoUtils.relate(
                box.minLat, box.maxLat, box.minLon, box.maxLon, lat, lon, distanceSortKey, axisLat);
    final Grid subBoxes =
        createSubBoxes(
            boundingBox.minLat,
            boundingBox.maxLat,
            boundingBox.minLon,
            boundingBox.maxLon,
            boxToRelation);

    return new DistancePredicate(
        subBoxes.latShift,
        subBoxes.lonShift,
        subBoxes.latBase,
        subBoxes.lonBase,
        subBoxes.maxLatDelta,
        subBoxes.maxLonDelta,
        subBoxes.relations,
        lat,
        lon,
        distanceSortKey);
  }

  /**
   * Create a predicate that checks whether points are within a geometry. It works the same way as
   * {@link #createDistancePredicate}.
   *
   * @lucene.internal
   */
  public static Component2DPredicate createComponentPredicate(Component2D tree) {
    final Function<Rectangle, Relation> boxToRelation =
        box -> tree.relate(box.minLon, box.maxLon, box.minLat, box.maxLat);
    final Grid subBoxes =
        createSubBoxes(
            tree.getMinY(), tree.getMaxY(), tree.getMinX(), tree.getMaxX(), boxToRelation);

    return new Component2DPredicate(
        subBoxes.latShift,
        subBoxes.lonShift,
        subBoxes.latBase,
        subBoxes.lonBase,
        subBoxes.maxLatDelta,
        subBoxes.maxLonDelta,
        subBoxes.relations,
        tree);
  }

  private static Grid createSubBoxes(
      double shapeMinLat,
      double shapeMaxLat,
      double shapeMinLon,
      double shapeMaxLon,
      Function<Rectangle, Relation> boxToRelation) {
    final int minLat = encodeLatitudeCeil(shapeMinLat);
    final int maxLat = encodeLatitude(shapeMaxLat);
    final int minLon = encodeLongitudeCeil(shapeMinLon);
    final int maxLon = encodeLongitude(shapeMaxLon);

    if (maxLat < minLat || (shapeMaxLon >= shapeMinLon && maxLon < minLon)) {
      // the box cannot match any quantized point
      return new Grid(1, 1, 0, 0, 0, 0, new byte[0]);
    }

    final int latShift, lonShift;
    final int latBase, lonBase;
    final int maxLatDelta, maxLonDelta;
    {
      long minLat2 = (long) minLat - Integer.MIN_VALUE;
      long maxLat2 = (long) maxLat - Integer.MIN_VALUE;
      latShift = computeShift(minLat2, maxLat2);
      latBase = (int) (minLat2 >>> latShift);
      maxLatDelta = (int) (maxLat2 >>> latShift) - latBase + 1;
      assert maxLatDelta > 0;
    }
    {
      long minLon2 = (long) minLon - Integer.MIN_VALUE;
      long maxLon2 = (long) maxLon - Integer.MIN_VALUE;
      if (shapeMaxLon < shapeMinLon) { // crosses dateline
        maxLon2 += 1L << 32; // wrap
      }
      lonShift = computeShift(minLon2, maxLon2);
      lonBase = (int) (minLon2 >>> lonShift);
      maxLonDelta = (int) (maxLon2 >>> lonShift) - lonBase + 1;
      assert maxLonDelta > 0;
    }

    final byte[] relations = new byte[maxLatDelta * maxLonDelta];
    for (int i = 0; i < maxLatDelta; ++i) {
      for (int j = 0; j < maxLonDelta; ++j) {
        final int boxMinLat = ((latBase + i) << latShift) + Integer.MIN_VALUE;
        final int boxMinLon = ((lonBase + j) << lonShift) + Integer.MIN_VALUE;
        final int boxMaxLat = boxMinLat + (1 << latShift) - 1;
        final int boxMaxLon = boxMinLon + (1 << lonShift) - 1;

        relations[i * maxLonDelta + j] =
            (byte)
                boxToRelation
                    .apply(
                        new Rectangle(
                            decodeLatitude(boxMinLat), decodeLatitude(boxMaxLat),
                            decodeLongitude(boxMinLon), decodeLongitude(boxMaxLon)))
                    .ordinal();
      }
    }

    return new Grid(latShift, lonShift, latBase, lonBase, maxLatDelta, maxLonDelta, relations);
  }

  /**
   * Compute the minimum shift value so that {@code (b>>>shift)-(a>>>shift)} is less that {@code
   * ARITY}.
   */
  private static int computeShift(long a, long b) {
    assert a <= b;
    // We enforce a shift of at least 1 so that when we work with unsigned ints
    // by doing (lat - MIN_VALUE), the result of the shift (lat - MIN_VALUE) >>> shift
    // can be used for comparisons without particular care: the sign bit has
    // been cleared so comparisons work the same for signed and unsigned ints
    for (int shift = 1; ; ++shift) {
      final long delta = (b >>> shift) - (a >>> shift);
      if (delta >= 0 && delta < Grid.ARITY) {
        return shift;
      }
    }
  }

  private static class Grid {
    static final int ARITY = 64;

    final int latShift, lonShift;
    final int latBase, lonBase;
    final int maxLatDelta, maxLonDelta;
    final byte[] relations;

    private Grid(
        int latShift,
        int lonShift,
        int latBase,
        int lonBase,
        int maxLatDelta,
        int maxLonDelta,
        byte[] relations) {
      if (latShift < 1 || latShift > 31) {
        throw new IllegalArgumentException();
      }
      if (lonShift < 1 || lonShift > 31) {
        throw new IllegalArgumentException();
      }
      this.latShift = latShift;
      this.lonShift = lonShift;
      this.latBase = latBase;
      this.lonBase = lonBase;
      this.maxLatDelta = maxLatDelta;
      this.maxLonDelta = maxLonDelta;
      this.relations = relations;
    }
  }

  /** A predicate that checks whether a given point is within a distance of another point. */
  public static class DistancePredicate extends Grid {

    private final double lat, lon;
    private final double distanceKey;

    private DistancePredicate(
        int latShift,
        int lonShift,
        int latBase,
        int lonBase,
        int maxLatDelta,
        int maxLonDelta,
        byte[] relations,
        double lat,
        double lon,
        double distanceKey) {
      super(latShift, lonShift, latBase, lonBase, maxLatDelta, maxLonDelta, relations);
      this.lat = lat;
      this.lon = lon;
      this.distanceKey = distanceKey;
    }

    /**
     * Check whether the given point is within a distance of another point. NOTE: this operates
     * directly on the encoded representation of points.
     */
    public boolean test(int lat, int lon) {
      final int lat2 = ((lat - Integer.MIN_VALUE) >>> latShift);
      if (lat2 < latBase || lat2 >= latBase + maxLatDelta) {
        return false;
      }
      int lon2 = ((lon - Integer.MIN_VALUE) >>> lonShift);
      if (lon2 < lonBase) { // wrap
        lon2 += 1 << (32 - lonShift);
      }
      assert Integer.toUnsignedLong(lon2) >= lonBase;
      assert lon2 - lonBase >= 0;
      if (lon2 - lonBase >= maxLonDelta) {
        return false;
      }

      final int relation = relations[(lat2 - latBase) * maxLonDelta + (lon2 - lonBase)];
      if (relation == Relation.CELL_CROSSES_QUERY.ordinal()) {
        return SloppyMath.haversinSortKey(
                decodeLatitude(lat), decodeLongitude(lon), this.lat, this.lon)
            <= distanceKey;
      } else {
        return relation == Relation.CELL_INSIDE_QUERY.ordinal();
      }
    }
  }

  /** A predicate that checks whether a given point is within a component2D geometry. */
  public static class Component2DPredicate extends Grid {

    private final Component2D tree;

    private Component2DPredicate(
        int latShift,
        int lonShift,
        int latBase,
        int lonBase,
        int maxLatDelta,
        int maxLonDelta,
        byte[] relations,
        Component2D tree) {
      super(latShift, lonShift, latBase, lonBase, maxLatDelta, maxLonDelta, relations);
      this.tree = tree;
    }

    /**
     * Check whether the given point is within the considered polygon. NOTE: this operates directly
     * on the encoded representation of points.
     */
    public boolean test(int lat, int lon) {
      final int lat2 = ((lat - Integer.MIN_VALUE) >>> latShift);
      if (lat2 < latBase || lat2 >= latBase + maxLatDelta) {
        return false;
      }
      int lon2 = ((lon - Integer.MIN_VALUE) >>> lonShift);
      if (lon2 < lonBase) { // wrap
        lon2 += 1 << (32 - lonShift);
      }
      assert Integer.toUnsignedLong(lon2) >= lonBase;
      assert lon2 - lonBase >= 0;
      if (lon2 - lonBase >= maxLonDelta) {
        return false;
      }

      final int relation = relations[(lat2 - latBase) * maxLonDelta + (lon2 - lonBase)];
      if (relation == Relation.CELL_CROSSES_QUERY.ordinal()) {
        return tree.contains(decodeLongitude(lon), decodeLatitude(lat));
      } else {
        return relation == Relation.CELL_INSIDE_QUERY.ordinal();
      }
    }
  }
}
