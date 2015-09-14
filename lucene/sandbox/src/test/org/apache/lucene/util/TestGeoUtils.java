package org.apache.lucene.util;

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.BeforeClass;

/**
 * Tests class for methods in GeoUtils
 *
 * @lucene.experimental
 */
public class TestGeoUtils extends LuceneTestCase {

  // Global bounding box we will "cover" in the random test; we have to make this "smallish" else the queries take very long:
  private static double originLat;
  private static double originLon;
  //  private static double range;
  private static double lonRange;
  private static double latRange;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Between 1.0 and 3.0:
    lonRange = 2 * (random().nextDouble() + 0.5);
    latRange = 2 * (random().nextDouble() + 0.5);

    originLon = GeoUtils.MIN_LON_INCL + lonRange + (GeoUtils.MAX_LON_INCL - GeoUtils.MIN_LON_INCL - 2 * lonRange) * random().nextDouble();
    originLon = GeoUtils.normalizeLon(originLon);
    originLat = GeoUtils.MIN_LAT_INCL + latRange + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2 * latRange) * random().nextDouble();
    originLat = GeoUtils.normalizeLat(originLat);

    if (VERBOSE) {
      System.out.println("TEST: originLon=" + originLon + " lonRange= " + lonRange + " originLat=" + originLat + " latRange=" + latRange);
    }
  }

  public void testGeoHash() {
    int numPoints = atLeast(100);
    String randomGeoHashString;
    String mortonGeoHash;
    long mortonLongFromGHLong, geoHashLong, mortonLongFromGHString;
    int randomLevel;
    for (int i = 0; i < numPoints; ++i) {
      // random point
      double lat = randomLatFullRange();
      double lon = randomLonFullRange();

      // compute geohash straight from lat/lon and from morton encoded value to ensure they're the same
      randomGeoHashString = GeoHashUtils.stringEncode(lon, lat, randomLevel = random().nextInt(12 - 1) + 1);
      mortonGeoHash = GeoHashUtils.stringEncodeFromMortonLong(GeoUtils.mortonHash(lon, lat), randomLevel);
      assertEquals(randomGeoHashString, mortonGeoHash);

      // v&v conversion from lat/lon or geohashstring to geohash long and back to geohash string
      geoHashLong = (random().nextBoolean()) ? GeoHashUtils.longEncode(lon, lat, randomLevel) : GeoHashUtils.longEncode(randomGeoHashString);
      assertEquals(randomGeoHashString, GeoHashUtils.stringEncode(geoHashLong));

      // v&v conversion from geohash long to morton long
      mortonLongFromGHString = GeoHashUtils.mortonEncode(randomGeoHashString);
      mortonLongFromGHLong = GeoHashUtils.mortonEncode(geoHashLong);
      assertEquals(mortonLongFromGHLong, mortonLongFromGHString);

      // v&v lat/lon from geohash string and geohash long
      assertEquals(GeoUtils.mortonUnhashLat(mortonLongFromGHString), GeoUtils.mortonUnhashLat(mortonLongFromGHLong), 0);
      assertEquals(GeoUtils.mortonUnhashLon(mortonLongFromGHString), GeoUtils.mortonUnhashLon(mortonLongFromGHLong), 0);
    }
  }

  /**
   * Pass condition: lat=42.6, lng=-5.6 should be encoded as "ezs42e44yx96",
   * lat=57.64911 lng=10.40744 should be encoded as "u4pruydqqvj8"
   */
  public void testEncode() {
    String hash = GeoHashUtils.stringEncode(-5.6, 42.6, 12);
    assertEquals("ezs42e44yx96", hash);

    hash = GeoHashUtils.stringEncode(10.40744, 57.64911, 12);
    assertEquals("u4pruydqqvj8", hash);
  }

  /**
   * Pass condition: lat=52.3738007, lng=4.8909347 should be encoded and then
   * decoded within 0.00001 of the original value
   */
  public void testDecodePreciseLongitudeLatitude() {
    final String geohash = GeoHashUtils.stringEncode(4.8909347, 52.3738007);
    final long hash = GeoHashUtils.mortonEncode(geohash);

    assertEquals(52.3738007, GeoUtils.mortonUnhashLat(hash), 0.00001D);
    assertEquals(4.8909347, GeoUtils.mortonUnhashLon(hash), 0.00001D);
  }

  /**
   * Pass condition: lat=84.6, lng=10.5 should be encoded and then decoded
   * within 0.00001 of the original value
   */
  public void testDecodeImpreciseLongitudeLatitude() {
    final String geohash = GeoHashUtils.stringEncode(10.5, 84.6);

    final long hash = GeoHashUtils.mortonEncode(geohash);

    assertEquals(84.6, GeoUtils.mortonUnhashLat(hash), 0.00001D);
    assertEquals(10.5, GeoUtils.mortonUnhashLon(hash), 0.00001D);
  }

  public void testDecodeEncode() {
    final String geoHash = "u173zq37x014";
    assertEquals(geoHash, GeoHashUtils.stringEncode(4.8909347, 52.3738007));
    final long mortonHash = GeoHashUtils.mortonEncode(geoHash);
    final double lon = GeoUtils.mortonUnhashLon(mortonHash);
    final double lat = GeoUtils.mortonUnhashLat(mortonHash);
    assertEquals(52.37380061d, GeoUtils.mortonUnhashLat(mortonHash), 0.000001d);
    assertEquals(4.8909343d, GeoUtils.mortonUnhashLon(mortonHash), 0.000001d);

    assertEquals(geoHash, GeoHashUtils.stringEncode(lon, lat));
  }

  public void testNeighbors() {
    String geohash = "gcpv";
    List<String> expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("gcpw");
    expectedNeighbors.add("gcpy");
    expectedNeighbors.add("u10n");
    expectedNeighbors.add("gcpt");
    expectedNeighbors.add("u10j");
    expectedNeighbors.add("gcps");
    expectedNeighbors.add("gcpu");
    expectedNeighbors.add("u10h");
    Collection<? super String> neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);

    // Border odd geohash
    geohash = "u09x";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("u0c2");
    expectedNeighbors.add("u0c8");
    expectedNeighbors.add("u0cb");
    expectedNeighbors.add("u09r");
    expectedNeighbors.add("u09z");
    expectedNeighbors.add("u09q");
    expectedNeighbors.add("u09w");
    expectedNeighbors.add("u09y");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors);
    assertEquals(expectedNeighbors, neighbors);

    // Border even geohash
    geohash = "u09tv";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("u09wh");
    expectedNeighbors.add("u09wj");
    expectedNeighbors.add("u09wn");
    expectedNeighbors.add("u09tu");
    expectedNeighbors.add("u09ty");
    expectedNeighbors.add("u09ts");
    expectedNeighbors.add("u09tt");
    expectedNeighbors.add("u09tw");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);

    // Border even and odd geohash
    geohash = "ezzzz";
    expectedNeighbors = new ArrayList<>();
    expectedNeighbors.add("gbpbn");
    expectedNeighbors.add("gbpbp");
    expectedNeighbors.add("u0000");
    expectedNeighbors.add("ezzzy");
    expectedNeighbors.add("spbpb");
    expectedNeighbors.add("ezzzw");
    expectedNeighbors.add("ezzzx");
    expectedNeighbors.add("spbp8");
    neighbors = new ArrayList<>();
    GeoHashUtils.addNeighbors(geohash, neighbors );
    assertEquals(expectedNeighbors, neighbors);
  }

  public void testClosestPointOnBBox() {
    double[] result = new double[2];
    GeoDistanceUtils.closestPointOnBBox(20, 30, 40, 50, 70, 70, result);
    assertEquals(40.0, result[0], 0.0);
    assertEquals(50.0, result[1], 0.0);

    GeoDistanceUtils.closestPointOnBBox(-20, -20, 0, 0, 70, 70, result);
    // nocommit this fails but should pass?
    assertEquals(0.0, result[0], 0.0);
    assertEquals(0.0, result[1], 0.0);
  }

  public static double randomLatFullRange() {
    return (180d * random().nextDouble()) - 90d;
  }
  public static double randomLonFullRange() {
    return (360d * random().nextDouble()) - 180d;
  }

  public static double randomLat() {
    return GeoUtils.normalizeLat(originLat + latRange * (random().nextDouble() - 0.5));
  }

  public static double randomLon() {
    return GeoUtils.normalizeLon(originLon + lonRange * (random().nextDouble() - 0.5));
  }

  // nocommit test other APIs, e.g. bbox around a circle
  public void testRandomRectsAndCircles() throws Exception {
    int iters = atLeast(1000);
    int iter = 0;

    while (iter < iters) {

      // Random rect:
      double minLat = randomLat();
      double maxLat = randomLat();
      double minLon = randomLon();
      double maxLon = randomLon();

      if (maxLat < minLat) {
        double x = minLat;
        minLat = maxLat;
        maxLat = x;
      }

      if (maxLon < minLon) {
        // nocommit but what about testing crossing the dateline?
        double x = minLon;
        minLon = maxLon;
        maxLon = x;
      }

      double centerLat = randomLat();
      double centerLon = randomLon();
      double radiusMeters = 10000000*random().nextDouble();

      boolean expected = false;
      boolean circleFullyInside = false;
      BBox bbox = computeBBox(centerLon, centerLat, radiusMeters);
      if (GeoUtils.rectCrossesCircle(minLon, minLat, maxLon, maxLat, centerLon, centerLat, radiusMeters) == false) {
        // Rect and circle are disjoint
        expected = false;
      } else if (GeoUtils.rectWithinCircle(minLon, minLat, maxLon, maxLat, centerLon, centerLat, radiusMeters)) {
        // Rect fully contained inside circle
        expected = true;
      } else if (GeoUtils.rectWithin(bbox.minLon, bbox.minLat, bbox.maxLon, bbox.maxLat, minLon, minLat, maxLon, maxLat)) {
        // circle fully contained inside rect
        circleFullyInside = true;
      } else {
        // TODO: would be nice to somehow test this case too
        continue;
      }

      if (VERBOSE) {
        System.out.println("\nTEST: iter=" + iter + " rect: lon=" + minLon + " TO " + maxLon + ", lat=" + minLat + " TO " + maxLat + "; circle: lon=" + centerLon + " lat=" + centerLat + " radiusMeters=" + radiusMeters);
        if (expected) {
          System.out.println("  circle fully contains rect");
        } else if (circleFullyInside) {
          System.out.println("  rect fully contains circle");
        } else {
          System.out.println("  circle and rect are disjoint");
        }
      }

      iter++;

      // Randomly pick points inside the rect and check distance to the center:
      int iters2 = atLeast(1000);
      for(int iter2=0;iter2<iters2;iter2++) {
        double lon = minLon + random().nextDouble() * (maxLon - minLon);
        double lat = minLat + random().nextDouble() * (maxLat - minLat);
        double distanceMeters = SloppyMath.haversin(centerLat, centerLon, lat, lon)*1000.0;
        boolean actual = distanceMeters < radiusMeters;
        if (circleFullyInside) {
          // nocommit why even test this?  expected will always be == actual when circleFullyInside
          expected = circleFullyInside && actual;
        }
        if (expected != actual) {
          System.out.println("  lon=" + lon + " lat=" + lat + " distanceMeters=" + distanceMeters);
          assertEquals(expected, actual);
        }
      }
    }
  }

  /**
   * Compute Bounding Box for a circle using WGS-84 parameters
   */
  // nocommit should this be in GeoUtils?
  static BBox computeBBox(final double centerLon, final double centerLat, final double radius) {
    final double radLat = StrictMath.toRadians(centerLat);
    final double radLon = StrictMath.toRadians(centerLon);
    double radDistance = (radius + 12000) / GeoProjectionUtils.SEMIMAJOR_AXIS;
    double minLat = radLat - radDistance;
    double maxLat = radLat + radDistance;
    double minLon;
    double maxLon;

    if (minLat > GeoProjectionUtils.MIN_LAT_RADIANS && maxLat < GeoProjectionUtils.MAX_LAT_RADIANS) {
      double deltaLon = StrictMath.asin(StrictMath.sin(radDistance) / StrictMath.cos(radLat));
      minLon = radLon - deltaLon;
      if (minLon < GeoProjectionUtils.MIN_LON_RADIANS) {
        minLon += 2d * StrictMath.PI;
      }
      maxLon = radLon + deltaLon;
      if (maxLon > GeoProjectionUtils.MAX_LON_RADIANS) {
        maxLon -= 2d * StrictMath.PI;
      }
    } else {
      // a pole is within the distance
      minLat = StrictMath.max(minLat, GeoProjectionUtils.MIN_LAT_RADIANS);
      maxLat = StrictMath.min(maxLat, GeoProjectionUtils.MAX_LAT_RADIANS);
      minLon = GeoProjectionUtils.MIN_LON_RADIANS;
      maxLon = GeoProjectionUtils.MAX_LON_RADIANS;
    }

    return new BBox(StrictMath.toDegrees(minLon), StrictMath.toDegrees(maxLon),
        StrictMath.toDegrees(minLat), StrictMath.toDegrees(maxLat));
  }

  static class BBox {
    final double minLon;
    final double minLat;
    final double maxLon;
    final double maxLat;

    BBox(final double minLon, final double maxLon, final double minLat, final double maxLat) {
      this.minLon = minLon;
      this.minLat = minLat;
      this.maxLon = maxLon;
      this.maxLat = maxLat;
    }
  }
}
