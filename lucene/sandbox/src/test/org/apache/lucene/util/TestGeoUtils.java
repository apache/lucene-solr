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
import org.junit.Test;

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

  @Test
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
  @Test
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
  @Test
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
  @Test
  public void testDecodeImpreciseLongitudeLatitude() {
    final String geohash = GeoHashUtils.stringEncode(10.5, 84.6);

    final long hash = GeoHashUtils.mortonEncode(geohash);

    assertEquals(84.6, GeoUtils.mortonUnhashLat(hash), 0.00001D);
    assertEquals(10.5, GeoUtils.mortonUnhashLon(hash), 0.00001D);
  }

  @Test
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

  @Test
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
}
