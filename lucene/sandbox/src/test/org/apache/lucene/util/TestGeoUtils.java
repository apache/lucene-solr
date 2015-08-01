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
