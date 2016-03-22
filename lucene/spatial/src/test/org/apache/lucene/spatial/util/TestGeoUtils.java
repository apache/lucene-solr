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
package org.apache.lucene.spatial.util;

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.SloppyMath;
import org.junit.BeforeClass;

/**
 * Tests class for methods in GeoUtils
 *
 * @lucene.experimental
 */
public class TestGeoUtils extends LuceneTestCase {

  private static final double LON_SCALE = (0x1L<<GeoEncodingUtils.BITS)/360.0D;
  private static final double LAT_SCALE = (0x1L<<GeoEncodingUtils.BITS)/180.0D;

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
    originLon = BaseGeoPointTestCase.normalizeLon(originLon);
    originLat = GeoUtils.MIN_LAT_INCL + latRange + (GeoUtils.MAX_LAT_INCL - GeoUtils.MIN_LAT_INCL - 2 * latRange) * random().nextDouble();
    originLat = BaseGeoPointTestCase.normalizeLat(originLat);

    if (VERBOSE) {
      System.out.println("TEST: originLon=" + originLon + " lonRange= " + lonRange + " originLat=" + originLat + " latRange=" + latRange);
    }
  }

  public long scaleLon(final double val) {
    return (long) ((val-GeoUtils.MIN_LON_INCL) * LON_SCALE);
  }

  public long scaleLat(final double val) {
    return (long) ((val-GeoUtils.MIN_LAT_INCL) * LAT_SCALE);
  }

  public double unscaleLon(final long val) {
    return (val / LON_SCALE) + GeoUtils.MIN_LON_INCL;
  }

  public double unscaleLat(final long val) {
    return (val / LAT_SCALE) + GeoUtils.MIN_LAT_INCL;
  }

  public double randomLat(boolean small) {
    double result;
    if (small) {
      result = BaseGeoPointTestCase.normalizeLat(originLat + latRange * (random().nextDouble() - 0.5));
    } else {
      result = -90 + 180.0 * random().nextDouble();
    }
    return result;
  }

  public double randomLon(boolean small) {
    double result;
    if (small) {
      result = BaseGeoPointTestCase.normalizeLon(originLon + lonRange * (random().nextDouble() - 0.5));
    } else {
      result = -180 + 360.0 * random().nextDouble();
    }
    return result;
  }

  /**
   * Tests stability of {@link GeoEncodingUtils#geoCodedToPrefixCoded}
   */
  public void testGeoPrefixCoding() throws Exception {
    int numIters = atLeast(1000);
    long hash;
    long decodedHash;
    BytesRefBuilder brb = new BytesRefBuilder();
    while (numIters-- >= 0) {
      hash = GeoEncodingUtils.mortonHash(randomLat(false), randomLon(false));
      for (int i=32; i<64; ++i) {
        GeoEncodingUtils.geoCodedToPrefixCoded(hash, i, brb);
        decodedHash = GeoEncodingUtils.prefixCodedToGeoCoded(brb.get());
        assertEquals((hash >>> i) << i, decodedHash);
      }
    }
  }

  public void testMortonEncoding() throws Exception {
    long hash = GeoEncodingUtils.mortonHash(90, 180);
    assertEquals(180.0, GeoEncodingUtils.mortonUnhashLon(hash), 0);
    assertEquals(90.0, GeoEncodingUtils.mortonUnhashLat(hash), 0);
  }

  public void testEncodeDecode() throws Exception {
    int iters = atLeast(10000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      assertEquals("lat=" + lat + " latEnc=" + latEnc + " diff=" + (lat - latEnc), lat, latEnc, GeoEncodingUtils.TOLERANCE);
      assertEquals("lon=" + lon + " lonEnc=" + lonEnc + " diff=" + (lon - lonEnc), lon, lonEnc, GeoEncodingUtils.TOLERANCE);
    }
  }

  public void testScaleUnscaleIsStable() throws Exception {
    int iters = atLeast(1000);
    boolean small = random().nextBoolean();
    for(int iter=0;iter<iters;iter++) {
      double lat = randomLat(small);
      double lon = randomLon(small);

      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);

      long enc2 = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc2 = GeoEncodingUtils.mortonUnhashLat(enc2);
      double lonEnc2 = GeoEncodingUtils.mortonUnhashLon(enc2);
      assertEquals(latEnc, latEnc2, 0.0);
      assertEquals(lonEnc, lonEnc2, 0.0);
    }
  }

  /** Returns random double min to max or up to 1% outside of that range */
  private double randomRangeMaybeSlightlyOutside(double min, double max) {
    return min + (random().nextDouble() + (0.5 - random().nextDouble()) * .02) * (max - min);
  }

  // We rely heavily on GeoUtils.circleToBBox so we test it here:
  public void testRandomCircleToBBox() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {

      boolean useSmallRanges = random().nextBoolean();

      double radiusMeters;

      double centerLat = randomLat(useSmallRanges);
      double centerLon = randomLon(useSmallRanges);

      if (useSmallRanges) {
        // Approx 4 degrees lon at the equator:
        radiusMeters = random().nextDouble() * 444000;
      } else {
        radiusMeters = random().nextDouble() * 50000000;
      }

      // TODO: randomly quantize radius too, to provoke exact math errors?

      GeoRect bbox = GeoUtils.circleToBBox(centerLat, centerLon, radiusMeters);

      int numPointsToTry = 1000;
      for(int i=0;i<numPointsToTry;i++) {

        double lat;
        double lon;

        if (random().nextBoolean()) {
          lat = randomLat(useSmallRanges);
          lon = randomLon(useSmallRanges);
        } else {
          // pick a lat/lon within the bbox or "slightly" outside it to try to improve test efficiency
          lat = BaseGeoPointTestCase.normalizeLat(randomRangeMaybeSlightlyOutside(bbox.minLat, bbox.maxLat));
          if (bbox.crossesDateline()) {
            if (random().nextBoolean()) {
              lon = BaseGeoPointTestCase.normalizeLon(randomRangeMaybeSlightlyOutside(bbox.maxLon, -180));
            } else {
              lon = BaseGeoPointTestCase.normalizeLon(randomRangeMaybeSlightlyOutside(0, bbox.minLon));
            }
          } else {
            lon = BaseGeoPointTestCase.normalizeLon(randomRangeMaybeSlightlyOutside(bbox.minLon, bbox.maxLon));
          }
        }

        double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

        // Haversin says it's within the circle:
        boolean haversinSays = distanceMeters <= radiusMeters;

        // BBox says its within the box:
        boolean bboxSays;
        if (bbox.crossesDateline()) {
          if (lat >= bbox.minLat && lat <= bbox.maxLat) {
            bboxSays = lon <= bbox.maxLon || lon >= bbox.minLon;
          } else {
            bboxSays = false;
          }
        } else {
          bboxSays = lat >= bbox.minLat && lat <= bbox.maxLat && lon >= bbox.minLon && lon <= bbox.maxLon;
        }

        if (haversinSays) {
          if (bboxSays == false) {
            System.out.println("small=" + useSmallRanges + " centerLat=" + centerLat + " cetnerLon=" + centerLon + " radiusMeters=" + radiusMeters);
            System.out.println("  bbox: lat=" + bbox.minLat + " to " + bbox.maxLat + " lon=" + bbox.minLon + " to " + bbox.maxLon);
            System.out.println("  point: lat=" + lat + " lon=" + lon);
            System.out.println("  haversin: " + distanceMeters);
            fail("point was within the distance according to haversin, but the bbox doesn't contain it");
          }
        } else {
          // it's fine if haversin said it was outside the radius and bbox said it was inside the box
        }
      }
    }
  }
  
  // similar to testRandomCircleToBBox, but different, less evil, maybe simpler
  public void testBoundingBoxOpto() {
    for (int i = 0; i < 1000; i++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();
      double radius = 50000000 * random().nextDouble();
      GeoRect box = GeoUtils.circleToBBox(lat, lon, radius);
      final GeoRect box1;
      final GeoRect box2;
      if (box.crossesDateline()) {
        box1 = new GeoRect(box.minLat, box.maxLat, -180, box.maxLon);
        box2 = new GeoRect(box.minLat, box.maxLat, box.minLon, 180);
      } else {
        box1 = box;
        box2 = null;
      }
      
      for (int j = 0; j < 10000; j++) {
        double lat2 = -90 + 180.0 * random().nextDouble();
        double lon2 = -180 + 360.0 * random().nextDouble();
        // if the point is within radius, then it should be in our bounding box
        if (SloppyMath.haversinMeters(lat, lon, lat2, lon2) <= radius) {
          assertTrue(lat >= box.minLat && lat <= box.maxLat);
          assertTrue(lon >= box1.minLon && lon <= box1.maxLon || (box2 != null && lon >= box2.minLon && lon <= box2.maxLon));
        }
      }
    }
  }

  // test we can use haversinSortKey() for distance queries.
  public void testHaversinOpto() {
    for (int i = 0; i < 1000; i++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();
      double radius = 50000000 * random().nextDouble();
      GeoRect box = GeoUtils.circleToBBox(lat, lon, radius);

      if (box.maxLon - lon < 90 && lon - box.minLon < 90) {
        double minPartialDistance = Math.max(SloppyMath.haversinSortKey(lat, lon, lat, box.maxLon),
                                             SloppyMath.haversinSortKey(lat, lon, box.maxLat, lon));
      
        for (int j = 0; j < 10000; j++) {
          double lat2 = -90 + 180.0 * random().nextDouble();
          double lon2 = -180 + 360.0 * random().nextDouble();
          // if the point is within radius, then it should be <= our sort key
          if (SloppyMath.haversinMeters(lat, lon, lat2, lon2) <= radius) {
            assertTrue(SloppyMath.haversinSortKey(lat, lon, lat2, lon2) <= minPartialDistance);
          }
        }
      }
    }
  }

  /** Test infinite radius covers whole earth */
  public void testInfiniteRect() {
    for (int i = 0; i < 1000; i++) {
      double centerLat = -90 + 180.0 * random().nextDouble();
      double centerLon = -180 + 360.0 * random().nextDouble();
      GeoRect rect = GeoUtils.circleToBBox(centerLat, centerLon, Double.POSITIVE_INFINITY);
      assertEquals(-180.0, rect.minLon, 0.0D);
      assertEquals(180.0, rect.maxLon, 0.0D);
      assertEquals(-90.0, rect.minLat, 0.0D);
      assertEquals(90.0, rect.maxLat, 0.0D);
      assertFalse(rect.crossesDateline());
    }
  }
}
