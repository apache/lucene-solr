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

import java.util.Locale;

import org.apache.lucene.geo.GeoUtils;
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

  // Global bounding box we will "cover" in the random test; we have to make this "smallish" else the queries take very long:
  private static double originLat;
  private static double originLon;

  @BeforeClass
  public static void beforeClass() throws Exception {
    originLon = GeoTestUtil.nextLongitude();
    originLat = GeoTestUtil.nextLatitude();
  }

  public double randomLat(boolean small) {
    double result;
    if (small) {
      result = GeoTestUtil.nextLatitudeNear(originLat);
    } else {
      result = GeoTestUtil.nextLatitude();
    }
    return result;
  }

  public double randomLon(boolean small) {
    double result;
    if (small) {
      result = GeoTestUtil.nextLongitudeNear(originLon);
    } else {
      result = GeoTestUtil.nextLongitude();
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
  
  /** make sure values always go down: this is important for edge case consistency */
  public void testEncodeDecodeRoundsDown() throws Exception {
    int iters = atLeast(1000);
    for(int iter=0;iter<iters;iter++) {
      double lat = -90 + 180.0 * random().nextDouble();
      double lon = -180 + 360.0 * random().nextDouble();
      
      long enc = GeoEncodingUtils.mortonHash(lat, lon);
      double latEnc = GeoEncodingUtils.mortonUnhashLat(enc);
      double lonEnc = GeoEncodingUtils.mortonUnhashLon(enc);
      assertTrue(latEnc <= lat);
      assertTrue(lonEnc <= lon);
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

      GeoRect bbox = GeoRect.fromPointDistance(centerLat, centerLon, radiusMeters);

      int numPointsToTry = 1000;
      for(int i=0;i<numPointsToTry;i++) {

        double lat;
        double lon;

        if (random().nextBoolean()) {
          lat = randomLat(useSmallRanges);
          lon = randomLon(useSmallRanges);
        } else {
          // pick a lat/lon within the bbox or "slightly" outside it to try to improve test efficiency
          lat = GeoTestUtil.nextLatitudeAround(bbox.minLat, bbox.maxLat);
          if (bbox.crossesDateline()) {
            if (random().nextBoolean()) {
              lon = GeoTestUtil.nextLongitudeAround(bbox.maxLon, -180);
            } else {
              lon = GeoTestUtil.nextLongitudeAround(0, bbox.minLon);
            }
          } else {
            lon = GeoTestUtil.nextLongitudeAround(bbox.minLon, bbox.maxLon);
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
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();
      double radius = 50000000 * random().nextDouble();
      GeoRect box = GeoRect.fromPointDistance(lat, lon, radius);
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
        double lat2 = GeoTestUtil.nextLatitude();
        double lon2 = GeoTestUtil.nextLongitude();
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
      double lat = GeoTestUtil.nextLatitude();
      double lon = GeoTestUtil.nextLongitude();
      double radius = 50000000 * random().nextDouble();
      GeoRect box = GeoRect.fromPointDistance(lat, lon, radius);

      if (box.maxLon - lon < 90 && lon - box.minLon < 90) {
        double minPartialDistance = Math.max(SloppyMath.haversinSortKey(lat, lon, lat, box.maxLon),
                                             SloppyMath.haversinSortKey(lat, lon, box.maxLat, lon));
      
        for (int j = 0; j < 10000; j++) {
          double lat2 = GeoTestUtil.nextLatitude();
          double lon2 = GeoTestUtil.nextLongitude();
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
      double centerLat = GeoTestUtil.nextLatitude();
      double centerLon = GeoTestUtil.nextLongitude();
      GeoRect rect = GeoRect.fromPointDistance(centerLat, centerLon, Double.POSITIVE_INFINITY);
      assertEquals(-180.0, rect.minLon, 0.0D);
      assertEquals(180.0, rect.maxLon, 0.0D);
      assertEquals(-90.0, rect.minLat, 0.0D);
      assertEquals(90.0, rect.maxLat, 0.0D);
      assertFalse(rect.crossesDateline());
    }
  }
  
  public void testAxisLat() {
    double earthCircumference = 2D * Math.PI * GeoUtils.EARTH_MEAN_RADIUS_METERS;
    assertEquals(90, GeoRect.axisLat(0, earthCircumference / 4), 0.0D);

    for (int i = 0; i < 100; ++i) {
      boolean reallyBig = random().nextInt(10) == 0;
      final double maxRadius = reallyBig ? 1.1 * earthCircumference : earthCircumference / 8;
      final double radius = maxRadius * random().nextDouble();
      double prevAxisLat = GeoRect.axisLat(0.0D, radius);
      for (double lat = 0.1D; lat < 90D; lat += 0.1D) {
        double nextAxisLat = GeoRect.axisLat(lat, radius);
        GeoRect bbox = GeoRect.fromPointDistance(lat, 180D, radius);
        double dist = SloppyMath.haversinMeters(lat, 180D, nextAxisLat, bbox.maxLon);
        if (nextAxisLat < GeoUtils.MAX_LAT_INCL) {
          assertEquals("lat = " + lat, dist, radius, 0.1D);
        }
        assertTrue("lat = " + lat, prevAxisLat <= nextAxisLat);
        prevAxisLat = nextAxisLat;
      }

      prevAxisLat = GeoRect.axisLat(-0.0D, radius);
      for (double lat = -0.1D; lat > -90D; lat -= 0.1D) {
        double nextAxisLat = GeoRect.axisLat(lat, radius);
        GeoRect bbox = GeoRect.fromPointDistance(lat, 180D, radius);
        double dist = SloppyMath.haversinMeters(lat, 180D, nextAxisLat, bbox.maxLon);
        if (nextAxisLat > GeoUtils.MIN_LAT_INCL) {
          assertEquals("lat = " + lat, dist, radius, 0.1D);
        }
        assertTrue("lat = " + lat, prevAxisLat >= nextAxisLat);
        prevAxisLat = nextAxisLat;
      }
    }
  }
  
  // TODO: does not really belong here, but we test it like this for now
  // we can make a fake IndexReader to send boxes directly to Point visitors instead?
  public void testCircleOpto() throws Exception {
    for (int i = 0; i < 50; i++) {
      // circle
      final double centerLat = -90 + 180.0 * random().nextDouble();
      final double centerLon = -180 + 360.0 * random().nextDouble();
      final double radius = 50_000_000D * random().nextDouble();
      final GeoRect box = GeoRect.fromPointDistance(centerLat, centerLon, radius);
      // TODO: remove this leniency!
      if (box.crossesDateline()) {
        --i; // try again...
        continue;
      }
      final double axisLat = GeoRect.axisLat(centerLat, radius);

      for (int k = 0; k < 1000; ++k) {

        double[] latBounds = {-90, box.minLat, axisLat, box.maxLat, 90};
        double[] lonBounds = {-180, box.minLon, centerLon, box.maxLon, 180};
        // first choose an upper left corner
        int maxLatRow = random().nextInt(4);
        double latMax = randomInRange(latBounds[maxLatRow], latBounds[maxLatRow + 1]);
        int minLonCol = random().nextInt(4);
        double lonMin = randomInRange(lonBounds[minLonCol], lonBounds[minLonCol + 1]);
        // now choose a lower right corner
        int minLatMaxRow = maxLatRow == 3 ? 3 : maxLatRow + 1; // make sure it will at least cross into the bbox
        int minLatRow = random().nextInt(minLatMaxRow);
        double latMin = randomInRange(latBounds[minLatRow], Math.min(latBounds[minLatRow + 1], latMax));
        int maxLonMinCol = Math.max(minLonCol, 1); // make sure it will at least cross into the bbox
        int maxLonCol = maxLonMinCol + random().nextInt(4 - maxLonMinCol);
        double lonMax = randomInRange(Math.max(lonBounds[maxLonCol], lonMin), lonBounds[maxLonCol + 1]);

        assert latMax >= latMin;
        assert lonMax >= lonMin;

        if (isDisjoint(centerLat, centerLon, radius, axisLat, latMin, latMax, lonMin, lonMax)) {
          // intersects says false: test a ton of points
          for (int j = 0; j < 200; j++) {
            double lat = latMin + (latMax - latMin) * random().nextDouble();
            double lon = lonMin + (lonMax - lonMin) * random().nextDouble();

            if (random().nextBoolean()) {
              // explicitly test an edge
              int edge = random().nextInt(4);
              if (edge == 0) {
                lat = latMin;
              } else if (edge == 1) {
                lat = latMax;
              } else if (edge == 2) {
                lon = lonMin;
              } else if (edge == 3) {
                lon = lonMax;
              }
            }
            double distance = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);
            try {
            assertTrue(String.format(Locale.ROOT, "\nisDisjoint(\n" +
                    "centerLat=%s\n" +
                    "centerLon=%s\n" +
                    "radius=%s\n" +
                    "latMin=%s\n" +
                    "latMax=%s\n" +
                    "lonMin=%s\n" +
                    "lonMax=%s) == false BUT\n" +
                    "haversin(%s, %s, %s, %s) = %s\nbbox=%s",
                centerLat, centerLon, radius, latMin, latMax, lonMin, lonMax,
                centerLat, centerLon, lat, lon, distance, GeoRect.fromPointDistance(centerLat, centerLon, radius)),
                distance > radius);
            } catch (AssertionError e) {
              GeoTestUtil.toWebGLEarth(latMin, latMax, lonMin, lonMax, centerLat, centerLon, radius);
              throw e;
            }
          }
        }
      }
    }
  }

  static double randomInRange(double min, double max) {
    return min + (max - min) * random().nextDouble();
  }
  
  static boolean isDisjoint(double centerLat, double centerLon, double radius, double axisLat, double latMin, double latMax, double lonMin, double lonMax) {
    if ((centerLon < lonMin || centerLon > lonMax) && (axisLat+GeoRect.AXISLAT_ERROR < latMin || axisLat-GeoRect.AXISLAT_ERROR > latMax)) {
      // circle not fully inside / crossing axis
      if (SloppyMath.haversinMeters(centerLat, centerLon, latMin, lonMin) > radius &&
          SloppyMath.haversinMeters(centerLat, centerLon, latMin, lonMax) > radius &&
          SloppyMath.haversinMeters(centerLat, centerLon, latMax, lonMin) > radius &&
          SloppyMath.haversinMeters(centerLat, centerLon, latMax, lonMax) > radius) {
        // no points inside
        return true;
      }
    }
    
    return false;
  }
}
