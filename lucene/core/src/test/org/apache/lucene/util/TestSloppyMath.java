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
package org.apache.lucene.util;


import static org.apache.lucene.util.SloppyMath.cos;
import static org.apache.lucene.util.SloppyMath.asin;
import static org.apache.lucene.util.SloppyMath.haversinMeters;
import static org.apache.lucene.util.SloppyMath.haversinSortKey;

import java.util.Random;

import org.apache.lucene.geo.GeoTestUtil;


public class TestSloppyMath extends LuceneTestCase {
  // accuracy for cos()
  static double COS_DELTA = 1E-15;
  // accuracy for asin()
  static double ASIN_DELTA = 1E-7;
  // accuracy for haversinMeters()
  static double HAVERSIN_DELTA = 38E-2;
  // accuracy for haversinMeters() for "reasonable" distances (< 1000km)
  static double REASONABLE_HAVERSIN_DELTA = 1E-5;
  
  public void testCos() {
    assertTrue(Double.isNaN(cos(Double.NaN)));
    assertTrue(Double.isNaN(cos(Double.NEGATIVE_INFINITY)));
    assertTrue(Double.isNaN(cos(Double.POSITIVE_INFINITY)));
    assertEquals(StrictMath.cos(1), cos(1), COS_DELTA);
    assertEquals(StrictMath.cos(0), cos(0), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/2), cos(Math.PI/2), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/2), cos(-Math.PI/2), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/4), cos(Math.PI/4), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/4), cos(-Math.PI/4), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI*2/3), cos(Math.PI*2/3), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI*2/3), cos(-Math.PI*2/3), COS_DELTA);
    assertEquals(StrictMath.cos(Math.PI/6), cos(Math.PI/6), COS_DELTA);
    assertEquals(StrictMath.cos(-Math.PI/6), cos(-Math.PI/6), COS_DELTA);
    
    // testing purely random longs is inefficent, as for stupid parameters we just 
    // pass thru to Math.cos() instead of doing some huperduper arg reduction
    for (int i = 0; i < 10000; i++) {
      double d = random().nextDouble() * SloppyMath.SIN_COS_MAX_VALUE_FOR_INT_MODULO;
      if (random().nextBoolean()) {
        d = -d;
      }
      assertEquals(StrictMath.cos(d), cos(d), COS_DELTA);
    }
  }
  
  public void testAsin() {
    assertTrue(Double.isNaN(asin(Double.NaN)));
    assertTrue(Double.isNaN(asin(2)));
    assertTrue(Double.isNaN(asin(-2)));
    assertEquals(-Math.PI/2, asin(-1), ASIN_DELTA);
    assertEquals(-Math.PI/3, asin(-0.8660254), ASIN_DELTA);
    assertEquals(-Math.PI/4, asin(-0.7071068), ASIN_DELTA);
    assertEquals(-Math.PI/6, asin(-0.5), ASIN_DELTA);
    assertEquals(0, asin(0), ASIN_DELTA);
    assertEquals(Math.PI/6, asin(0.5), ASIN_DELTA);
    assertEquals(Math.PI/4, asin(0.7071068), ASIN_DELTA);
    assertEquals(Math.PI/3, asin(0.8660254), ASIN_DELTA);
    assertEquals(Math.PI/2, asin(1), ASIN_DELTA);
    // only values -1..1 are useful
    for (int i = 0; i < 10000; i++) {
      double d = random().nextDouble();
      if (random().nextBoolean()) {
        d = -d;
      }
      assertEquals(StrictMath.asin(d), asin(d), ASIN_DELTA);
      assertTrue(asin(d) >= -Math.PI/2);
      assertTrue(asin(d) <= Math.PI/2);
    }
  }
  
  public void testHaversin() {
    assertTrue(Double.isNaN(haversinMeters(1, 1, 1, Double.NaN)));
    assertTrue(Double.isNaN(haversinMeters(1, 1, Double.NaN, 1)));
    assertTrue(Double.isNaN(haversinMeters(1, Double.NaN, 1, 1)));
    assertTrue(Double.isNaN(haversinMeters(Double.NaN, 1, 1, 1)));
    
    assertEquals(0, haversinMeters(0, 0, 0, 0), 0D);
    assertEquals(0, haversinMeters(0, -180, 0, -180), 0D);
    assertEquals(0, haversinMeters(0, -180, 0, 180), 0D);
    assertEquals(0, haversinMeters(0, 180, 0, 180), 0D);
    assertEquals(0, haversinMeters(90, 0, 90, 0), 0D);
    assertEquals(0, haversinMeters(90, -180, 90, -180), 0D);
    assertEquals(0, haversinMeters(90, -180, 90, 180), 0D);
    assertEquals(0, haversinMeters(90, 180, 90, 180), 0D);
    
    // Test half a circle on the equator, using WGS84 mean earth radius in meters
    double earthRadiusMs = 6_371_008.7714;
    double halfCircle = earthRadiusMs * Math.PI;
    assertEquals(halfCircle, haversinMeters(0, 0, 0, 180), 0D);

    Random r = random();
    double randomLat1 = 40.7143528 + (r.nextInt(10) - 5) * 360;
    double randomLon1 = -74.0059731 + (r.nextInt(10) - 5) * 360;

    double randomLat2 = 40.65 + (r.nextInt(10) - 5) * 360;
    double randomLon2 = -73.95 + (r.nextInt(10) - 5) * 360;
    
    assertEquals(8_572.1137, haversinMeters(randomLat1, randomLon1, randomLat2, randomLon2), 0.01D);
    
    
    // from solr and ES tests (with their respective epsilons)
    assertEquals(0, haversinMeters(40.7143528, -74.0059731, 40.7143528, -74.0059731), 0D);
    assertEquals(5_285.89, haversinMeters(40.7143528, -74.0059731, 40.759011, -73.9844722), 0.01D);
    assertEquals(462.10, haversinMeters(40.7143528, -74.0059731, 40.718266, -74.007819), 0.01D);
    assertEquals(1_054.98, haversinMeters(40.7143528, -74.0059731, 40.7051157, -74.0088305), 0.01D);
    assertEquals(1_258.12, haversinMeters(40.7143528, -74.0059731, 40.7247222, -74), 0.01D);
    assertEquals(2_028.52, haversinMeters(40.7143528, -74.0059731, 40.731033, -73.9962255), 0.01D);
    assertEquals(8_572.11, haversinMeters(40.7143528, -74.0059731, 40.65, -73.95), 0.01D);
  }
  
  /** Test this method sorts the same way as real haversin */
  public void testHaversinSortKey() {
    int iters = atLeast(10000);
    for (int i = 0; i < iters; i++) {
      double centerLat = GeoTestUtil.nextLatitude();
      double centerLon = GeoTestUtil.nextLongitude();

      double lat1 = GeoTestUtil.nextLatitude();
      double lon1 = GeoTestUtil.nextLongitude();

      double lat2 = GeoTestUtil.nextLatitude();
      double lon2 = GeoTestUtil.nextLongitude();

      int expected = Integer.signum(Double.compare(haversinMeters(centerLat, centerLon, lat1, lon1),
                                                   haversinMeters(centerLat, centerLon, lat2, lon2)));
      int actual = Integer.signum(Double.compare(haversinSortKey(centerLat, centerLon, lat1, lon1),
                                                 haversinSortKey(centerLat, centerLon, lat2, lon2)));
      assertEquals(expected, actual);
      assertEquals(haversinMeters(centerLat, centerLon, lat1, lon1), haversinMeters(haversinSortKey(centerLat, centerLon, lat1, lon1)), 0.0D);
      assertEquals(haversinMeters(centerLat, centerLon, lat2, lon2), haversinMeters(haversinSortKey(centerLat, centerLon, lat2, lon2)), 0.0D);
    }
  }
  
  public void testHaversinFromSortKey() {
    assertEquals(0.0, haversinMeters(0), 0.0D);
  }
  
  public void testAgainstSlowVersion() {
    for (int i = 0; i < 100_000; i++) {
      double lat1 = GeoTestUtil.nextLatitude();
      double lon1 = GeoTestUtil.nextLongitude();
      double lat2 = GeoTestUtil.nextLatitude();
      double lon2 = GeoTestUtil.nextLongitude();

      double expected = slowHaversin(lat1, lon1, lat2, lon2);
      double actual = haversinMeters(lat1, lon1, lat2, lon2);
      assertEquals(expected, actual, HAVERSIN_DELTA);
    }
  }

  /**
   * Step across the whole world to find huge absolute errors.
   * Don't rely on random number generator to pick these massive distances. */
  public void testAcrossWholeWorldSteps() {
    for (int lat1 = -90; lat1 <= 90; lat1 += 10) {
      for (int lon1 = -180; lon1 <= 180; lon1 += 10) {
        for (int lat2 = -90; lat2 <= 90; lat2 += 10) {
          for (int lon2 = -180; lon2 <= 180; lon2 += 10) {
            double expected = slowHaversin(lat1, lon1, lat2, lon2);
            double actual = haversinMeters(lat1, lon1, lat2, lon2);
            assertEquals(expected, actual, HAVERSIN_DELTA);
          }
        }
      }
    }
  }
  
  public void testAgainstSlowVersionReasonable() {
    for (int i = 0; i < 100_000; i++) {
      double lat1 = GeoTestUtil.nextLatitude();
      double lon1 = GeoTestUtil.nextLongitude();
      double lat2 = GeoTestUtil.nextLatitude();
      double lon2 = GeoTestUtil.nextLongitude();

      double expected = haversinMeters(lat1, lon1, lat2, lon2);
      if (expected < 1_000_000) {
        double actual = slowHaversin(lat1, lon1, lat2, lon2);
        assertEquals(expected, actual, REASONABLE_HAVERSIN_DELTA);
      }
    }
  }
  
  // simple incorporation of the wikipedia formula
  private static double slowHaversin(double lat1, double lon1, double lat2, double lon2) {
    double h1 = (1 - StrictMath.cos(StrictMath.toRadians(lat2) - StrictMath.toRadians(lat1))) / 2;
    double h2 = (1 - StrictMath.cos(StrictMath.toRadians(lon2) - StrictMath.toRadians(lon1))) / 2;
    double h = h1 + StrictMath.cos(StrictMath.toRadians(lat1)) * StrictMath.cos(StrictMath.toRadians(lat2)) * h2;
    return 2 * 6371008.7714 * StrictMath.asin(Math.min(1, Math.sqrt(h))); 
  }
}
