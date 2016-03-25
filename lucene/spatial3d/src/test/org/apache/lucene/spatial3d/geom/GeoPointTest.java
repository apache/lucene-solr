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
package org.apache.lucene.spatial3d.geom;

import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;

/**
 * Test basic GeoPoint functionality.
 */
public class GeoPointTest extends LuceneTestCase {
  static final double DEGREES_TO_RADIANS =  Math.PI / 180;

  @Test
  public void testConversion() {
    testPointRoundTrip(PlanetModel.SPHERE, 90 * DEGREES_TO_RADIANS, 0, 1e-6);
    testPointRoundTrip(PlanetModel.SPHERE, -90 * DEGREES_TO_RADIANS, 0, 1e-6);
    testPointRoundTrip(PlanetModel.WGS84, 90 * DEGREES_TO_RADIANS, 0, 1e-6);
    testPointRoundTrip(PlanetModel.WGS84, -90 * DEGREES_TO_RADIANS, 0, 1e-6);

    final int times = atLeast(100);
    for (int i = 0; i < times; i++) {
      final double pLat = (randomFloat() * 180.0 - 90.0) * DEGREES_TO_RADIANS;
      final double pLon = (randomFloat() * 360.0 - 180.0) * DEGREES_TO_RADIANS;
      testPointRoundTrip(PlanetModel.SPHERE, pLat, pLon, 1e-6);//1e-6 since there's a square root in there (Karl says)
      testPointRoundTrip(PlanetModel.WGS84, pLat, pLon, 1e-6);
    }
  }

  protected void testPointRoundTrip(PlanetModel planetModel, double pLat, double pLon, double epsilon) {
    final GeoPoint p1 = new GeoPoint(planetModel, pLat, pLon);
    // In order to force the reverse conversion, we have to construct a geopoint from just x,y,z
    final GeoPoint p2 = new GeoPoint(p1.x, p1.y, p1.z);
    // Now, construct the final point based on getLatitude() and getLongitude()
    final GeoPoint p3 = new GeoPoint(planetModel, p2.getLatitude(), p2.getLongitude());
    double dist = p1.arcDistance(p3);
    assertEquals(0, dist, epsilon);
  }

  @Test
  public void testSurfaceDistance() {
    final int times = atLeast(100);
    for (int i = 0; i < times; i++) {
      final double p1Lat = (randomFloat() * 180.0 - 90.0) * DEGREES_TO_RADIANS;
      final double p1Lon = (randomFloat() * 360.0 - 180.0) * DEGREES_TO_RADIANS;
      final double p2Lat = (randomFloat() * 180.0 - 90.0) * DEGREES_TO_RADIANS;
      final double p2Lon = (randomFloat() * 360.0 - 180.0) * DEGREES_TO_RADIANS;
      final GeoPoint p1 = new GeoPoint(PlanetModel.SPHERE, p1Lat, p1Lon);
      final GeoPoint p2 = new GeoPoint(PlanetModel.SPHERE, p2Lat, p2Lon);
      final double arcDistance = p1.arcDistance(p2);
      // Compute ellipsoid distance; it should agree for a sphere
      final double surfaceDistance = PlanetModel.SPHERE.surfaceDistance(p1,p2);
      assertEquals(arcDistance, surfaceDistance, 1e-6);
    }

    // Now try some WGS84 points (taken randomly and compared against a known-good implementation)
    assertEquals(1.1444648695765323, PlanetModel.WGS84.surfaceDistance(
      new GeoPoint(PlanetModel.WGS84, 0.038203808753702884, -0.6701260455506466),
      new GeoPoint(PlanetModel.WGS84, -0.8453720422675458, 0.1737353153814496)), 1e-6);
    assertEquals(1.4345148695890722, PlanetModel.WGS84.surfaceDistance(
      new GeoPoint(PlanetModel.WGS84, 0.5220926323378574, 0.6758041581907408),
      new GeoPoint(PlanetModel.WGS84, -0.8453720422675458, 0.1737353153814496)), 1e-6);
    assertEquals(2.32418144616446, PlanetModel.WGS84.surfaceDistance(
      new GeoPoint(PlanetModel.WGS84, 0.09541335760967473, 1.2091829760623236),
      new GeoPoint(PlanetModel.WGS84, -0.8501591797459979, -2.3044806381627594)), 1e-6);
    assertEquals(2.018421047005435, PlanetModel.WGS84.surfaceDistance(
      new GeoPoint(PlanetModel.WGS84, 0.3402853531962009, -0.43544195327249957),
      new GeoPoint(PlanetModel.WGS84, -0.8501591797459979, -2.3044806381627594)), 1e-6);
  }

  @Test
  public void testBisection() {
    final int times = atLeast(100);
    for (int i = 0; i < times; i++) {
      final double p1Lat = (randomFloat() * 180.0 - 90.0) * DEGREES_TO_RADIANS;
      final double p1Lon = (randomFloat() * 360.0 - 180.0) * DEGREES_TO_RADIANS;
      final double p2Lat = (randomFloat() * 180.0 - 90.0) * DEGREES_TO_RADIANS;
      final double p2Lon = (randomFloat() * 360.0 - 180.0) * DEGREES_TO_RADIANS;
      final GeoPoint p1 = new GeoPoint(PlanetModel.WGS84, p1Lat, p1Lon);
      final GeoPoint p2 = new GeoPoint(PlanetModel.WGS84, p2Lat, p2Lon);
      final GeoPoint pMid = PlanetModel.WGS84.bisection(p1, p2);
      if (pMid != null) {
        final double arcDistance = p1.arcDistance(p2);
        final double sum = pMid.arcDistance(p1) + pMid.arcDistance(p2);
        assertEquals(arcDistance, sum, 1e-6);
      }
    }
  }
  
  @Test(expected = IllegalArgumentException.class)
  public void testBadLatLon() {
    new GeoPoint(PlanetModel.SPHERE, 50.0, 32.2);
  }
}
