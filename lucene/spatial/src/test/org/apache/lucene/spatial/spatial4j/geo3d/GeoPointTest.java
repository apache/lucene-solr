package org.apache.lucene.spatial.spatial4j.geo3d;

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

import com.spatial4j.core.distance.DistanceUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;

/**
 * Test basic GeoPoint functionality.
 */
public class GeoPointTest extends LuceneTestCase {

  @Test
  public void testConversion() {
    testPointRoundTrip(PlanetModel.SPHERE, 90, 0, 1e-12);
    testPointRoundTrip(PlanetModel.SPHERE, -90, 0, 1e-12);
    testPointRoundTrip(PlanetModel.WGS84, 90, 0, 1e-12);
    testPointRoundTrip(PlanetModel.WGS84, -90, 0, 1e-12);

    final int times = atLeast(100);
    for (int i = 0; i < times; i++) {
      final double pLat = (randomFloat() * 180.0 - 90.0) * DistanceUtils.DEGREES_TO_RADIANS;
      final double pLon = (randomFloat() * 360.0 - 180.0) * DistanceUtils.DEGREES_TO_RADIANS;
      testPointRoundTrip(PlanetModel.SPHERE, pLat, pLon, 1e-6);//1e-6 since there's a square root in there (Karl says)
      testPointRoundTrip(PlanetModel.WGS84, pLat, pLon, 1e-6);
    }
  }

  protected void testPointRoundTrip(PlanetModel planetModel, double pLat, double pLon, double epsilon) {
    final GeoPoint p1 = new GeoPoint(planetModel, pLat, pLon);
    final GeoPoint p2 = new GeoPoint(planetModel, p1.getLatitude(), p1.getLongitude());
    double dist = p1.arcDistance(p2);
    assertEquals(0, dist, epsilon);
  }

}


