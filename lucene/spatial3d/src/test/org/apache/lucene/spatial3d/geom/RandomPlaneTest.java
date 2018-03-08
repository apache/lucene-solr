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

import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.junit.Test;

/**
 * Random test for planes.
 */
public class RandomPlaneTest extends RandomGeo3dShapeGenerator {

  @Test
  @Repeat(iterations = 10)
  public void testPlaneAccuracy() {
    PlanetModel planetModel = randomPlanetModel();
    GeoPoint point1 = randomGeoPoint(planetModel);
    for (int i= 0; i < 1000; i++) {
      double dist = random().nextDouble() * Vector.MINIMUM_ANGULAR_RESOLUTION + Vector.MINIMUM_ANGULAR_RESOLUTION;
      double bearing = random().nextDouble() * 2 * Math.PI;
      GeoPoint point2 = planetModel.surfacePointOnBearing(point1, dist, bearing );
      GeoPoint check = randomGeoPoint(planetModel);
      if (!point1.isNumericallyIdentical(point2)) {
        SidedPlane plane = new SidedPlane(check, point1, point2);
        String msg = dist + " point 1: " + point1 + ", point 2: " + point2 + " , check: " + check;
        assertTrue(msg, plane.isWithin(check));
        assertTrue(msg, plane.isWithin(point2));
        assertTrue(msg, plane.isWithin(point1));
      }
      else {
        assertFalse("numerically identical", true);
      }
    }
  }
  
  @Test
  @Repeat(iterations = 10)
  public void testPlaneThreePointsAccuracy() {
    PlanetModel planetModel = randomPlanetModel();
    for (int i= 0; i < 1000; i++) {
      GeoPoint point1 = randomGeoPoint(planetModel);
      double dist = random().nextDouble() * Math.PI - Vector.MINIMUM_ANGULAR_RESOLUTION;
      double bearing = random().nextDouble() * 2 * Math.PI;
      GeoPoint point2 = planetModel.surfacePointOnBearing(point1, dist, bearing );
      dist = random().nextDouble() * Vector.MINIMUM_ANGULAR_RESOLUTION + Vector.MINIMUM_ANGULAR_RESOLUTION;
      bearing = random().nextDouble() * 2 * Math.PI;
      GeoPoint point3 = planetModel.surfacePointOnBearing(point1, dist, bearing );
      GeoPoint check = randomGeoPoint(planetModel);
      SidedPlane plane  = SidedPlane.constructNormalizedThreePointSidedPlane(check, point1, point2, point3);
      String msg = planetModel + " point 1: " + point1 + ", point 2: " + point2 + ", point 3: " + point3 + " , check: " + check;
      if (plane == null) {
        fail(msg);
      }
      // This is not expected
      //assertTrue(plane.evaluate(check) + " " + msg, plane.isWithin(check));
      assertTrue(plane.evaluate(point1) + " " +msg, plane.isWithin(point1));
      assertTrue(plane.evaluate(point2) + " " +msg, plane.isWithin(point2));
      assertTrue(plane.evaluate(point3) + " " +msg, plane.isWithin(point3));
    }
  }



  @Test
  @Repeat(iterations = 10)
  public void testPolygonAccuracy() {
    PlanetModel planetModel = randomPlanetModel();
    GeoPoint point1 = randomGeoPoint(planetModel);
    for (int i= 0; i < 1000; i++) {
      double dist = random().nextDouble() * 1e-6 + Vector.MINIMUM_ANGULAR_RESOLUTION;
      GeoPoint point2 = planetModel.surfacePointOnBearing(point1, dist, 0);
      GeoPoint point3 = planetModel.surfacePointOnBearing(point1, dist, 0.5 * Math.PI);

      List<GeoPoint> points = new ArrayList<>();
      points.add(point1);
      points.add(point2);
      points.add(point3);
      GeoPolygonFactory.makeGeoPolygon(planetModel, points);

    }
  }

}
