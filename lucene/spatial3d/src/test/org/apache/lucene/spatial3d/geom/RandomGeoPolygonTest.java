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
 * Random test for polygons.
 */
public class RandomGeoPolygonTest extends RandomGeo3dShapeGenerator {

  @Test
  @Repeat(iterations = 10)
  public void testRandomLUCENE8157() {
    final PlanetModel planetModel = randomPlanetModel();
    final GeoPoint startPoint = randomGeoPoint(planetModel);
    double d = random().nextDouble();
    final double distanceSmall = d * 1e-9  + Vector.MINIMUM_ANGULAR_RESOLUTION;
    final double distanceBig = d * 1e-7 + Vector.MINIMUM_ANGULAR_RESOLUTION ;
    final double bearing = random().nextDouble() *  Math.PI;
    GeoPoint point1 = planetModel.surfacePointOnBearing(startPoint, distanceSmall, bearing*1.001);
    GeoPoint point2 = planetModel.surfacePointOnBearing(startPoint, distanceBig, bearing);
    GeoPoint point3 = planetModel.surfacePointOnBearing(startPoint, distanceBig, bearing - 0.5 * Math.PI);
    List<GeoPoint> points = new ArrayList<>();
    points.add(startPoint);
    points.add(point1);
    points.add(point2);
    points.add(point3);
    try {
      GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, points);
      assertTrue(polygon != null);
    }
    catch(Exception e) {
      fail(points.toString());
    }
  }

  public void testLUCENE8157() {
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, 0.281855362988772, -0.7816673189809037);
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, 0.28185536309057774, -0.7816673188511931);
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, 0.28186535556824205, -0.7816546103463846);
    GeoPoint point4 = new GeoPoint(PlanetModel.SPHERE, 0.28186757010406716, -0.7816777221140381);
    List<GeoPoint> points = new ArrayList<>();
    points.add(point1);
    points.add(point2);
    points.add(point3);
    points.add(point4);
    try {
      GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    }
    catch(Exception e) {
      fail(points.toString());
    }
  }

  @Test
  public void testCoplanarityTilePolygon() {
    //POLYGON((-90.55764 -0.34907,-90.55751 -0.34868,-90.55777 -0.34842,-90.55815 -0.34766,-90.55943 -0.34842, -90.55918 -0.34842,-90.55764 -0.34907))
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34907), Geo3DUtil.fromDegrees(-90.55764)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34868), Geo3DUtil.fromDegrees(-90.55751)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34842), Geo3DUtil.fromDegrees(-90.55777)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34766), Geo3DUtil.fromDegrees(-90.55815)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34842), Geo3DUtil.fromDegrees(-90.55943)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.34842), Geo3DUtil.fromDegrees(-90.55918)));
    GeoCompositePolygon polygon = (GeoCompositePolygon)GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    assertTrue(polygon.size() == 3);
  }
}
