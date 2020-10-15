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
import java.util.Collections;
import java.util.List;

import com.carrotsearch.randomizedtesting.generators.BiasedNumbers;
import org.junit.Test;

/**
 * Random test for polygons.
 */
public class RandomGeoPolygonTest extends RandomGeo3dShapeGenerator {

  @Test
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

  /**
   * Test comparing different polygon (Big) technologies using random
   * biased doubles.
   */
  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8281")
  public void testCompareBigPolygons() {
    testComparePolygons(Math.PI);
  }

  /**
   * Test comparing different polygon (Small) technologies using random
   * biased doubles.
   */
  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8281")
  public void testCompareSmallPolygons() {
    testComparePolygons(1e-4 * Math.PI);
  }


  private void testComparePolygons(double limitDistance) {
    final PlanetModel planetModel = randomPlanetModel();
    //Create polygon points using a reference point and a maximum distance to the point
    final GeoPoint referencePoint;
    if (random().nextBoolean()) {
     referencePoint = getBiasedPoint(planetModel);
    } else {
      referencePoint = randomGeoPoint(planetModel);
    }
    final int n = random().nextInt(4) + 4;

    List<GeoPoint> orderedPoints = null;
    GeoPolygon polygon = null;
    GeoPolygon largePolygon = null;
    do {
      final List<GeoPoint> points = new ArrayList<>(n);
      double maxDistance = random().nextDouble() * limitDistance;
      //if distance is too small we can fail
      //building the polygon.
      while (maxDistance < 1e-7) {
        maxDistance = random().nextDouble() * limitDistance;
      }
      for (int i = 0; i < n; i++) {
        while (true) {
          final double distance = BiasedNumbers.randomDoubleBetween(random(), 0, maxDistance);// random().nextDouble() * maxDistance;
          final double bearing = random().nextDouble() * 2 * Math.PI;
          final GeoPoint p = planetModel.surfacePointOnBearing(referencePoint, distance, bearing);
          if (!contains(p, points)) {
            if (points.size() > 1 && Plane.arePointsCoplanar(points.get(points.size() - 1), points.get(points.size() - 2), p)) {
              continue;
            }
            points.add(p);
            break;
          }
        }
      }
      //order points so we don't get crossing edges
      orderedPoints = orderPoints(points);
      if (random().nextBoolean() && random().nextBoolean()) {
        Collections.reverse(orderedPoints);
      }
      final GeoPolygonFactory.PolygonDescription polygonDescription = new GeoPolygonFactory.PolygonDescription(orderedPoints);

      try {
        polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, polygonDescription);
      } catch (Exception e) {
        final StringBuilder buffer = new StringBuilder("Polygon failed to build with an exception:\n");
        buffer.append(points.toString() + "\n");
        buffer.append("WKT:" + getWKT(orderedPoints));
        buffer.append(e.toString());
        fail(buffer.toString());
      }
      if (polygon == null) {
        final StringBuilder buffer = new StringBuilder("Polygon failed to build:\n");
        buffer.append(points.toString() + "\n");
        buffer.append("WKT:" + getWKT(orderedPoints));
        fail(buffer.toString());
      }
      try {
        largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(planetModel, Collections.singletonList(polygonDescription));
      } catch (Exception e) {
        final StringBuilder buffer = new StringBuilder("Large polygon failed to build with an exception:\n");
        buffer.append(points.toString() + "\n");
        buffer.append("WKT:" + getWKT(orderedPoints));
        buffer.append(e.toString());
        fail(buffer.toString());
      }
      if (largePolygon == null) {
        StringBuilder buffer = new StringBuilder("Large polygon failed to build:\n");
        buffer.append(points.toString() + "\n");
        buffer.append("WKT:" + getWKT(orderedPoints));
        fail(buffer.toString());
      }
    } while(polygon.getClass().equals(largePolygon.getClass()));
    //Some of these do not work but it seems it s from the way the point is created
    //GeoPoint centerOfMass = getCenterOfMass(planetModel, orderedPoints);
    //checkPoint(polygon, largePolygon, centerOfMass, orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(-centerOfMass.x, -centerOfMass.y, -centerOfMass.z), orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(centerOfMass.x, -centerOfMass.y, -centerOfMass.z), orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(centerOfMass.x, centerOfMass.y, -centerOfMass.z), orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(-centerOfMass.x, -centerOfMass.y, centerOfMass.z), orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(-centerOfMass.x, centerOfMass.y, -centerOfMass.z), orderedPoints);
    //checkPoint(polygon, largePolygon, new GeoPoint(centerOfMass.x, -centerOfMass.y, centerOfMass.z), orderedPoints);
    for(int i = 0; i < 100000; i++) {
      final GeoPoint point;
      if (random().nextBoolean()) {
        point = getBiasedPoint(planetModel);
      } else {
        point = randomGeoPoint(planetModel);
      }
      checkPoint(polygon, largePolygon, point, orderedPoints);
    }
  }

  private void checkPoint(final GeoPolygon polygon, final GeoPolygon largePolygon, final GeoPoint point, final List<GeoPoint> orderedPoints) {
    final boolean withIn1 = polygon.isWithin(point);
    final boolean withIn2 = largePolygon.isWithin(point);
    StringBuilder buffer = new StringBuilder();
    if (withIn1 != withIn2) {
      //NOTE: Standard and large polygon are mathematically slightly different
      //close to the edges (due to bounding planes). Nothing we can do about that
      //so we filter the differences.
      final double d1 = polygon.computeOutsideDistance(DistanceStyle.ARC, point);
      final double d2  = largePolygon.computeOutsideDistance(DistanceStyle.ARC, point);
      if (d1 == 0 && d2 == 0) {
        return;
      }
      buffer = buffer.append("\nStandard polygon: " + polygon.toString() +"\n");
      buffer = buffer.append("\nLarge polygon: " + largePolygon.toString() +"\n");
      buffer = buffer.append("\nPoint: " + point.toString() +"\n");
      buffer.append("\nWKT: " + getWKT(orderedPoints));
      buffer.append("\nWKT: POINT(" + Geo3DUtil.toDegrees(point.getLongitude()) + " " + Geo3DUtil.toDegrees(point.getLatitude()) + ")\n");
      buffer.append("normal polygon: " +withIn1 + "\n");
      buffer.append("large polygon: " + withIn2 + "\n");
    }
    assertTrue(buffer.toString(), withIn1 == withIn2);
  }

  private GeoPoint getBiasedPoint(PlanetModel planetModel) {
    double lat = BiasedNumbers.randomDoubleBetween(random(), 0, Math.PI / 2);
    if (random().nextBoolean()) {
      lat = (-1) * lat;
    }
    double lon = BiasedNumbers.randomDoubleBetween(random(), 0, Math.PI);
    if (random().nextBoolean()) {
      lon = (-1) * lon;
    }
    return new GeoPoint(planetModel, lat, lon);
  }

  private String getWKT(List<GeoPoint> points) {
    StringBuffer buffer = new StringBuffer("POLYGON((");
    for (GeoPoint point : points) {
      buffer.append(Geo3DUtil.toDegrees(point.getLongitude()) + " " + Geo3DUtil.toDegrees(point.getLatitude()) + ",");
    }
    buffer.append(Geo3DUtil.toDegrees(points.get(0).getLongitude()) + " " + Geo3DUtil.toDegrees(points.get(0).getLatitude()) + "))\n");
    return buffer.toString();
  }

  private boolean contains(GeoPoint p, List<GeoPoint> points) {
    for (GeoPoint point : points) {
      if (point.isNumericallyIdentical(p)) {
        return true;
      }
    }
    return false;
  }

  private GeoPoint getCenterOfMass(final PlanetModel planetModel, final List<GeoPoint> points) {
    double x = 0;
    double y = 0;
    double z = 0;
    //get center of mass
    for (final GeoPoint point : points) {
      x += point.x;
      y += point.y;
      z += point.z;
    }
    // Normalization is not needed because createSurfacePoint does the scaling anyway.
    return planetModel.createSurfacePoint(x, y, z);
  }

}
