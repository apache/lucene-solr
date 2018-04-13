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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.generators.BiasedNumbers;
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
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34907), fromDegrees(-90.55764)));
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34868), fromDegrees(-90.55751)));
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34842), fromDegrees(-90.55777)));
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34766), fromDegrees(-90.55815)));
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34842), fromDegrees(-90.55943)));
    points.add(new GeoPoint(PlanetModel.SPHERE, fromDegrees(-0.34842), fromDegrees(-90.55918)));
    GeoCompositePolygon polygon = (GeoCompositePolygon)GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    assertTrue(polygon.size() == 3);
  }

  /**
   * Test comparing different polygon technologies using random
   * biased doubles.
   */
  @Test
  @Repeat(iterations = 10)
  public void testComparePolygons() {
    final PlanetModel planetModel = randomPlanetModel();
    //Create polygon points using a reference point and a maximum distance to the point
    final GeoPoint referencePoint = getBiasedPoint(planetModel);
    final int n = random().nextInt(4) + 4;
    final List<GeoPoint> points = new ArrayList<>(n);
    final double maxDistance = random().nextDouble() *  Math.PI;
    for (int i = 0; i < n; i++) {
      while(true) {
        final double distance = BiasedNumbers.randomDoubleBetween(random(), 0, maxDistance);// random().nextDouble() * maxDistance;
        final double bearing = random().nextDouble() * 2 * Math.PI;
        GeoPoint p = planetModel.surfacePointOnBearing(referencePoint, distance, bearing);
        if (!contains(p, points)) {
          if (points.size() > 1 && Plane.arePointsCoplanar(points.get(points.size() -1), points.get(points.size() - 2), p)) {
            continue;
          }
          points.add(p);
          break;
        }
      }
    }
    //order points so we don't get crossing edges
    final List<GeoPoint> orderedPoints = orderPoints(points);
    //Comment out below to get clock-wise polygons
    if (random().nextBoolean() && random().nextBoolean()) {
      Collections.reverse(orderedPoints);
    }
    GeoPolygonFactory.PolygonDescription polygonDescription = new GeoPolygonFactory.PolygonDescription(orderedPoints);
    GeoPolygon polygon = null;
    try {
      polygon = GeoPolygonFactory.makeGeoPolygon(planetModel, polygonDescription);
    } catch(Exception e) {
      StringBuilder buffer = new StringBuilder("Polygon failed to build with an exception:\n");
      buffer.append(points.toString()+ "\n");
      buffer.append("WKT:" + getWKT(orderedPoints));
      buffer.append(e.toString());
      fail(buffer.toString());
    }
    if (polygon == null) {
      StringBuilder buffer = new StringBuilder("Polygon failed to build:\n");
      buffer.append(points.toString()+ "\n");
      buffer.append("WKT:" + getWKT(orderedPoints));
      fail(buffer.toString());
    }
    GeoPolygon largePolygon = null;
    try {
      largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(planetModel, Collections.singletonList(polygonDescription));
    } catch(Exception e) {
      StringBuilder buffer = new StringBuilder("Large polygon failed to build with an exception:\n");
      buffer.append(points.toString()+ "\n");
      buffer.append("WKT:" + getWKT(orderedPoints));
      buffer.append(e.toString());
      fail(buffer.toString());
    }
    if (largePolygon == null) {
      StringBuilder buffer = new StringBuilder("Large polygon failed to build:\n");
      buffer.append(points.toString()+ "\n");
      buffer.append("WKT:" + getWKT(orderedPoints));
      fail(buffer.toString());
    }

    for(int i=0;i<100000;i++) {
      GeoPoint point = getBiasedPoint(planetModel);
      boolean withIn1 = polygon.isWithin(point);
      boolean withIn2 = largePolygon.isWithin(point);
      StringBuilder buffer = new StringBuilder();
      if (withIn1 != withIn2) {
        //NOTE: Sometimes we get errors when check point is near a polygon point.
        // For the time being, we filter this errors.
        double d1 = polygon.computeOutsideDistance(DistanceStyle.ARC, point);
        double d2  = largePolygon.computeOutsideDistance(DistanceStyle.ARC, point);
        if (d1 == 0 && d2 == 0) {
          continue;
        }
        buffer = buffer.append("\nStandard polygon: " + polygon.toString() +"\n");
        buffer = buffer.append("\nLarge polygon: " + largePolygon.toString() +"\n");
        buffer = buffer.append("\nPoint: " + point.toString() +"\n");
        buffer.append("\nWKT: " + getWKT(orderedPoints));
        buffer.append("\nWKT: POINT(" + toDegrees(point.getLongitude()) + " " + toDegrees(point.getLatitude()) + ")\n");
        buffer.append("normal polygon: " +withIn1 + "\n");
        buffer.append("large polygon: " + withIn2 + "\n");
      }
      assertTrue(buffer.toString(), withIn1 == withIn2);
    }
    //Not yet tested
//    for(int i=0;i<100;i++) {
//      GeoShape shape = randomGeoShape(randomShapeType(), planetModel);
//      int rel1 = polygon.getRelationship(shape);
//      int rel2 = largePolygon.getRelationship(shape);
//      StringBuilder buffer = new StringBuilder();
//      if (rel1 != rel2) {
//        buffer = buffer.append(polygon.toString() +"\n" + shape.toString() + "\n");
//        buffer.append("WKT: " + getWKT(orderedPoints) + "\n");
//        buffer.append("normal polygon: " + rel1 + "\n");
//        buffer.append("large polygon: " + rel2 + "\n");
//      }
//      assertTrue(buffer.toString(), rel1 == rel2);
//    }
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
      buffer.append(toDegrees(point.getLongitude()) + " " + toDegrees(point.getLatitude()) + ",");
    }
    buffer.append(toDegrees(points.get(0).getLongitude()) + " " + toDegrees(points.get(0).getLatitude()) + "))\n");
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
  
  final private static double DEGREES_PER_RADIAN = 180.0 / Math.PI;
  final private static double RADIANS_PER_DEGREE = Math.PI / 180.0;

  /** Converts radians to degrees */
  private static double toDegrees(final double radians) {
    return radians * DEGREES_PER_RADIAN;
  }

  /** Converts radians to degrees */
  private static double fromDegrees(final double degrees) {
    return degrees * RADIANS_PER_DEGREE;
  }

}
