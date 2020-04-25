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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.junit.Test;

/**
 * Tests for GeoExactCircle.
 */
public class GeoExactCircleTest extends RandomGeo3dShapeGenerator{

  @Test
  public void testExactCircle() {
    GeoCircle c;
    GeoPoint gp;

    // Construct a variety of circles to see how many actual planes are involved
    c = new GeoExactCircle(PlanetModel.WGS84, 0.0, 0.0, 0.1, 1e-6);
    gp = new GeoPoint(PlanetModel.WGS84, 0.0, 0.2);
    assertTrue(!c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.WGS84, 0.0, 0.0);
    assertTrue(c.isWithin(gp));

    c = new GeoExactCircle(PlanetModel.WGS84, 0.1, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.2, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.3, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.4, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, Math.PI * 0.5, 0.0, 0.1, 1e-6);
    gp = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5 - 0.2, 0.0);
    assertTrue(!c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5, 0.0);
    assertTrue(c.isWithin(gp));
  }

  @Test
  public void testSurfacePointOnBearingScale(){
    PlanetModel p1 = PlanetModel.WGS84;
    PlanetModel p2 = new PlanetModel(0.5 * PlanetModel.WGS84.xyScaling, 0.5 * PlanetModel.WGS84.zScaling);
    GeoPoint point1P1 = new GeoPoint(p1, 0, 0);
    GeoPoint point2P1 =  new GeoPoint(p1, 1, 1);
    GeoPoint point1P2 = new GeoPoint(p2, point1P1.getLatitude(), point1P1.getLongitude());
    GeoPoint point2P2 = new GeoPoint(p2, point2P1.getLatitude(), point2P1.getLongitude());

    double dist =  0.2* Math.PI;
    double bearing = 0.2 * Math.PI;

    GeoPoint new1 = p1.surfacePointOnBearing(point2P1, dist, bearing);
    GeoPoint new2 = p2.surfacePointOnBearing(point2P2, dist, bearing);

    assertEquals(new1.getLatitude(), new2.getLatitude(), 1e-12);
    assertEquals(new1.getLongitude(), new2.getLongitude(), 1e-12);
    //This is true if surfaceDistance return results always in radians
    double d1 = p1.surfaceDistance(point1P1, point2P1);
    double d2 = p2.surfaceDistance(point1P2, point2P2);
    assertEquals(d1, d2, 1e-12);
  }

  @Test
  @Repeat(iterations = 100)
  public void RandomPointBearingWGS84Test(){
    PlanetModel planetModel = PlanetModel.WGS84;
    RandomGeo3dShapeGenerator generator = new RandomGeo3dShapeGenerator();
    GeoPoint center = generator.randomGeoPoint(planetModel);
    double radius = random().nextDouble() * Math.PI;
    checkBearingPoint(planetModel, center, radius, 0);
    checkBearingPoint(planetModel, center, radius, 0.5 * Math.PI);
    checkBearingPoint(planetModel, center, radius, Math.PI);
    checkBearingPoint(planetModel, center, radius, 1.5 * Math.PI);
  }

  @Test
  @Repeat(iterations = 100)
  public void RandomPointBearingCardinalTest(){
    //surface distance calculations methods start not converging when
    //planet scaledFlattening > 0.4
    PlanetModel planetModel;
    do {
      double ab = random().nextDouble() * 2;
      double c = random().nextDouble() * 2;
      if (random().nextBoolean()) {
        planetModel = new PlanetModel(ab, c);
      } else {
        planetModel = new PlanetModel(c, ab);
      }
    } while (Math.abs(planetModel.scaledFlattening) > 0.4);
    GeoPoint center = randomGeoPoint(planetModel);
    double radius =  random().nextDouble() * 0.9 * planetModel.minimumPoleDistance;
    checkBearingPoint(planetModel, center, radius, 0);
    checkBearingPoint(planetModel, center, radius, 0.5 * Math.PI);
    checkBearingPoint(planetModel, center, radius, Math.PI);
    checkBearingPoint(planetModel, center, radius, 1.5 * Math.PI);
  }

  private void checkBearingPoint(PlanetModel planetModel, GeoPoint center, double radius, double bearingAngle) {
    GeoPoint point = planetModel.surfacePointOnBearing(center, radius, bearingAngle);
    double surfaceDistance = planetModel.surfaceDistance(center, point);
    assertTrue(planetModel.toString() + " " + Double.toString(surfaceDistance - radius) + " " + Double.toString(radius), surfaceDistance - radius < Vector.MINIMUM_ANGULAR_RESOLUTION);
  }

  @Test
  public void testExactCircleBounds() {

    GeoPoint center = new GeoPoint(PlanetModel.WGS84, 0, 0);
    // Construct four cardinal points, and then we'll build the first two planes
    final GeoPoint northPoint = PlanetModel.WGS84.surfacePointOnBearing(center, 1, 0.0);
    final GeoPoint southPoint = PlanetModel.WGS84.surfacePointOnBearing(center, 1, Math.PI);
    final GeoPoint eastPoint = PlanetModel.WGS84.surfacePointOnBearing(center, 1, Math.PI * 0.5);
    final GeoPoint westPoint = PlanetModel.WGS84.surfacePointOnBearing(center, 1, Math.PI * 1.5);

    GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0, 0, 1, 1e-6);
    LatLonBounds bounds = new LatLonBounds();
    circle.getBounds(bounds);
    assertEquals(northPoint.getLatitude(), bounds.getMaxLatitude(), 1e-2);
    assertEquals(southPoint.getLatitude(), bounds.getMinLatitude(), 1e-2);
    assertEquals(westPoint.getLongitude(), bounds.getLeftLongitude(), 1e-2);
    assertEquals(eastPoint.getLongitude(), bounds.getRightLongitude(), 1e-2);
  }

  @Test
  public void exactCircleLargeTest(){
    boolean success = true;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(new PlanetModel(0.99, 1.05), 0.25 * Math.PI,  0,0.35 * Math.PI, 1e-12);
    } catch (IllegalArgumentException e) {
      success = false;
    }
    assertTrue(success);
    success = false;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0.25 * Math.PI,  0,0.9996 * Math.PI, 1e-12);
    } catch (IllegalArgumentException e) {
      success = true;
    }
    assertTrue(success);
  }

  @Test
  public void testExactCircleDoesNotFit() {
    boolean exception = false;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 1.5633796542562415, -1.0387149580695152,3.1409865861032844, 1e-12);
    } catch (IllegalArgumentException e) {
      exception = true;
    }
    assertTrue(exception);
  }

  public void testBigCircleInSphere() {
    //In Planet model Sphere if circle is close to Math.PI we can get the situation where
    //circle slice planes are bigger than half of a hemisphere. We need to make
    //sure we divide the circle in at least 4 slices.
    GeoCircle circle1 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.SPHERE, 1.1306735252307394, -0.7374283438171261, 3.1415760537549234, 4.816939220262406E-12);
    GeoPoint point = new GeoPoint(PlanetModel.SPHERE, -1.5707963267948966, 0.0);
    assertTrue(circle1.isWithin(point));
  }

  /**
   * in LUCENE-8054 we have problems with exact circles that have
   * edges that are close together. This test creates those circles with the same
   * center and slightly different radius.
   */
  @Test
  @Repeat(iterations = 100)
  public void testRandomLUCENE8054() {
    PlanetModel planetModel = randomPlanetModel();
    GeoCircle circle1 = (GeoCircle) randomGeoAreaShape(EXACT_CIRCLE, planetModel);
    // new radius, a bit smaller than the generated one!
    double radius = circle1.getRadius() *  (1 - 0.01 * random().nextDouble());
    //circle with same center and new radius
    GeoCircle circle2 = GeoCircleFactory.makeExactGeoCircle(planetModel,
        circle1.getCenter().getLatitude(),
        circle1.getCenter().getLongitude(),
        radius, 1e-5 );
    StringBuilder b = new StringBuilder();
    b.append("circle1: " + circle1 + "\n");
    b.append("circle2: " + circle2);
    //It cannot be disjoint, same center!
    assertTrue(b.toString(), circle1.getRelationship(circle2) != GeoArea.DISJOINT);
  }

  @Test
  public void testLUCENE8054(){
    GeoCircle circle1 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, -1.0394053553992673, -1.9037325881389144, 1.1546166170607672, 4.231100485201301E-4);
    GeoCircle circle2 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, -1.3165961602008989, -1.887137823746273, 1.432516663588956, 3.172052880854355E-4);
    // Relationship between circles must be different than DISJOINT as centers are closer than the radius.
    int rel = circle1.getRelationship(circle2);
    assertTrue(rel != GeoArea.DISJOINT);
  }

  @Test
  public void testLUCENE8056(){
    GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0.647941905154693, 0.8542472362428436, 0.8917883700569315, 1.2173787103955335E-8);
    GeoBBox bBox = GeoBBoxFactory.makeGeoBBox(PlanetModel.WGS84, 0.5890486225480862, 0.4908738521234052, 1.9634954084936207, 2.159844949342983);
    //Center iis out of the shape
    assertFalse(circle.isWithin(bBox.getCenter()));
    //Edge point is in the shape
    assertTrue(circle.isWithin(bBox.getEdgePoints()[0]));
    //Shape should intersect!!!
    assertTrue(bBox.getRelationship(circle) == GeoArea.OVERLAPS);
  }

  @Test
  public void testExactCircleLUCENE8054() {
    // [junit4]    > Throwable #1: java.lang.AssertionError: circle1: GeoExactCircle:
    // {planetmodel=PlanetModel.WGS84, center=[lat=-1.2097332228999564, lon=0.749061883738567([X=0.25823775418663625, Y=0.2401212674846636, Z=-0.9338185278804293])],
    //  radius=0.20785254459485322(11.909073566339822), accuracy=6.710701666727661E-9}
    // [junit4]    > circle2: GeoExactCircle: {planetmodel=PlanetModel.WGS84, center=[lat=-1.2097332228999564, lon=0.749061883738567([X=0.25823775418663625, Y=0.2401212674846636, Z=-0.9338185278804293])],
    // radius=0.20701584142315682(11.861134005896407), accuracy=1.0E-5}
    final GeoCircle c1 = new GeoExactCircle(PlanetModel.WGS84, -1.2097332228999564, 0.749061883738567, 0.20785254459485322, 6.710701666727661E-9);
    final GeoCircle c2 = new GeoExactCircle(PlanetModel.WGS84, -1.2097332228999564, 0.749061883738567, 0.20701584142315682, 1.0E-5);
    assertTrue("cannot be disjoint", c1.getRelationship(c2) != GeoArea.DISJOINT);
  }

  @Test
  public void testLUCENE8065(){
    //Circle planes are convex
    GeoCircle circle1 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0.03186456479560385, -2.2254294002683617, 1.5702573535090856, 8.184299676008562E-6);
    GeoCircle circle2 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0.03186456479560385, -2.2254294002683617 , 1.5698163157923914, 1.0E-5);
    assertTrue(circle1.getRelationship(circle2) != GeoArea.DISJOINT);
  }

  public void testLUCENE8080() {
    PlanetModel planetModel = new PlanetModel(1.6304230055804751, 1.0199671157571204);
    boolean fail = false;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(planetModel, 0.8853814403571284, 0.9784990176851283, 0.9071033527030907, 1e-11);
    } catch (IllegalArgumentException e) {
      fail = true;
    }
    assertTrue(fail);
  }

}
