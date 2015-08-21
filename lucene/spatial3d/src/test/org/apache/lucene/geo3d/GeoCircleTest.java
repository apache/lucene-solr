package org.apache.lucene.geo3d;

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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoCircleTest {

  @Test
  public void testCircleDistance() {
    GeoCircle c;
    GeoPoint gp;
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertEquals(Double.MAX_VALUE, c.computeDistance(DistanceStyle.ARC,gp), 0.0);
    assertEquals(Double.MAX_VALUE, c.computeDistance(DistanceStyle.NORMAL,gp), 0.0);
    assertEquals(Double.MAX_VALUE, c.computeDistance(DistanceStyle.NORMAL,gp), 0.0);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertEquals(0.0, c.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    assertEquals(0.0, c.computeDistance(DistanceStyle.NORMAL,gp), 0.000001);
    assertEquals(0.0, c.computeDistance(DistanceStyle.NORMAL,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, -0.5);
    assertEquals(0.05, c.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    assertEquals(0.049995, c.computeDistance(DistanceStyle.LINEAR,gp), 0.000001);
    assertEquals(0.049979, c.computeDistance(DistanceStyle.NORMAL,gp), 0.000001);
  }

  @Test
  public void testCircleFullWorld() {
    GeoCircle c;
    GeoPoint gp;
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, -0.5, Math.PI);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.55);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.45);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.0);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertTrue(c.isWithin(gp));
    LatLonBounds b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
  }

  @Test
  public void testCirclePointWithin() {
    GeoCircle c;
    GeoPoint gp;
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertFalse(c.isWithin(gp));
    assertEquals(0.4,c.computeOutsideDistance(DistanceStyle.ARC,gp),1e-12);
    assertEquals(0.12,c.computeOutsideDistance(DistanceStyle.NORMAL,gp),0.01);
    assertEquals(0.4,c.computeOutsideDistance(DistanceStyle.LINEAR,gp),0.01);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.55);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.45);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.0);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertFalse(c.isWithin(gp));
  }

  @Test
  public void testCircleBounds() {
    GeoCircle c;
    LatLonBounds b;
    XYZBounds xyzb;
    GeoArea area;
    GeoPoint p1;
    GeoPoint p2;

    // Fifth BKD discovered failure
    c = new GeoCircle(PlanetModel.SPHERE, -0.004282454525970269, -1.6739831367422277E-4, 1.959639723134033E-6);
    assertTrue(c.isWithin(c.getEdgePoints()[0]));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(GeoArea.WITHIN == area.getRelationship(c) || GeoArea.OVERLAPS == area.getRelationship(c));
    
    // Fourth BKD discovered failure
    c = new GeoCircle(PlanetModel.SPHERE, -0.0048795517261255, 0.004053904306995974, 5.93699764258874E-6);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(GeoArea.WITHIN == area.getRelationship(c) || GeoArea.OVERLAPS == area.getRelationship(c));
    
    // Yet another test case from BKD
    c = new GeoCircle(PlanetModel.WGS84, 0.006229478708446979, 0.005570196723795424, 3.840276763694387E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    p1 = new GeoPoint(PlanetModel.WGS84, 0.006224927111830945, 0.005597367237251763);
    p2 = new GeoPoint(1.0010836083810235, 0.005603490759433942, 0.006231850560862502);
    assertTrue(PlanetModel.WGS84.pointOnSurface(p1));
    //assertTrue(PlanetModel.WGS84.pointOnSurface(p2));
    assertTrue(c.isWithin(p1));
    assertTrue(c.isWithin(p2));
    assertTrue(area.isWithin(p1));
    assertTrue(area.isWithin(p2));
    
    // Another test case from BKD
    c = new GeoCircle(PlanetModel.SPHERE, -0.005955031040627789, -0.0029274772647399153, 1.601488279374338E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    
    int relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.WITHIN || relationship == GeoArea.OVERLAPS);
    
    // Test case from BKD
    c = new GeoCircle(PlanetModel.SPHERE, -0.765816119338, 0.991848766844, 0.8153163226330487);
    p1 = new GeoPoint(0.7692262265236023, -0.055089298115534646, -0.6365973465711254);
    assertTrue(c.isWithin(p1));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertTrue(p1.x >= xyzb.getMinimumX() && p1.x <= xyzb.getMaximumX());
    assertTrue(p1.y >= xyzb.getMinimumY() && p1.y <= xyzb.getMaximumY());
    assertTrue(p1.z >= xyzb.getMinimumZ() && p1.z <= xyzb.getMaximumZ());
    
    // Vertical circle cases
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.6, b.getLeftLongitude(), 0.000001);
    assertEquals(-0.4, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.4, b.getLeftLongitude(), 0.000001);
    assertEquals(0.6, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    // Horizontal circle cases
    c = new GeoCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI * 0.5 - 0.1, b.getMinLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    assertEquals(-Math.PI * 0.5 + 0.1, b.getMaxLatitude(), 0.000001);

    // Now do a somewhat tilted plane, facing different directions.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    // Slightly tilted, PI/4 direction.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, -0.01, Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, -0.01, -Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    // Now do a somewhat tilted plane.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.6, b.getLeftLongitude(), 0.00001);
    assertEquals(-0.4, b.getRightLongitude(), 0.00001);

  }

}
