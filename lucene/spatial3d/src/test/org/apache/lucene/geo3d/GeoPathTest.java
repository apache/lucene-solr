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

import static java.lang.Math.toRadians;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoPathTest {

  @Test
  public void testPathDistance() {
    // Start with a really simple case
    GeoPath p;
    GeoPoint gp;
    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    p.addPoint(0.0, 0.0);
    p.addPoint(0.0, 0.1);
    p.addPoint(0.0, 0.2);
    p.done();
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.15);
    assertEquals(Double.MAX_VALUE, p.computeDistance(DistanceStyle.ARC,gp), 0.0);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, 0.15);
    assertEquals(0.15 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.12);
    assertEquals(0.12 + 0.0, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.15, 0.05);
    assertEquals(Double.MAX_VALUE, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.25);
    assertEquals(0.20 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.05);
    assertEquals(0.0 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);

    // Compute path distances now
    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    p.addPoint(0.0, 0.0);
    p.addPoint(0.0, 0.1);
    p.addPoint(0.0, 0.2);
    p.done();
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, 0.15);
    assertEquals(0.15 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.12);
    assertEquals(0.12, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);

    // Now try a vertical path, and make sure distances are as expected
    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    p.addPoint(-Math.PI * 0.25, -0.5);
    p.addPoint(Math.PI * 0.25, -0.5);
    p.done();
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertEquals(Double.MAX_VALUE, p.computeDistance(DistanceStyle.ARC,gp), 0.0);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -1.0);
    assertEquals(Double.MAX_VALUE, p.computeDistance(DistanceStyle.ARC,gp), 0.0);
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.25 + 0.05, -0.5);
    assertEquals(Math.PI * 0.5 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.25 - 0.05, -0.5);
    assertEquals(0.0 + 0.05, p.computeDistance(DistanceStyle.ARC,gp), 0.000001);
  }

  @Test
  public void testPathPointWithin() {
    // Tests whether we can properly detect whether a point is within a path or not
    GeoPath p;
    GeoPoint gp;
    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    // Build a diagonal path crossing the equator
    p.addPoint(-0.2, -0.2);
    p.addPoint(0.2, 0.2);
    p.done();
    // Test points on the path
    gp = new GeoPoint(PlanetModel.SPHERE, -0.2, -0.2);
    assertTrue(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertTrue(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, 0.1);
    assertTrue(p.isWithin(gp));
    // Test points off the path
    gp = new GeoPoint(PlanetModel.SPHERE, -0.2, 0.2);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.2, -0.2);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertFalse(p.isWithin(gp));
    // Repeat the test, but across the terminator
    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    // Build a diagonal path crossing the equator
    p.addPoint(-0.2, Math.PI - 0.2);
    p.addPoint(0.2, -Math.PI + 0.2);
    p.done();
    // Test points on the path
    gp = new GeoPoint(PlanetModel.SPHERE, -0.2, Math.PI - 0.2);
    assertTrue(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertTrue(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, -Math.PI + 0.1);
    assertTrue(p.isWithin(gp));
    // Test points off the path
    gp = new GeoPoint(PlanetModel.SPHERE, -0.2, -Math.PI + 0.2);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.2, Math.PI - 0.2);
    assertFalse(p.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertFalse(p.isWithin(gp));

  }

  @Test
  public void testGetRelationship() {
    GeoArea rect;
    GeoPath p;

    // Start by testing the basic kinds of relationship, increasing in order of difficulty.

    p = new GeoPath(PlanetModel.SPHERE, 0.1);
    p.addPoint(-0.3, -0.3);
    p.addPoint(0.3, 0.3);
    p.done();
    // Easiest: The path is wholly contains the georect
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.05, -0.05, -0.05, 0.05);
    assertEquals(GeoArea.CONTAINS, rect.getRelationship(p));
    // Next easiest: Some endpoints of the rectangle are inside, and some are outside.
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.05, -0.05, -0.05, 0.5);
    assertEquals(GeoArea.OVERLAPS, rect.getRelationship(p));
    // Now, all points are outside, but the figures intersect
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.05, -0.05, -0.5, 0.5);
    assertEquals(GeoArea.OVERLAPS, rect.getRelationship(p));
    // Finally, all points are outside, and the figures *do not* intersect
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.5, -0.5, -0.5, 0.5);
    assertEquals(GeoArea.WITHIN, rect.getRelationship(p));
    // Check that segment edge overlap detection works
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.1, 0.0, -0.1, 0.0);
    assertEquals(GeoArea.OVERLAPS, rect.getRelationship(p));
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.2, 0.1, -0.2, -0.1);
    assertEquals(GeoArea.DISJOINT, rect.getRelationship(p));
    // Check if overlap at endpoints behaves as expected next
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.5, -0.5, -0.5, -0.35);
    assertEquals(GeoArea.OVERLAPS, rect.getRelationship(p));
    rect = new GeoRectangle(PlanetModel.SPHERE, 0.5, -0.5, -0.5, -0.45);
    assertEquals(GeoArea.DISJOINT, rect.getRelationship(p));

  }

  @Test
  public void testPathBounds() {
    GeoPath c;
    Bounds b;

    c = new GeoPath(PlanetModel.SPHERE, 0.1);
    c.addPoint(-0.3, -0.3);
    c.addPoint(0.3, 0.3);
    c.done();

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.4046919, b.getLeftLongitude(), 0.000001);
    assertEquals(0.4046919, b.getRightLongitude(), 0.000001);
    assertEquals(-0.3999999, b.getMinLatitude(), 0.000001);
    assertEquals(0.3999999, b.getMaxLatitude(), 0.000001);

  }

  @Test
  public void testCoLinear() {
    // p1: (12,-90), p2: (11, -55), (129, -90)
    GeoPath p = new GeoPath(PlanetModel.SPHERE, 0.1);
    p.addPoint(toRadians(-90), toRadians(12));//south pole
    p.addPoint(toRadians(-55), toRadians(11));
    p.addPoint(toRadians(-90), toRadians(129));//south pole again
    p.done();//at least test this doesn't bomb like it used too -- LUCENE-6520
  }

}
