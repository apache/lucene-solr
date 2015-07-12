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

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoBBoxTest {

  protected final double DEGREES_TO_RADIANS = Math.PI / 180.0;

  @Test
  public void testBBoxDegenerate() {
    GeoBBox box;
    GeoConvexPolygon cp;
    int relationship;
    List<GeoPoint> points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 24 * DEGREES_TO_RADIANS, -30 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -11 * DEGREES_TO_RADIANS, 101 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -49 * DEGREES_TO_RADIANS, -176 * DEGREES_TO_RADIANS));
    GeoMembershipShape shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points, 0);
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, -64 * DEGREES_TO_RADIANS, -64 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS, 180 * DEGREES_TO_RADIANS);
    relationship = box.getRelationship(shape);
    assertEquals(GeoArea.CONTAINS, relationship);
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, -61.85 * DEGREES_TO_RADIANS, -67.5 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS, -168.75 * DEGREES_TO_RADIANS);
    System.out.println("Shape = " + shape + " Rect = " + box);
    relationship = box.getRelationship(shape);
    assertEquals(GeoArea.CONTAINS, relationship);
  }

  @Test
  public void testBBoxPointWithin() {
    GeoBBox box;
    GeoPoint gp;

    // Standard normal Rect box, not crossing dateline
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -1.0, 1.0);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, 0.0);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, 0.0);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, 1.1);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -1.1);
    assertFalse(box.isWithin(gp));
    assertEquals(0.1,box.computeOutsideDistance(DistanceStyle.ARC,gp),1e-2);
    assertEquals(0.1,box.computeOutsideDistance(DistanceStyle.NORMAL,gp),1e-2);
    assertEquals(0.1,box.computeOutsideDistance(DistanceStyle.NORMAL,gp),1e-2);

    // Standard normal Rect box, crossing dateline
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, Math.PI - 1.0, -Math.PI + 1.0);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, -Math.PI);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, -Math.PI);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI + 1.1);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, (-Math.PI - 1.1) + Math.PI * 2.0);
    assertFalse(box.isWithin(gp));

    // Latitude zone rectangle
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -Math.PI, Math.PI);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, -Math.PI);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, -Math.PI);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI + 1.1);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, (-Math.PI - 1.1) + Math.PI * 2.0);
    assertTrue(box.isWithin(gp));

    // World
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI, Math.PI);
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.1, -Math.PI);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.5, -Math.PI);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -Math.PI + 1.1);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, (-Math.PI - 1.1) + Math.PI * 2.0);
    assertTrue(box.isWithin(gp));

  }

  @Test
  public void testBBoxExpand() {
    GeoBBox box;
    GeoPoint gp;
    // Standard normal Rect box, not crossing dateline
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -1.0, 1.0);
    box = box.expand(0.1);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, 0.0);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.15, 0.0);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.25 - 0.05, 0.0);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -Math.PI * 0.25 - 0.15, 0.0);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -1.05);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, -1.15);
    assertFalse(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, 1.05);
    assertTrue(box.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.1, 1.15);
    assertFalse(box.isWithin(gp));
  }

  @Test
  public void testBBoxBounds() {
    GeoBBox c;
    Bounds b;

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -1.0, 1.0);

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-1.0, b.getLeftLongitude(), 0.000001);
    assertEquals(1.0, b.getRightLongitude(), 0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, 1.0, -1.0);

    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    //assertEquals(1.0,b.getLeftLongitude(),0.000001);
    //assertEquals(-1.0,b.getRightLongitude(),0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -1.0, 1.0);

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    assertEquals(-1.0, b.getLeftLongitude(), 0.000001);
    assertEquals(1.0, b.getRightLongitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 1.0, -1.0);

    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(1.0,b.getLeftLongitude(),0.000001);
    //assertEquals(-1.0,b.getRightLongitude(),0.000001);

    // Check wide variants of rectangle and longitude slice

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -Math.PI + 0.1, Math.PI - 0.1);

    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI+0.1,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI-0.1,b.getRightLongitude(),0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, Math.PI - 0.1, -Math.PI + 0.1);

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI + 0.1, Math.PI - 0.1);

    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI+0.1,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI-0.1,b.getRightLongitude(),0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, Math.PI - 0.1, -Math.PI + 0.1);

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);

    // Check latitude zone
    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 1.0, -1.0, -Math.PI, Math.PI);

    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-1.0, b.getMinLatitude(), 0.000001);
    assertEquals(1.0, b.getMaxLatitude(), 0.000001);

    // Now, combine a few things to test the bounds object
    GeoBBox c1;
    GeoBBox c2;

    c1 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI, 0.0);
    c2 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 0.0, Math.PI);

    b = new Bounds();
    b = c1.getBounds(b);
    b = c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());

    c1 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI, 0.0);
    c2 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 0.0, Math.PI * 0.5);

    b = new Bounds();
    b = c1.getBounds(b);
    b = c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI*0.5,b.getRightLongitude(),0.000001);

    c1 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI * 0.5, 0.0);
    c2 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 0.0, Math.PI);

    b = new Bounds();
    b = c1.getBounds(b);
    b = c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI * 0.5,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI,b.getRightLongitude(),0.000001);

  }

}
