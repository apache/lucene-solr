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
    Bounds b = c.getBounds(null);
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
    Bounds b;


    // Vertical circle cases
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.6, b.getLeftLongitude(), 0.000001);
    assertEquals(-0.4, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.5, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.4, b.getLeftLongitude(), 0.000001);
    assertEquals(0.6, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    // Horizontal circle cases
    c = new GeoCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI * 0.5 - 0.1, b.getMinLatitude(), 0.000001);
    c = new GeoCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    b = c.getBounds(null);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    assertEquals(-Math.PI * 0.5 + 0.1, b.getMaxLatitude(), 0.000001);

    // Now do a somewhat tilted plane, facing different directions.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, 0.0, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.5, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.5, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    // Slightly tilted, PI/4 direction.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.25, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.25, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, -0.01, Math.PI * 0.25, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = new GeoCircle(PlanetModel.SPHERE, -0.01, -Math.PI * 0.25, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    // Now do a somewhat tilted plane.
    c = new GeoCircle(PlanetModel.SPHERE, 0.01, -0.5, 0.1);
    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.6, b.getLeftLongitude(), 0.00001);
    assertEquals(-0.4, b.getRightLongitude(), 0.00001);

  }

}
