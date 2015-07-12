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

public class GeoPolygonTest {


  @Test
  public void testPolygonPointWithin() {
    GeoMembershipShape c;
    GeoPoint gp;
    List<GeoPoint> points;

    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points, 0);
    // Sample some points within
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.55);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.45);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.05, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, -0.5);
    assertTrue(c.isWithin(gp));
    // Sample some nearby points outside
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.65);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.35);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.15, -0.5);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.15, -0.5);
    assertFalse(c.isWithin(gp));
    // Random points outside
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.0);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertFalse(c.isWithin(gp));

    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.01, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.7));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.8));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.7));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.01, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
        
        /*
        System.out.println("Points: ");
        for (GeoPoint p : points) {
            System.out.println(" "+p);
        }
        */

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points, 0);
    // Sample some points within
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.55);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.45);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.05, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.05, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.7);
    assertTrue(c.isWithin(gp));
    // Sample some nearby points outside
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.35);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.15, -0.5);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.15, -0.5);
    assertFalse(c.isWithin(gp));
    // Random points outside
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.0);
    assertFalse(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertFalse(c.isWithin(gp));

  }

  @Test
  public void testPolygonBounds() {
    GeoMembershipShape c;
    Bounds b;
    List<GeoPoint> points;

    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points, 0);

    b = c.getBounds(null);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.6, b.getLeftLongitude(), 0.000001);
    assertEquals(-0.4, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
  }

}
