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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoBBoxTest {

  protected static final double DEGREES_TO_RADIANS = Math.PI / 180.0;

  @Test
  public void testBBoxDegenerate() {
    GeoBBox box;
    GeoConvexPolygon cp;
    int relationship;
    List<GeoPoint> points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -49 * DEGREES_TO_RADIANS, -176 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -11 * DEGREES_TO_RADIANS, 101 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, 24 * DEGREES_TO_RADIANS, -30 * DEGREES_TO_RADIANS));
    GeoMembershipShape shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, -64 * DEGREES_TO_RADIANS, -64 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS, 180 * DEGREES_TO_RADIANS);
    relationship = box.getRelationship(shape);
    assertEquals(GeoArea.CONTAINS, relationship);
    box = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, -61.85 * DEGREES_TO_RADIANS, -67.5 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS, -168.75 * DEGREES_TO_RADIANS);
    //System.out.println("Shape = " + shape + " Rect = " + box);
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
    LatLonBounds b;
    XYZBounds xyzb;
    GeoArea solid;
    GeoPoint point;
    int relationship;
    
    c= GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.7570958596622309, -0.7458670829264561, -0.9566079379002148, 1.4802570961901191);
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,0.10922258701604912,0.1248184603754517,-0.8172414690802067,0.9959041483215542,-0.6136586624726926,0.6821740363641521);
    point = new GeoPoint(PlanetModel.SPHERE, 0.3719987557178081, 1.4529582778845198);
    assertTrue(c.isWithin(point));
    assertTrue(solid.isWithin(point));
    relationship = solid.getRelationship(c);
    assertTrue(relationship == GeoArea.OVERLAPS || relationship == GeoArea.CONTAINS || relationship == GeoArea.WITHIN);

    c= GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.006607096847842122, -0.002828135860810422, -0.0012934461873348349, 0.006727418645092394);
    solid = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,0.9999995988328008,1.0000000002328306,-0.0012934708508166816,0.006727393021214471,-0.002828157275369464,0.006607074060760007);
    point = new GeoPoint(PlanetModel.SPHERE, -5.236470872437899E-4, 3.992578692654256E-4);
    assertTrue(c.isWithin(point));
    assertTrue(solid.isWithin(point));
    relationship = solid.getRelationship(c);
    assertTrue(relationship == GeoArea.OVERLAPS || relationship == GeoArea.CONTAINS || relationship == GeoArea.WITHIN);
    
    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.25, -Math.PI * 0.25, -1.0, 1.0);
    b = new LatLonBounds();
    c.getBounds(b);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-1.0, b.getLeftLongitude(), 0.000001);
    assertEquals(1.0, b.getRightLongitude(), 0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25, b.getMaxLatitude(), 0.000001);
    assertEquals(0.382051, xyzb.getMinimumX(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumX(), 0.000001);
    assertEquals(-0.841471, xyzb.getMinimumY(), 0.000001);
    assertEquals(0.841471, xyzb.getMaximumY(), 0.000001);
    assertEquals(-0.707107, xyzb.getMinimumZ(), 0.000001);
    assertEquals(0.707107, xyzb.getMaximumZ(), 0.000001);
    
    GeoArea area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX() - 2.0 * Vector.MINIMUM_RESOLUTION,
      xyzb.getMaximumX() + 2.0 * Vector.MINIMUM_RESOLUTION,
      xyzb.getMinimumY() - 2.0 * Vector.MINIMUM_RESOLUTION,
      xyzb.getMaximumY() + 2.0 * Vector.MINIMUM_RESOLUTION,
      xyzb.getMinimumZ() - 2.0 * Vector.MINIMUM_RESOLUTION,
      xyzb.getMaximumZ() + 2.0 * Vector.MINIMUM_RESOLUTION);
    assertEquals(GeoArea.WITHIN, area.getRelationship(c));

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -1.0, 1.0);
    b = new LatLonBounds();
    c.getBounds(b);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-1.0, b.getLeftLongitude(), 0.000001);
    assertEquals(1.0, b.getRightLongitude(), 0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);
    assertEquals(0.382051, xyzb.getMinimumX(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumX(), 0.000001);
    assertEquals(-0.841471, xyzb.getMinimumY(), 0.000001);
    assertEquals(0.841471, xyzb.getMaximumY(), 0.000001);
    assertEquals(-0.707107, xyzb.getMinimumZ(), 0.000001);
    assertEquals(0.0, xyzb.getMaximumZ(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, 1.0, -1.0);

    b = new LatLonBounds();
    c.getBounds(b);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    //assertEquals(1.0,b.getLeftLongitude(),0.000001);
    //assertEquals(-1.0,b.getRightLongitude(),0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);
    assertEquals(-1.0, xyzb.getMinimumX(), 0.000001);
    assertEquals(0.540303, xyzb.getMaximumX(), 0.000001);
    assertEquals(-1.0, xyzb.getMinimumY(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumY(), 0.000001);
    assertEquals(-0.707107, xyzb.getMinimumZ(), 0.000001);
    assertEquals(0.0, xyzb.getMaximumZ(), 0.000001);


    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -1.0, 1.0);

    b = new LatLonBounds();
    c.getBounds(b);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-1.0, b.getLeftLongitude(), 0.000001);
    //assertEquals(1.0, b.getRightLongitude(), 0.000001);
    assertEquals(0.0, xyzb.getMinimumX(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumX(), 0.000001);
    assertEquals(-0.841471, xyzb.getMinimumY(), 0.000001);
    assertEquals(0.841471, xyzb.getMaximumY(), 0.000001);
    assertEquals(-1.0, xyzb.getMinimumZ(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumZ(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 1.0, -1.0);

    b = new LatLonBounds();
    c.getBounds(b);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(1.0,b.getLeftLongitude(),0.000001);
    //assertEquals(-1.0,b.getRightLongitude(),0.000001);
    assertEquals(-1.0, xyzb.getMinimumX(), 0.000001);
    assertEquals(0.540303, xyzb.getMaximumX(), 0.000001);
    assertEquals(-1.0, xyzb.getMinimumY(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumY(), 0.000001);
    assertEquals(-1.0, xyzb.getMinimumZ(), 0.000001);
    assertEquals(1.0, xyzb.getMaximumZ(), 0.000001);

    // Check wide variants of rectangle and longitude slice

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, -Math.PI + 0.1, Math.PI - 0.1);

    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI+0.1,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI-0.1,b.getRightLongitude(),0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 0.0, -Math.PI * 0.25, Math.PI - 0.1, -Math.PI + 0.1);

    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-Math.PI * 0.25, b.getMinLatitude(), 0.000001);
    assertEquals(0.0, b.getMaxLatitude(), 0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI + 0.1, Math.PI - 0.1);

    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI+0.1,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI-0.1,b.getRightLongitude(),0.000001);

    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, Math.PI - 0.1, -Math.PI + 0.1);

    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    //assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);

    // Check latitude zone
    c = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, 1.0, -1.0, -Math.PI, Math.PI);

    b = new LatLonBounds();
    c.getBounds(b);
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

    b = new LatLonBounds();
    c1.getBounds(b);
    c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());

    c1 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI, 0.0);
    c2 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 0.0, Math.PI * 0.5);

    b = new LatLonBounds();
    c1.getBounds(b);
    c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI*0.5,b.getRightLongitude(),0.000001);

    c1 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, -Math.PI * 0.5, 0.0);
    c2 = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, Math.PI * 0.5, -Math.PI * 0.5, 0.0, Math.PI);

    b = new LatLonBounds();
    c1.getBounds(b);
    c2.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    //assertEquals(-Math.PI * 0.5,b.getLeftLongitude(),0.000001);
    //assertEquals(Math.PI,b.getRightLongitude(),0.000001);

  }
  
  @Test
  public void testFailureCase1() {
    final GeoPoint point = new GeoPoint(-0.017413370801260174, -2.132522881412925E-18, 0.9976113450663769);
    final GeoBBox box = new GeoNorthRectangle(PlanetModel.WGS84, 0.35451471030934045, 9.908337057950734E-15, 2.891004593509811E-11);
    final XYZBounds bounds = new XYZBounds();
    box.getBounds(bounds);
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(), bounds.getMinimumY(), bounds.getMaximumY(), bounds.getMinimumZ(), bounds.getMaximumZ());
    
    assertTrue(box.isWithin(point)?solid.isWithin(point):true);
  }
  
  @Test
  public void testFailureCase2() {
    //final GeoPoint point = new GeoPoint(-0.7375647084975573, -2.3309121299774915E-10, 0.6746626163258577);
    final GeoPoint point = new GeoPoint(-0.737564708579924, -9.032562595264542E-17, 0.6746626165197899);
    final GeoBBox box = new GeoRectangle(PlanetModel.WGS84, 0.7988584710911523, 0.25383311815493353, -1.2236144735575564E-12, 7.356011300929654E-49);
    final XYZBounds bounds = new XYZBounds();
    box.getBounds(bounds);
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(), bounds.getMinimumY(), bounds.getMaximumY(), bounds.getMinimumZ(), bounds.getMaximumZ());

    //System.out.println("Is within Y value? "+(point.y >= bounds.getMinimumY() && point.y <= bounds.getMaximumY()));
    //System.out.println("Shape = "+box+" is within? "+box.isWithin(point));
    //System.out.println("XYZBounds = "+bounds+" is within? "+solid.isWithin(point)+" solid="+solid);
    assertTrue(box.isWithin(point) == solid.isWithin(point));
  }
  
}
