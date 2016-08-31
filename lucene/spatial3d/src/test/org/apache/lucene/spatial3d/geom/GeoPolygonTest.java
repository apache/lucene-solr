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
import java.util.BitSet;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GeoPolygonTest {

  @Test
  public void testPolygonPointFiltering() {
    final GeoPoint point1 = new GeoPoint(PlanetModel.WGS84, 1.0, 2.0);
    final GeoPoint point2 = new GeoPoint(PlanetModel.WGS84, 0.5, 2.5);
    final GeoPoint point3 = new GeoPoint(PlanetModel.WGS84, 0.0, 0.0);
    final GeoPoint point4 = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5, 0.0);
    final GeoPoint point5 = new GeoPoint(PlanetModel.WGS84, 1.0, 0.0);
    
    // First: duplicate points in the middle
    {
      final List<GeoPoint> originalPoints = new ArrayList<>();
      originalPoints.add(point1);
      originalPoints.add(point2);
      originalPoints.add(point2);
      originalPoints.add(point3);
      final List<GeoPoint> filteredPoints = GeoPolygonFactory.filterEdges(GeoPolygonFactory.filterPoints(originalPoints), 0.0);
      assertEquals(3, filteredPoints.size());
      assertEquals(point1, filteredPoints.get(0));
      assertEquals(point2, filteredPoints.get(1));
      assertEquals(point3, filteredPoints.get(2));
    }
    // Next, duplicate points at the beginning
    {
      final List<GeoPoint> originalPoints = new ArrayList<>();
      originalPoints.add(point2);
      originalPoints.add(point1);
      originalPoints.add(point3);
      originalPoints.add(point2);
      final List<GeoPoint> filteredPoints = GeoPolygonFactory.filterEdges(GeoPolygonFactory.filterPoints(originalPoints), 0.0);
      assertEquals(3, filteredPoints.size());
      assertEquals(point2, filteredPoints.get(0));
      assertEquals(point1, filteredPoints.get(1));
      assertEquals(point3, filteredPoints.get(2));
    }

    // Coplanar point removal
    {
      final List<GeoPoint> originalPoints = new ArrayList<>();
      originalPoints.add(point1);
      originalPoints.add(point3);
      originalPoints.add(point4);
      originalPoints.add(point5);
      final List<GeoPoint> filteredPoints = GeoPolygonFactory.filterEdges(GeoPolygonFactory.filterPoints(originalPoints), 0.0);
      assertEquals(3, filteredPoints.size());
      assertEquals(point1, filteredPoints.get(0));
      assertEquals(point3, filteredPoints.get(1));
      assertEquals(point5, filteredPoints.get(2));
    }
    // Over the boundary
    {
      final List<GeoPoint> originalPoints = new ArrayList<>();
      originalPoints.add(point5);
      originalPoints.add(point1);
      originalPoints.add(point3);
      originalPoints.add(point4);
      final List<GeoPoint> filteredPoints = GeoPolygonFactory.filterEdges(GeoPolygonFactory.filterPoints(originalPoints), 0.0);
      assertEquals(3, filteredPoints.size());
      assertEquals(point5, filteredPoints.get(0));
      assertEquals(point1, filteredPoints.get(1));
      assertEquals(point3, filteredPoints.get(2));
    }

  }
  
  @Test
  public void testPolygonClockwise() {
    GeoPolygon c;
    GeoPoint gp;
    List<GeoPoint> points;
    List<GeoPolygonFactory.PolygonDescription> shapes;

    // Points go counterclockwise, so 
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    //System.out.println(c);
    
    // Middle point should NOT be within!!
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(!c.isWithin(gp));

    shapes = new ArrayList<>();
    shapes.add(new GeoPolygonFactory.PolygonDescription(points));
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
    assertTrue(!c.isWithin(gp));
    
    // Now, go clockwise
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));    
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    //System.out.println(c);
    
    // Middle point should be within!!
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));

    shapes = new ArrayList<>();
    shapes.add(new GeoPolygonFactory.PolygonDescription(points));
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
    assertTrue(c.isWithin(gp));

  }

  @Test
  public void testPolygonIntersects() {
    GeoPolygon c;
    List<GeoPoint> points;
    List<GeoPolygonFactory.PolygonDescription> shapes;
    XYZBounds xyzBounds;
    XYZSolid xyzSolid;
    
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);

    xyzBounds = new XYZBounds();
    c.getBounds(xyzBounds);
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumX(), xyzBounds.getMaximumX(), xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    assertEquals(GeoArea.WITHIN, xyzSolid.getRelationship(c));
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ(), xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    assertEquals(GeoArea.DISJOINT, xyzSolid.getRelationship(c));

    shapes = new ArrayList<>();
    shapes.add(new GeoPolygonFactory.PolygonDescription(points));
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);

    // Same bounds should work
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumX(), xyzBounds.getMaximumX(), xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    assertEquals(GeoArea.WITHIN, xyzSolid.getRelationship(c));
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ(), xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    assertEquals(GeoArea.DISJOINT, xyzSolid.getRelationship(c));

    // Bounds we obtain from the large polygon also should work.
    xyzBounds = new XYZBounds();
    c.getBounds(xyzBounds);
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumX(), xyzBounds.getMaximumX(), xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    assertEquals(GeoArea.WITHIN, xyzSolid.getRelationship(c));
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ(), xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    assertEquals(GeoArea.DISJOINT, xyzSolid.getRelationship(c));

  }
  
  @Test
  public void testPolygonPointWithin() {
    GeoPolygon c;
    GeoPoint gp;
    List<GeoPoint> points;
    List<GeoPolygonFactory.PolygonDescription> shapes;

    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
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

    // Now, same thing for large polygon
    shapes = new ArrayList<>();
    shapes.add(new GeoPolygonFactory.PolygonDescription(points));
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
    
    // Sample some points within
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.45);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.55);
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

    // Next bunch of small polygon points
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.01, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.7));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.8));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.7));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.01, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
        
        /*
        System.out.println("Points: ");
        for (GeoPoint p : points) {
            System.out.println(" "+p);
        }
        */

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
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

    // Now, same thing for large polygon
    shapes = new ArrayList<>();
    shapes.add(new GeoPolygonFactory.PolygonDescription(points));
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
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
    LatLonBounds b;
    List<GeoPoint> points;
    XYZBounds xyzb;
    GeoPoint point;
    GeoArea area;
    
    // BKD failure
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.WGS84, -0.36716183577912814, 1.4836349969188696));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.7846038240742979, -0.02743348424931823));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.7376479402362607, -0.5072961758807019));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.3760415907667887, 1.4970455334565513));
    
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points);
    
    point = new GeoPoint(PlanetModel.WGS84, -0.01580760332365284, -0.03956004622490505);
    assertTrue(c.isWithin(point));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(area.isWithin(point));
    
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);

    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.6, b.getLeftLongitude(), 0.000001);
    assertEquals(-0.4, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
  }

  @Test
  public void testPolygonBoundsCase1() {
    GeoPolygon c;
    LatLonBounds b;
    List<GeoPoint> points;
    XYZBounds xyzb;
    GeoPoint point1;
    GeoPoint point2;
    GeoArea area;
    
    // Build the polygon
    points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, 0.7769776943105245, -2.157536559188766));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.9796549195552824, -0.25078026625235256));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.17644522781457245, 2.4225312555674967));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.4459804612164617, -1.2970934639728127));
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points);
    // GeoCompositeMembershipShape: {[GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
    // [[lat=0.17644522781457245, lon=2.4225312555674967], 
    //  [lat=-1.4459804612164617, lon=-1.2970934639728127], 
    // [lat=0.7769776943105245, lon=-2.157536559188766]]}, 
    // GeoConcavePolygon: {planetmodel=PlanetModel.WGS84, points=
    // [[lat=-0.9796549195552824, lon=-0.25078026625235256],
    //  [lat=0.17644522781457245, lon=2.4225312555674967], 
    //  [lat=0.7769776943105245, lon=-2.157536559188766]]}]}
    point1 = new GeoPoint(PlanetModel.WGS84, -1.2013743680763862, 0.48458963747230094);
    point2 = new GeoPoint(0.3189285805649921, 0.16790264636909197, -0.9308557496413026);
    
    assertTrue(c.isWithin(point1));
    assertTrue(c.isWithin(point2));
    
    // Now try bounds
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
      
    assertTrue(area.isWithin(point1));
    assertTrue(area.isWithin(point2));
  }
  
  @Test
  public void testGeoPolygonBoundsCase2() {
    // [junit4]   1> TEST: iter=23 shape=GeoCompositeMembershipShape: {[GeoConvexPolygon: {planetmodel=PlanetModel(ab=0.7563871189161702 c=1.2436128810838298), points=
    // [[lat=0.014071770744627236, lon=0.011030818292803128],
    //  [lat=0.006772117088906782, lon=-0.0012531892445234592],
    //  [lat=0.0022201615609504792, lon=0.005941293187389326]]}, GeoConcavePolygon: {planetmodel=PlanetModel(ab=0.7563871189161702 c=1.2436128810838298), points=
    // [[lat=-0.005507100238396111, lon=-0.008487706131259667],
    //  [lat=0.014071770744627236, lon=0.011030818292803128],
    //  [lat=0.0022201615609504792, lon=0.005941293187389326]]}]}

    PlanetModel pm = new PlanetModel(0.7563871189161702, 1.2436128810838298);
    // Build the polygon
    GeoCompositeMembershipShape c = new GeoCompositeMembershipShape();
    List<GeoPoint> points1 = new ArrayList<>();
    points1.add(new GeoPoint(pm, 0.014071770744627236, 0.011030818292803128));
    points1.add(new GeoPoint(pm, 0.006772117088906782, -0.0012531892445234592));
    points1.add(new GeoPoint(pm, 0.0022201615609504792, 0.005941293187389326));
    BitSet p1bits = new BitSet();
    c.addShape(new GeoConvexPolygon(pm, points1, p1bits, true));
    List<GeoPoint> points2 = new ArrayList<>();
    points2.add(new GeoPoint(pm, -0.005507100238396111, -0.008487706131259667));
    points2.add(new GeoPoint(pm, 0.014071770744627236, 0.011030818292803128));
    points2.add(new GeoPoint(pm, 0.0022201615609504792, 0.005941293187389326));
    BitSet p2bits = new BitSet();
    p2bits.set(1, true);
    c.addShape(new GeoConcavePolygon(pm, points2, p2bits, false));
    //System.out.println(c);
    
    // [junit4]   1>   point=[lat=0.003540694517552105, lon=-9.99517927901697E-4]
    // [junit4]   1>   quantized=[X=0.7563849869428783, Y=-7.560204674780763E-4, Z=0.0026781405884151086]
    GeoPoint point = new GeoPoint(pm, 0.003540694517552105, -9.99517927901697E-4);
    GeoPoint pointQuantized = new GeoPoint(0.7563849869428783, -7.560204674780763E-4, 0.0026781405884151086);
    
    // Now try bounds
    XYZBounds xyzb = new XYZBounds();
    c.getBounds(xyzb);
    GeoArea area = GeoAreaFactory.makeGeoArea(pm,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
      
    assertTrue(c.isWithin(point));
    assertTrue(c.isWithin(pointQuantized));
    // This fails!!
    assertTrue(area.isWithin(point));
    assertTrue(area.isWithin(pointQuantized));
  }

  @Test
  public void testGeoConcaveRelationshipCase1() {
    /*
   [junit4]   1> doc=906 matched but should not
   [junit4]   1>   point=[lat=-0.9825762558001477, lon=2.4832136904725273]
   [junit4]   1>   quantized=[X=-0.4505446160475436, Y=0.34850109186970535, Z=-0.8539966368663765]

doc=906 added here:

   [junit4]   1>   cycle: cell=107836 parentCellID=107835 x: -1147288468 TO -742350917, y: -1609508490 TO 1609508490, z: -2147483647 TO 2147483647, splits: 3 queue.size()=1
   [junit4]   1>     minx=-0.6107484000858642 maxx=-0.39518364125756916 miny=-0.8568069517709872 maxy=0.8568069517709872 minz=-1.1431930485939341 maxz=1.1431930485939341
   [junit4]   1>     GeoArea.CONTAINS: now addAll

shape:
   [junit4]   1> TEST: iter=18 shape=GeoCompositeMembershipShape: {[GeoConvexPolygon: {
   planetmodel=PlanetModel(ab=0.8568069516722363 c=1.1431930483277637), points=
   [[lat=1.1577814487635816, lon=1.6283601832010004],
   [lat=0.6664570999069251, lon=2.0855825542851574],
   [lat=-0.23953537010974632, lon=1.8498724094352876]]}, GeoConcavePolygon: {planetmodel=PlanetModel(ab=0.8568069516722363 c=1.1431930483277637), points=
   [[lat=1.1577814487635816, lon=1.6283601832010004],
   [lat=-0.23953537010974632, lon=1.8498724094352876],
   [lat=-1.1766904875978805, lon=-2.1346828411344436]]}]}
    */
    PlanetModel pm = new PlanetModel(0.8568069516722363, 1.1431930483277637);
    // Build the polygon
    GeoCompositeMembershipShape c = new GeoCompositeMembershipShape();
    List<GeoPoint> points1 = new ArrayList<>();
    points1.add(new GeoPoint(pm, 1.1577814487635816, 1.6283601832010004));
    points1.add(new GeoPoint(pm, 0.6664570999069251, 2.0855825542851574));
    points1.add(new GeoPoint(pm, -0.23953537010974632, 1.8498724094352876));
    BitSet p1bits = new BitSet();
    c.addShape(new GeoConvexPolygon(pm, points1, p1bits, true));
    List<GeoPoint> points2 = new ArrayList<>();
    points2.add(new GeoPoint(pm, 1.1577814487635816, 1.6283601832010004));
    points2.add(new GeoPoint(pm, -0.23953537010974632, 1.8498724094352876));
    points2.add(new GeoPoint(pm, -1.1766904875978805, -2.1346828411344436));
    BitSet p2bits = new BitSet();
    p2bits.set(1, true);
    c.addShape(new GeoConcavePolygon(pm, points2, p2bits, false));
    //System.out.println(c);
    
    GeoPoint point = new GeoPoint(pm, -0.9825762558001477, 2.4832136904725273);
    GeoPoint quantizedPoint = new GeoPoint(-0.4505446160475436, 0.34850109186970535, -0.8539966368663765);
    
    GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(pm,
      -0.6107484000858642, -0.39518364125756916, -0.8568069517709872, 0.8568069517709872, -1.1431930485939341, 1.1431930485939341);
    //System.out.println("relationship = "+xyzSolid.getRelationship(c));
    assertTrue(xyzSolid.getRelationship(c) == GeoArea.OVERLAPS);
  }
  
  @Test
  public void testPolygonFactoryCase1() {
    /*
       [junit4]   1> Initial points:
       [junit4]   1>  [X=-0.17279348371564082, Y=0.24422965662722748, Z=0.9521675605930696]
       [junit4]   1>  [X=-0.6385022730019092, Y=-0.6294493901210775, Z=0.4438687423720006]
       [junit4]   1>  [X=-0.9519561011293354, Y=-0.05324061687857965, Z=-0.30423702782227385]
       [junit4]   1>  [X=-0.30329807815178533, Y=-0.9447434167936289, Z=0.13262941042055737]
       [junit4]   1>  [X=-0.5367607140926697, Y=0.8179452639396644, Z=0.21163783898691005]
       [junit4]   1>  [X=0.39285411191111597, Y=0.6369575362013932, Z=0.6627439307500357]
       [junit4]   1>  [X=-0.44715655239362595, Y=0.8332957749253644, Z=0.3273923501593971]
       [junit4]   1>  [X=0.33024322515264537, Y=0.6945246730529289, Z=0.6387986432043298]
       [junit4]   1>  [X=-0.1699323603224724, Y=0.8516746480592872, Z=0.4963385521664198]
       [junit4]   1>  [X=0.2654788898359613, Y=0.7380222309164597, Z=0.6200740473100581]
       [junit4]   1> For start plane, the following points are in/out:
       [junit4]   1>  [X=-0.17279348371564082, Y=0.24422965662722748, Z=0.9521675605930696] is: in
       [junit4]   1>  [X=-0.6385022730019092, Y=-0.6294493901210775, Z=0.4438687423720006] is: in
       [junit4]   1>  [X=-0.9519561011293354, Y=-0.05324061687857965, Z=-0.30423702782227385] is: out
       [junit4]   1>  [X=-0.30329807815178533, Y=-0.9447434167936289, Z=0.13262941042055737] is: in
       [junit4]   1>  [X=-0.5367607140926697, Y=0.8179452639396644, Z=0.21163783898691005] is: out
       [junit4]   1>  [X=0.39285411191111597, Y=0.6369575362013932, Z=0.6627439307500357] is: in
       [junit4]   1>  [X=-0.44715655239362595, Y=0.8332957749253644, Z=0.3273923501593971] is: out
       [junit4]   1>  [X=0.33024322515264537, Y=0.6945246730529289, Z=0.6387986432043298] is: in
       [junit4]   1>  [X=-0.1699323603224724, Y=0.8516746480592872, Z=0.4963385521664198] is: out
       [junit4]   1>  [X=0.2654788898359613, Y=0.7380222309164597, Z=0.6200740473100581] is: out
      */
    
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(0.17279348371564082, 0.24422965662722748, 0.9521675605930696));
    points.add(new GeoPoint(-0.6385022730019092, -0.6294493901210775, 0.4438687423720006));
    points.add(new GeoPoint(-0.9519561011293354, -0.05324061687857965, -0.30423702782227385));
    points.add(new GeoPoint(-0.30329807815178533, -0.9447434167936289, 0.13262941042055737));
    points.add(new GeoPoint(-0.5367607140926697, 0.8179452639396644, 0.21163783898691005));
    points.add(new GeoPoint(0.39285411191111597, 0.6369575362013932, 0.6627439307500357));
    points.add(new GeoPoint(-0.44715655239362595, 0.8332957749253644, 0.3273923501593971));
    points.add(new GeoPoint(0.33024322515264537, 0.6945246730529289, 0.6387986432043298));
    points.add(new GeoPoint(-0.1699323603224724, 0.8516746480592872, 0.4963385521664198));
    points.add(new GeoPoint(0.2654788898359613, 0.7380222309164597, 0.6200740473100581));

    boolean illegalArgumentException = false;
    try {
      final GeoPolygon p = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, null);
    } catch (IllegalArgumentException e) {
      illegalArgumentException = true;
    }
    assertTrue(illegalArgumentException);
  }

  @Test
  public void testPolygonFactoryCase2() {
    /*
   [[lat=-0.48522750470337056, lon=-1.7370471071224087([X=-0.14644023172524287, Y=-0.8727091042681705, Z=-0.4665895520487907])], 
   [lat=-0.4252164254406539, lon=-1.0929282311747601([X=0.41916238097763436, Y=-0.8093435958043177, Z=-0.4127428785664968])], 
   [lat=0.2055150822737076, lon=0.8094775925193464([X=0.6760197133035871, Y=0.7093859395658346, Z=0.20427109186920892])], 
   [lat=-0.504360159046884, lon=-1.27628468850318([X=0.25421329462858633, Y=-0.8380671569889917, Z=-0.4834077932502288])], 
   [lat=-0.11994023948700858, lon=0.07857194136150605([X=0.9908123546871113, Y=0.07801065055912473, Z=-0.11978097184039621])], 
   [lat=0.39346633764155237, lon=1.306697331415816([X=0.24124272064589647, Y=0.8921189226448045, Z=0.3836311592666308])], 
   [lat=-0.07741593942416389, lon=0.5334693210962216([X=0.8594122640512101, Y=0.50755758923985, Z=-0.07742360418968308])], 
   [lat=0.4654236264787552, lon=1.3013260557429494([X=0.2380080413677112, Y=0.8617612419312584, Z=0.4489988990508502])], 
   [lat=-1.2964641581620537, lon=-1.487600369139357([X=0.022467282495493006, Y=-0.26942922375508405, Z=-0.960688317984634])]]
    */
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, -0.48522750470337056, -1.7370471071224087));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.4252164254406539, -1.0929282311747601));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.2055150822737076, 0.8094775925193464));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.504360159046884, -1.27628468850318));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.11994023948700858, 0.07857194136150605));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.39346633764155237, 1.306697331415816));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.07741593942416389, 0.5334693210962216));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.4654236264787552, 1.3013260557429494));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.2964641581620537, -1.487600369139357));
    
    boolean illegalArgumentException = false;
    try {
      final GeoPolygon p = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, null);
    } catch (IllegalArgumentException e) {
      illegalArgumentException = true;
    }
    assertTrue(illegalArgumentException);
  }
  
  @Test
  public void testPolygonFactoryCase3() {
    /*
    This one failed to be detected as convex:

   [junit4]   1> convex part = GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
   [[lat=0.39346633764155237, lon=1.306697331415816([X=0.24124272064589647, Y=0.8921189226448045, Z=0.3836311592666308])], 
   [lat=-0.4252164254406539, lon=-1.0929282311747601([X=0.41916238097763436, Y=-0.8093435958043177, Z=-0.4127428785664968])], 
   [lat=0.4654236264787552, lon=1.3013260557429494([X=0.2380080413677112, Y=0.8617612419312584, Z=0.4489988990508502])]], internalEdges={0, 1, 2}}
    */
    final GeoPoint p3 = new GeoPoint(PlanetModel.WGS84, 0.39346633764155237, 1.306697331415816);
    final GeoPoint p2 = new GeoPoint(PlanetModel.WGS84, -0.4252164254406539, -1.0929282311747601);
    final GeoPoint p1 = new GeoPoint(PlanetModel.WGS84, 0.4654236264787552, 1.3013260557429494);
    
    final List<GeoPoint> points = new ArrayList<>();
    points.add(p3);
    points.add(p2);
    points.add(p1);

    final BitSet internal = new BitSet();
    final GeoCompositePolygon rval = new GeoCompositePolygon();
    final GeoPolygonFactory.MutableBoolean mutableBoolean = new GeoPolygonFactory.MutableBoolean();
    
    boolean result = GeoPolygonFactory.buildPolygonShape(rval, mutableBoolean, PlanetModel.WGS84, points, internal, 0, 1,
      new SidedPlane(p1, p3, p2), new ArrayList<GeoPolygon>(), null);
    
    assertFalse(mutableBoolean.value);
    
  }
  
  @Test
  public void testPolygonFactoryCase4() {
    // [[lat=0.897812132711355, lon=0.0025364171887532795([X=0.6227358672251874, Y=0.0015795213449218714, Z=0.7812318690127594])],
    // [lat=0.897812132711355, lon=0.0025363997354607595([X=0.6227358672527552, Y=0.001579510476130618, Z=0.7812318690127594])],
    // [lat=0.8978120628981849, lon=0.0025362601091206([X=0.6227359221556139, Y=0.0015794236644894651, Z=0.7812318257158789])]]
    
    final GeoPoint p1 = new GeoPoint(PlanetModel.WGS84, 0.897812132711355, 0.0025364171887532795);
    final GeoPoint p2 = new GeoPoint(PlanetModel.WGS84, 0.897812132711355, 0.0025363997354607595);
    final GeoPoint p3 = new GeoPoint(PlanetModel.WGS84, 0.8978120628981849, 0.0025362601091206);
    
    final List<GeoPoint> points = new ArrayList<>();
    points.add(p1);
    points.add(p2);
    points.add(p3);
    
    final List<GeoPolygonFactory.PolygonDescription> shapeList = new ArrayList<>();
    final GeoPolygonFactory.PolygonDescription desc = new GeoPolygonFactory.PolygonDescription(points, new ArrayList<GeoPolygonFactory.PolygonDescription>());
    
    shapeList.add(desc);
    
    GeoPolygon p = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, shapeList);
    
  }
  
  @Test
  public void testPolygonFactoryCase5() {
    /*
   [junit4]   1> points=[[lat=0.0425265613312593, lon=0.0([X=1.0002076326868337, Y=0.0, Z=0.042561051669501374])], 
    [lat=0.8894380320379947, lon=-2.8993466885897496([X=-0.6109015457368775, Y=-0.1509528453728308, Z=0.7760109675775679])], 
    [lat=-0.8298163536994994, lon=-0.1462586594666574([X=0.6673285226073522, Y=-0.09830454048435874, Z=-0.7372817203741138])], 
    [lat=0.0, lon=-1.7156310907312492E-12([X=1.0011188539924791, Y=-1.7175506314267352E-12, Z=0.0])], 
    [lat=-0.7766317703682181, lon=3.141592653589793([X=-0.7128972529667801, Y=8.730473389667082E-17, Z=-0.7005064828988063])]]

   {[GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
   [[lat=0.0425265613312593, lon=0.0([X=1.0002076326868337, Y=0.0, Z=0.042561051669501374])], 
   [lat=0.8894380320379947, lon=-2.8993466885897496([X=-0.6109015457368775, Y=-0.1509528453728308, Z=0.7760109675775679])], 
   [lat=-0.8298163536994994, lon=-0.1462586594666574([X=0.6673285226073522, Y=-0.09830454048435874, Z=-0.7372817203741138])], 
   [lat=0.0, lon=-1.7156310907312492E-12([X=1.0011188539924791, Y=-1.7175506314267352E-12, Z=0.0])]], internalEdges={3}}, 
   GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
   [[lat=0.0425265613312593, lon=0.0([X=1.0002076326868337, Y=0.0, Z=0.042561051669501374])], 
   [lat=0.0, lon=-1.7156310907312492E-12([X=1.0011188539924791, Y=-1.7175506314267352E-12, Z=0.0])], 
   [lat=-0.7766317703682181, lon=3.141592653589793([X=-0.7128972529667801, Y=8.730473389667082E-17, Z=-0.7005064828988063])]], internalEdges={0}}]}
    */
    final GeoPoint p1 = new GeoPoint(PlanetModel.WGS84, 0.0425265613312593, 0.0);
    final GeoPoint p2 = new GeoPoint(PlanetModel.WGS84, 0.8894380320379947, -2.8993466885897496);
    final GeoPoint p3 = new GeoPoint(PlanetModel.WGS84, -0.8298163536994994, -0.1462586594666574);
    final GeoPoint p4 = new GeoPoint(PlanetModel.WGS84, 0.0, -1.7156310907312492E-12);
    final GeoPoint p5 = new GeoPoint(PlanetModel.WGS84, -0.7766317703682181, 3.141592653589793);

    final List<GeoPoint> polyList = new ArrayList<>();
    polyList.add(p1);
    polyList.add(p2);
    polyList.add(p3);
    polyList.add(p4);
    polyList.add(p5);
    
    GeoPolygon p = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, polyList);
    //System.out.println("p = "+p);

    XYZBounds bounds = new XYZBounds();
    p.getBounds(bounds);
    XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(),
      bounds.getMinimumY(), bounds.getMaximumY(),
      bounds.getMinimumZ(), bounds.getMaximumZ());

    //final List<GeoPoint> p1List = new ArrayList<>();
    //p1List.add(p1);
    //p1List.add(p2);
    //p1List.add(p3);
    //p1List.add(p4);
    //final BitSet p1Internal = new BitSet();
    //final GeoConvexPolygon poly1 = new GeoConvexPolygon(PlanetModel.WGS84, p1List, p1Internal, false);
    
    /*
    final List<GeoPoint> p2List = new ArrayList<>();
    p2List.add(p1);
    p2List.add(p4);
    p2List.add(p5);
    final BitSet p2Internal = new BitSet();
    final GeoConvexPolygon poly2 = new GeoConvexPolygon(PlanetModel.WGS84, p2List, p2Internal, false);
    */
    
    //XYZBounds bounds1 = new XYZBounds();
    //poly1.getBounds(bounds1);
    //XYZSolid solid1 = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds1.getMinimumX(), bounds1.getMaximumX(),
    //  bounds1.getMinimumY(), bounds1.getMaximumY(),
    //  bounds1.getMinimumZ(), bounds1.getMaximumZ());
    
    /*
    XYZBounds bounds2 = new XYZBounds();
    poly2.getBounds(bounds2);
    XYZSolid solid2 = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds2.getMinimumX(), bounds2.getMaximumX(),
      bounds2.getMinimumY(), bounds2.getMaximumY(),
      bounds2.getMinimumZ(), bounds2.getMaximumZ());
    */
    
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, -0.41518838180529244, 3.141592653589793);
    final GeoPoint encodedPoint = new GeoPoint(-0.9155623168963972, 2.3309121299774915E-10, -0.40359240449795253);
    //System.out.println("point = "+point);
    //System.out.println("encodedPoint = "+encodedPoint);
    
    assertTrue(p.isWithin(point));
    assertTrue(solid.isWithin(point));
    
    //System.out.println("bounds1 = "+bounds1);
    //System.out.println("bounds2 = "+bounds2);
    //assertTrue(poly1.isWithin(point));
    //assertTrue(poly2.isWithin(point));
    //assertTrue(solid2.isWithin(point));
    
    //assertTrue(poly2.isWithin(encodedPoint));
  }
  
  @Test
  public void testLargePolygonFailureCase1() {
    /*
   [junit4]    >   shape=GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=65f193fc, 
    testPoint=[lat=1.3005550159098878, lon=-2.4043250791032897([X=-0.1972404544647752, Y=-0.17911237095124333, Z=0.9617794725902562])], 
    testPointInSet=false,
    shapes={ 
     {[lat=0.972005250702484, lon=-1.9776473855435277([X=-0.22278290030997686, Y=-0.5170266140533727, Z=0.8250470449472769])],
    [lat=0.5530477484903267, lon=2.5300578442038137([X=-0.6968439858923609, Y=0.4886310878468911, Z=0.5253825248638686])],
    [lat=1.5185372097372358, lon=-0.33848566616392867([X=0.04916162127975167, Y=-0.01730656055596007, Z=0.9964092501726799])]}}
   [junit4]    >   bounds=XYZBounds: [xmin=-1.0011188544924792 xmax=0.04916162177975167 ymin=-1.0011188544924792 ymax=1.0011188544924792 zmin=-5.0E-10 zmax=0.99766957331525]
   [junit4]    >   world bounds=( minX=-1.0011188539924791 maxX=1.0011188539924791 minY=-1.0011188539924791 maxY=1.0011188539924791 minZ=-0.9977622920221051 maxZ=0.9977622920221051
   [junit4]    >   quantized point=[X=0.32866145093230836, Y=0.21519085912590594, Z=0.9177348472123349] within shape? true within bounds? false
   [junit4]    >   unquantized point=[lat=1.166339260547107, lon=0.5797066870374205([X=0.3286614507856878, Y=0.21519085911319938, Z=0.9177348470779726])] within shape? true within bounds? false
   [junit4]    >   docID=10 deleted?=false
   [junit4]    >   query=PointInGeo3DShapeQuery: field=point: Shape: GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=65f193fc, testPoint=[lat=1.3005550159098878, lon=-2.4043250791032897([X=-0.1972404544647752, Y=-0.17911237095124333, Z=0.9617794725902562])], testPointInSet=false, shapes={ {[lat=0.972005250702484, lon=-1.9776473855435277([X=-0.22278290030997686, Y=-0.5170266140533727, Z=0.8250470449472769])], [lat=0.5530477484903267, lon=2.5300578442038137([X=-0.6968439858923609, Y=0.4886310878468911, Z=0.5253825248638686])], [lat=1.5185372097372358, lon=-0.33848566616392867([X=0.04916162127975167, Y=-0.01730656055596007, Z=0.9964092501726799])]}}
   [junit4]    >   explanation:
   [junit4]    >     target is in leaf _0(7.0.0):c13 of full reader StandardDirectoryReader(segments:3:nrt _0(7.0.0):c13)
   [junit4]    >     full BKD path to target doc:
   [junit4]    >       Cell(x=-0.9060562472023252 TO 1.0010658113048514 y=-0.5681445384324596 TO 0.7613281936331098 z=-0.43144274682272304 TO 0.9977622920582089); Shape relationship = OVERLAPS; Quantized point within cell = true; Unquantized point within cell = true
   [junit4]    >     on cell Cell(x=-0.9060562472023252 TO 1.0010658113048514 y=-0.5681445384324596 TO 0.7613281936331098 z=-0.43144274682272304 TO 0.9977622920582089); Shape relationship = OVERLAPS; Quantized point within cell = true; Unquantized point within cell = true, wrapped visitor returned CELL_CROSSES_QUERY
   [junit4]    >   leaf visit docID=10 x=0.32866145093230836 y=0.21519085912590594 z=0.9177348472123349
    */
    final GeoPoint testPoint = new GeoPoint(PlanetModel.WGS84, 1.3005550159098878, -2.4043250791032897);
    final boolean testPointInSet = false;
    final List<GeoPoint> pointList = new ArrayList<>();
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.972005250702484, -1.9776473855435277));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.5530477484903267, 2.5300578442038137));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 1.5185372097372358, -0.33848566616392867));
    
    final GeoPolygon pSanity = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pointList);
    
    assertTrue(pSanity.isWithin(testPoint) == testPointInSet);
    
    final List<List<GeoPoint>> shapeList = new ArrayList<>();
    shapeList.add(pointList);
    final GeoPolygon p = new GeoComplexPolygon(PlanetModel.WGS84, shapeList, testPoint, testPointInSet);
    
    final GeoPoint intersectionPoint = new GeoPoint(0.26643017529034996, 0.0, 0.9617794725902564);
    assertTrue(pSanity.isWithin(intersectionPoint) == p.isWithin(intersectionPoint));
    assertTrue(p.isWithin(intersectionPoint));
    
    final GeoPoint maxXPoint = new GeoPoint(PlanetModel.WGS84, 0.0, 0.0);
    
    assertTrue(pSanity.isWithin(maxXPoint) == p.isWithin(maxXPoint));
    
    final GeoPoint checkPoint = new GeoPoint(PlanetModel.WGS84, 1.166339260547107, 0.5797066870374205);
    
    // Given the choice of test point, does this all make sense?
    assertTrue(pSanity.isWithin(checkPoint) == p.isWithin(checkPoint));
    
    final XYZBounds referenceBounds = new XYZBounds();
    pSanity.getBounds(referenceBounds);
    
    final XYZBounds actualBounds = new XYZBounds();
    p.getBounds(actualBounds);
    
    assertEquals(referenceBounds.getMinimumX(), actualBounds.getMinimumX(), 0.0000001);
    assertEquals(referenceBounds.getMaximumX(), actualBounds.getMaximumX(), 0.0000001);
    assertEquals(referenceBounds.getMinimumY(), actualBounds.getMinimumY(), 0.0000001);
    assertEquals(referenceBounds.getMaximumY(), actualBounds.getMaximumY(), 0.0000001);
    assertEquals(referenceBounds.getMinimumZ(), actualBounds.getMinimumZ(), 0.0000001);
    assertEquals(referenceBounds.getMaximumZ(), actualBounds.getMaximumZ(), 0.0000001);

  }
  
  @Test
  public void testLargePolygonFailureCase2() {
    /*
   [junit4]    > Throwable #1: java.lang.AssertionError: FAIL: id=2 should have matched but did not
   [junit4]    >   shape=GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=6eccd33b, 
   testPoint=[lat=0.03170690566178683, lon=1.0862414976732029([X=0.46609969117964495, Y=0.8854242006628827, Z=0.0317369552646047])], 
   testPointInSet=false, 
   shapes={ {
   [lat=1.0774842300167298, lon=-0.11534121538553185([X=0.46969930266058374, Y=-0.054417217622152375, Z=0.8794587218580684])], 
   [lat=0.05101544777239065, lon=1.031558236908661([X=0.5133835679471972, Y=0.8579350866926241, Z=0.051049928818862174])], 
   [lat=-0.011222928649880962, lon=1.5851249038356199([X=-0.01434320835886277, Y=1.0009526216234983, Z=-0.011235244842183226])], 
   [lat=-0.02571365137215876, lon=0.5627875521419741([X=0.8464356149277266, Y=0.5339650936800929, Z=-0.025739527171261035])], 
   [lat=0.03833766792865358, lon=1.0082901344798614([X=0.5335096521470836, Y=0.8462411929752105, Z=0.03837097111317845])], 
   [lat=0.1719054969347345, lon=0.9024290407832926([X=0.6111941952395734, Y=0.7740553755547761, Z=0.17123457719021212])], 
   [lat=0.08180947807010808, lon=1.0107147265848113([X=0.5300590148023426, Y=0.8453039531721928, Z=0.08180784289673602])]}}
   [junit4]    >   bounds=XYZBounds: [xmin=-1.0011188544924792 xmax=1.0011188544924792 
    ymin=-1.0011188544924792 ymax=1.0011188544924792 
    zmin=-0.025739527671261034 zmax=0.9977622925221051]
   [junit4]    >   world bounds=( minX=-1.0011188539924791 maxX=1.0011188539924791 minY=-1.0011188539924791 maxY=1.0011188539924791 minZ=-0.9977622920221051 maxZ=0.9977622920221051
   [junit4]    >   quantized point=[X=-0.477874179571219, Y=0.5908091335156603, Z=-0.6495967142221521] within shape? true within bounds? false
   [junit4]    >   unquantized point=[lat=-0.7073124559987376, lon=2.2509085326629887([X=-0.47787417938801546, Y=0.5908091336704123, Z=-0.6495967140640758])] within shape? true within bounds? false
   [junit4]    >   docID=2 deleted?=false
   [junit4]    >   query=PointInGeo3DShapeQuery: field=point: Shape: GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=6eccd33b, testPoint=[lat=0.03170690566178683, lon=1.0862414976732029([X=0.46609969117964495, Y=0.8854242006628827, Z=0.0317369552646047])], testPointInSet=false, shapes={ {[lat=1.0774842300167298, lon=-0.11534121538553185([X=0.46969930266058374, Y=-0.054417217622152375, Z=0.8794587218580684])], [lat=0.05101544777239065, lon=1.031558236908661([X=0.5133835679471972, Y=0.8579350866926241, Z=0.051049928818862174])], [lat=-0.011222928649880962, lon=1.5851249038356199([X=-0.01434320835886277, Y=1.0009526216234983, Z=-0.011235244842183226])], [lat=-0.02571365137215876, lon=0.5627875521419741([X=0.8464356149277266, Y=0.5339650936800929, Z=-0.025739527171261035])], [lat=0.03833766792865358, lon=1.0082901344798614([X=0.5335096521470836, Y=0.8462411929752105, Z=0.03837097111317845])], [lat=0.1719054969347345, lon=0.9024290407832926([X=0.6111941952395734, Y=0.7740553755547761, Z=0.17123457719021212])], [lat=0.08180947807010808, lon=1.0107147265848113([X=0.5300590148023426, Y=0.8453039531721928, Z=0.08180784289673602])]}}
   [junit4]    >   explanation:
   [junit4]    >     target is in leaf _0(7.0.0):C11 of full reader StandardDirectoryReader(segments:3:nrt _0(7.0.0):C11)
   [junit4]    >     full BKD path to target doc:
   [junit4]    >       Cell(x=-0.8906255176936849 TO 1.0005089994430834 y=-0.6808995306272861 TO 0.9675171153117977 z=-0.997762292058209 TO 0.9939318087373729); Shape relationship = OVERLAPS; Quantized point within cell = true; Unquantized point within cell = true
   [junit4]    >     on cell Cell(x=-0.8906255176936849 TO 1.0005089994430834 y=-0.6808995306272861 TO 0.9675171153117977 z=-0.997762292058209 TO 0.9939318087373729); Shape relationship = OVERLAPS; Quantized point within cell = true; Unquantized point within cell = true, wrapped visitor returned CELL_CROSSES_QUERY
   [junit4]    >   leaf visit docID=2 x=-0.477874179571219 y=0.5908091335156603 z=-0.6495967142221521
    */
    final GeoPoint testPoint = new GeoPoint(PlanetModel.WGS84, 0.03170690566178683, 1.0862414976732029);
    final boolean testPointInSet = false;
    final List<GeoPoint> pointList = new ArrayList<>();
    // If the 1.07748... line is at the top, the bounds are correct and the test succeeds. 
    // If this line is at the bottom, though, the bounds are wrong and the test fails.
    //pointList.add(new GeoPoint(PlanetModel.WGS84, 1.0774842300167298, -0.11534121538553185));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.05101544777239065, 1.031558236908661));
    pointList.add(new GeoPoint(PlanetModel.WGS84, -0.011222928649880962, 1.5851249038356199));
    pointList.add(new GeoPoint(PlanetModel.WGS84, -0.02571365137215876, 0.5627875521419741));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.03833766792865358, 1.0082901344798614));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.1719054969347345, 0.9024290407832926));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 0.08180947807010808, 1.0107147265848113));
    pointList.add(new GeoPoint(PlanetModel.WGS84, 1.0774842300167298, -0.11534121538553185));
    
    final GeoPolygon pSanity = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pointList);
    
    assertTrue(pSanity.isWithin(testPoint) == testPointInSet);
    
    final List<List<GeoPoint>> shapeList = new ArrayList<>();
    shapeList.add(pointList);
    final GeoPolygon p = new GeoComplexPolygon(PlanetModel.WGS84, shapeList, testPoint, testPointInSet);
    
    //System.err.println(p);
    /*
   [junit4]   2> GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=dcf3e99, 
   testPoint=[lat=0.03170690566178683, lon=1.0862414976732029([X=0.46609969117964506, Y=0.8854242006628825, Z=0.0317369552646047])], 
   testPointInSet=false, 
   shapes={ {
   [lat=1.0774842300167298, lon=-0.11534121538553185([X=0.46969930266058374, Y=-0.054417217622152375, Z=0.8794587218580684])], 
   [lat=0.05101544777239065, lon=1.031558236908661([X=0.5133835679471972, Y=0.8579350866926241, Z=0.051049928818862174])], 
   [lat=-0.011222928649880962, lon=1.5851249038356199([X=-0.01434320835886277, Y=1.0009526216234983, Z=-0.011235244842183226])], 
   [lat=-0.02571365137215876, lon=0.5627875521419741([X=0.8464356149277266, Y=0.5339650936800929, Z=-0.025739527171261035])], 
   [lat=0.03833766792865358, lon=1.0082901344798614([X=0.5335096521470836, Y=0.8462411929752105, Z=0.03837097111317845])], 
   [lat=0.1719054969347345, lon=0.9024290407832926([X=0.6111941952395734, Y=0.7740553755547761, Z=0.17123457719021212])]}}
   [lat=0.08180947807010808, lon=1.0107147265848113([X=0.5300590148023426, Y=0.8453039531721928, Z=0.08180784289673602])], 
    */
    final XYZBounds referenceBounds = new XYZBounds();
    pSanity.getBounds(referenceBounds);
    
    final XYZBounds actualBounds = new XYZBounds();
    p.getBounds(actualBounds);
    
    assertEquals(referenceBounds.getMinimumX(), actualBounds.getMinimumX(), 0.0000001);
    assertEquals(referenceBounds.getMaximumX(), actualBounds.getMaximumX(), 0.0000001);
    assertEquals(referenceBounds.getMinimumY(), actualBounds.getMinimumY(), 0.0000001);
    assertEquals(referenceBounds.getMaximumY(), actualBounds.getMaximumY(), 0.0000001);
    assertEquals(referenceBounds.getMinimumZ(), actualBounds.getMinimumZ(), 0.0000001);
    assertEquals(referenceBounds.getMaximumZ(), actualBounds.getMaximumZ(), 0.0000001);

    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84,
      actualBounds.getMinimumX(), actualBounds.getMaximumX(),
      actualBounds.getMinimumY(), actualBounds.getMaximumY(),
      actualBounds.getMinimumZ(), actualBounds.getMaximumZ());

    final GeoPoint checkPoint = new GeoPoint(PlanetModel.WGS84, -0.7073124559987376, 2.2509085326629887);
    
    // Given the choice of test point, does this all make sense?
    assertTrue(pSanity.isWithin(checkPoint) == p.isWithin(checkPoint));
    assertTrue(p.isWithin(checkPoint));
    assertTrue(solid.isWithin(checkPoint));
    
  }

  @Test
  public void testPolygonFailureCase1() {
    final List<GeoPoint> poly2List = new ArrayList<>();
    poly2List.add(new GeoPoint(PlanetModel.WGS84, -0.6370451769779303, 2.5318373679431616));
    poly2List.add(new GeoPoint(PlanetModel.WGS84, 1.5707963267948966, -3.141592653589793));
    poly2List.add(new GeoPoint(PlanetModel.WGS84, -1.0850383189690824, 2.4457272005608357E-47));
    poly2List.add(new GeoPoint(PlanetModel.WGS84, -0.5703530503197992, -3.141592653589793));
    final BitSet poly2Bitset = new BitSet();
    poly2Bitset.set(1);
    
    boolean result;
    try {
      final GeoConvexPolygon poly2 = new GeoConvexPolygon(PlanetModel.WGS84, poly2List);
      result = true;
    } catch (IllegalArgumentException e) {
      result = false;
    }
    
    assertTrue(!result);
  }

  @Test
  public void testPolygonFailureCase2() {
    /*
   [junit4]   1>   shape=GeoCompositeMembershipShape: {[GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
   [lat=1.079437865394857, lon=-1.720224083538152E-11([X=0.47111944719262044, Y=-8.104310192839264E-12, Z=0.8803759987367299])], 
   [lat=-1.5707963267948966, lon=0.017453291479645996([X=6.108601474971234E-17, Y=1.066260290095308E-18, Z=-0.997762292022105])], 
   [lat=0.017453291479645996, lon=2.4457272005608357E-47([X=1.0009653513901666, Y=2.448088186713865E-47, Z=0.01747191415779267])]], internalEdges={2}},
   GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
   [lat=1.079437865394857, lon=-1.720224083538152E-11([X=0.47111944719262044, Y=-8.104310192839264E-12, Z=0.8803759987367299])], 
   [lat=0.017453291479645996, lon=2.4457272005608357E-47([X=1.0009653513901666, Y=2.448088186713865E-47, Z=0.01747191415779267])], 
   [lat=0.0884233366943164, lon=0.4323234231678824([X=0.9054355304510789, Y=0.4178006803188124, Z=0.08840463683725623])]], internalEdges={0}}]}
    */
    final List<GeoPoint> poly1List = new ArrayList<>();
    poly1List.add(new GeoPoint(PlanetModel.WGS84, 1.079437865394857, -1.720224083538152E-11));
    poly1List.add(new GeoPoint(PlanetModel.WGS84, -1.5707963267948966, 0.017453291479645996));
    poly1List.add(new GeoPoint(PlanetModel.WGS84, 0.017453291479645996, 2.4457272005608357E-47));
    
    final GeoConvexPolygon poly1 = new GeoConvexPolygon(PlanetModel.WGS84, poly1List);
    
    /*
   [junit4]   1>       unquantized=[lat=-1.5316724989005415, lon=3.141592653589793([X=-0.03902652216795768, Y=4.779370545484258E-18, Z=-0.9970038705813589])]
   [junit4]   1>       quantized=[X=-0.03902652216283731, Y=2.3309121299774915E-10, Z=-0.9970038706538652]
    */
    
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, -1.5316724989005415, 3.141592653589793);

    assertTrue(poly1.isWithin(point));
    
    final XYZBounds actualBounds1 = new XYZBounds();
    poly1.getBounds(actualBounds1);
    
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84,
      actualBounds1.getMinimumX(), actualBounds1.getMaximumX(),
      actualBounds1.getMinimumY(), actualBounds1.getMaximumY(),
      actualBounds1.getMinimumZ(), actualBounds1.getMaximumZ());

    assertTrue(solid.isWithin(point));
  }
  
}
