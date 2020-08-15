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
import java.util.Collections;

import org.apache.lucene.util.LuceneTestCase;

import org.junit.Test;

public class GeoPolygonTest extends LuceneTestCase {

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
  public void testPolygonPointFiltering2() {
    //all coplanar
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, 1.1264101919629863, -0.9108307879480759);
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, 1.1264147298190414, -0.9108309624810013);
    GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, 1.1264056541069312, -0.9108306134151508);
    final List<GeoPoint> originalPoints = new ArrayList<>();
    originalPoints.add(point1);
    originalPoints.add(point2);
    originalPoints.add(point3);
    final List<GeoPoint> filteredPoints = GeoPolygonFactory.filterEdges(GeoPolygonFactory.filterPoints(originalPoints), 0.0);
    assertEquals(null, filteredPoints);
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

    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, pd);
    //System.out.println(zScaling);
    
    // Middle point should NOT be within!!
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(!c.isWithin(gp));

    shapes = new ArrayList<>();
    shapes.add(pd);
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
    assertTrue(!c.isWithin(gp));
    
    // Now, go clockwise
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));    
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));

    pd = new GeoPolygonFactory.PolygonDescription(points);
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, pd);
    //System.out.println(zScaling);
    
    // Middle point should be within!!
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.5);
    assertTrue(c.isWithin(gp));

    shapes = new ArrayList<>();
    shapes.add(pd);
    
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

    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, pd);

    xyzBounds = new XYZBounds();
    c.getBounds(xyzBounds);
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumX(), xyzBounds.getMaximumX(), xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ());
    assertEquals(GeoArea.WITHIN, xyzSolid.getRelationship(c));
    xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.SPHERE, xyzBounds.getMinimumY(), xyzBounds.getMaximumY(), xyzBounds.getMinimumZ(), xyzBounds.getMaximumZ(), xyzBounds.getMinimumX(), xyzBounds.getMaximumX());
    assertEquals(GeoArea.DISJOINT, xyzSolid.getRelationship(c));

    shapes = new ArrayList<>();
    shapes.add(pd);
    
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

    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    // Now, same thing for large polygon
    shapes = new ArrayList<>();
    shapes.add(pd);
    
    c = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, shapes);
    //System.out.println("Large polygon = "+zScaling);
    
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
    
    pd = new GeoPolygonFactory.PolygonDescription(points);
        /*
        System.out.println("Points: ");
        for (GeoPoint p : points) {
            System.out.println(" "+p);
        }
        */

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, pd);
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
    shapes.add(pd);
    
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
    // [junit4]   1> TEST: iter=23 shape=GeoCompositeMembershipShape: {[GeoConvexPolygon: {planetmodel=PlanetModel(xyScaling=0.7563871189161702 zScaling=1.2436128810838298), points=
    // [[lat=0.014071770744627236, lon=0.011030818292803128],
    //  [lat=0.006772117088906782, lon=-0.0012531892445234592],
    //  [lat=0.0022201615609504792, lon=0.005941293187389326]]}, GeoConcavePolygon: {planetmodel=PlanetModel(xyScaling=0.7563871189161702 zScaling=1.2436128810838298), points=
    // [[lat=-0.005507100238396111, lon=-0.008487706131259667],
    //  [lat=0.014071770744627236, lon=0.011030818292803128],
    //  [lat=0.0022201615609504792, lon=0.005941293187389326]]}]}

    PlanetModel pm = new PlanetModel(0.7563871189161702, 1.2436128810838298);
    // Build the polygon
    GeoCompositeMembershipShape c = new GeoCompositeMembershipShape(pm);
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
    //System.out.println(zScaling);
    
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
   planetmodel=PlanetModel(xyScaling=0.8568069516722363 zScaling=1.1431930483277637), points=
   [[lat=1.1577814487635816, lon=1.6283601832010004],
   [lat=0.6664570999069251, lon=2.0855825542851574],
   [lat=-0.23953537010974632, lon=1.8498724094352876]]}, GeoConcavePolygon: {planetmodel=PlanetModel(xyScaling=0.8568069516722363 zScaling=1.1431930483277637), points=
   [[lat=1.1577814487635816, lon=1.6283601832010004],
   [lat=-0.23953537010974632, lon=1.8498724094352876],
   [lat=-1.1766904875978805, lon=-2.1346828411344436]]}]}
    */
    PlanetModel pm = new PlanetModel(0.8568069516722363, 1.1431930483277637);
    // Build the polygon
    GeoCompositeMembershipShape c = new GeoCompositeMembershipShape(pm);
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
    //System.out.println(zScaling);
    
    GeoPoint point = new GeoPoint(pm, -0.9825762558001477, 2.4832136904725273);
    GeoPoint quantizedPoint = new GeoPoint(-0.4505446160475436, 0.34850109186970535, -0.8539966368663765);
    
    GeoArea xyzSolid = GeoAreaFactory.makeGeoArea(pm,
      -0.6107484000858642, -0.39518364125756916, -0.8568069517709872, 0.8568069517709872, -1.1431930485939341, 1.1431930485939341);
    //System.out.println("relationship = "+xyzSolid.getRelationship(zScaling));
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
  public void testPolygonFactoryCase3() throws Exception {
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
    final GeoCompositePolygon rval = new GeoCompositePolygon(PlanetModel.WGS84);
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
    
    assertTrue(p.isWithin(point)?solid.isWithin(point):true);
    
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

    final GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(poly1List);
    
    final GeoPolygon poly1 = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pd);
    
    /*
   [junit4]   1>       unquantized=[lat=-1.5316724989005415, lon=3.141592653589793([X=-0.03902652216795768, Y=4.779370545484258E-18, Z=-0.9970038705813589])]
   [junit4]   1>       quantized=[X=-0.03902652216283731, Y=2.3309121299774915E-10, Z=-0.9970038706538652]
    */
    
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, -1.5316724989005415, 3.141592653589793);

    final XYZBounds actualBounds1 = new XYZBounds();
    poly1.getBounds(actualBounds1);
    
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, actualBounds1);

    assertTrue(poly1.isWithin(point)?solid.isWithin(point):true);
  }

  @Test
  public void testConcavePolygon() {
    ArrayList<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    GeoPolygon polygon = ((GeoCompositePolygon)GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points)).getShape(0);
    GeoPolygon polygonConcave = GeoPolygonFactory.makeGeoConcavePolygon(PlanetModel.SPHERE,points);
    assertEquals(polygon,polygonConcave);
  }

  @Test
  public void testPolygonWithHole() {
    ArrayList<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -1.1, -1.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1.0, -1.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1.1, -1.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1.0, -1.4));
    ArrayList<GeoPoint> hole_points = new ArrayList<>();
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    
    GeoPolygonFactory.PolygonDescription holeDescription = new GeoPolygonFactory.PolygonDescription(hole_points);
    List<GeoPolygonFactory.PolygonDescription> holes = new ArrayList<>(1);
    holes.add(holeDescription);
    GeoPolygonFactory.PolygonDescription polygonDescription = new GeoPolygonFactory.PolygonDescription(points, holes);
    
    // Create two polygons -- one simple, the other complex.  Both have holes.  Compare their behavior.
    GeoPolygon holeSimplePolygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE,polygonDescription);
    List<GeoPolygonFactory.PolygonDescription> polys = new ArrayList<>(1);
    polys.add(polygonDescription);
    GeoPolygon holeComplexPolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE,polys);

    // Sample some nearby points outside
    GeoPoint gp;
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.65);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, -0.35);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, -0.15, -0.5);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.15, -0.5);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    // Random points outside
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, Math.PI * 0.5, 0.0);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, Math.PI);
    assertEquals(holeSimplePolygon.isWithin(gp), holeComplexPolygon.isWithin(gp));

  }

  @Test
  public void testConvexPolygon() {
    ArrayList<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0, 0));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.5, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.5, 0));
    GeoPolygon polygon = ((GeoCompositePolygon)GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points)).getShape(0);
    GeoPolygon polygon2 = GeoPolygonFactory.makeGeoConvexPolygon(PlanetModel.SPHERE,points);
    assertEquals(polygon,polygon2);
  }

  @Test
  public void testConvexPolygonWithHole() {
    ArrayList<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -1, -1));
    points.add(new GeoPoint(PlanetModel.SPHERE, -1, 1));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1, 1));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1, -1));
    ArrayList<GeoPoint> hole_points = new ArrayList<>();
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    hole_points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));
    GeoPolygon hole = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE,hole_points);

    GeoPolygon polygon = ((GeoCompositePolygon)GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points,Collections.singletonList(hole))).getShape(0);
    GeoPolygon polygon2 = GeoPolygonFactory.makeGeoConvexPolygon(PlanetModel.SPHERE,points,Collections.singletonList(hole));
    assertEquals(polygon,polygon2);
  }

  @Test
  public void testLUCENE8133() {
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.434456), Geo3DUtil.fromDegrees(14.459204));
    GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.43394), Geo3DUtil.fromDegrees(14.459206));
    GeoPoint check =  new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.434067), Geo3DUtil.fromDegrees(14.458927));
    if (!point1.isIdentical(point2) && !check.isIdentical(point1) && !check.isIdentical(point2)) {
      SidedPlane plane = new SidedPlane(check, point1, point2);
      assertTrue(plane.isWithin(check));
      assertTrue(plane.isWithin(point1));
      assertTrue(plane.isWithin(point2));
      //POLYGON((14.459204 -23.434456, 14.459206 -23.43394,14.458647 -23.434196, 14.458646 -23.434452,14.459204 -23.434456))
      List<GeoPoint> points = new ArrayList<>();
      points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.434456), Geo3DUtil.fromDegrees(14.459204)));
      points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees( -23.43394), Geo3DUtil.fromDegrees(14.459206)));
      points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.434196), Geo3DUtil.fromDegrees(14.458647)));
      points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-23.434452), Geo3DUtil.fromDegrees(14.458646)));
      GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    }
  }

  @Test
  public void testLUCENE8140() throws Exception {
    //POINT(15.426026 68.35078) is coplanar
    //"POLYGON((15.426411 68.35069,15.4261 68.35078,15.426026 68.35078,15.425868 68.35078,15.425745 68.350746,15.426411 68.35069))";
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(68.35069), Geo3DUtil.fromDegrees(15.426411)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(68.35078), Geo3DUtil.fromDegrees(15.4261)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(68.35078), Geo3DUtil.fromDegrees(15.426026)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(68.35078), Geo3DUtil.fromDegrees(15.425868)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(68.350746), Geo3DUtil.fromDegrees(15.426411)));
    assertTrue(GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points) != null);
  }


  @Test
  public void testLUCENE8211() {
    //We need to handle the situation where the check point is parallel to
    //the test point.
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 0, 0));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0, 1));
    points.add(new GeoPoint(PlanetModel.SPHERE, 1, 1));
    points.add(new GeoPoint(PlanetModel.SPHERE,1, 0));
    GeoPoint testPoint = new GeoPoint(PlanetModel.SPHERE, 0.5, 0.5);
    final List<List<GeoPoint>> pointsList = new ArrayList<>();
    pointsList.add(points);
    GeoPolygon polygon = new GeoComplexPolygon(PlanetModel.SPHERE, pointsList, testPoint, true);
    assertTrue(polygon.isWithin(PlanetModel.SPHERE.createSurfacePoint(testPoint.x, testPoint.y, testPoint.z)));
    assertFalse(polygon.isWithin(PlanetModel.SPHERE.createSurfacePoint(-testPoint.x, -testPoint.y, -testPoint.z)));
    //special cases
    assertFalse(polygon.isWithin(PlanetModel.SPHERE.createSurfacePoint(testPoint.x, -testPoint.y, -testPoint.z)));
    assertFalse(polygon.isWithin(PlanetModel.SPHERE.createSurfacePoint(-testPoint.x, testPoint.y, -testPoint.z)));
    assertFalse(polygon.isWithin(PlanetModel.SPHERE.createSurfacePoint(-testPoint.x, -testPoint.y, testPoint.z)));
  }

  @Test
  public void testCoplanarityTileConvex() throws Exception {
    // This test has been disabled because it is possible that the polygon specified actually intersects itself.
    //POLYGON((24.39398 65.77519,24.3941 65.77498,24.394024 65.77497,24.393976 65.77495,24.393963 65.77493,24.394068 65.774925,24.394156 65.77495,24.394201 65.77495,24.394234 65.77496,24.394266 65.77498,24.394318 65.77498,24.39434 65.774956,24.394377 65.77495,24.394451 65.77494,24.394476 65.77495,24.394457 65.77498,24.39398 65.77519))"
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77519), Geo3DUtil.fromDegrees(24.39398)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77498), Geo3DUtil.fromDegrees(24.3941)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77497), Geo3DUtil.fromDegrees(24.394024)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77495), Geo3DUtil.fromDegrees(24.393976)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77493), Geo3DUtil.fromDegrees(24.393963)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.774925), Geo3DUtil.fromDegrees(24.394068)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77495), Geo3DUtil.fromDegrees(24.394156)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77495), Geo3DUtil.fromDegrees(24.394201)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77496), Geo3DUtil.fromDegrees(24.394234)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77498), Geo3DUtil.fromDegrees(24.394266)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77498), Geo3DUtil.fromDegrees(24.394318)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.774956), Geo3DUtil.fromDegrees(24.39434)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77495), Geo3DUtil.fromDegrees(24.394377)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77494), Geo3DUtil.fromDegrees(24.394451)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77495), Geo3DUtil.fromDegrees(24.394476)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(65.77498), Geo3DUtil.fromDegrees(24.394457)));
    GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    assertTrue(polygon != null);
  }

  @Test
  public void testCoplanarityConcave() throws Exception {
    //POLYGON((-52.18851 64.53777,-52.18853 64.53828,-52.18675 64.53829,-52.18676 64.53855,-52.18736 64.53855,-52.18737 64.53881,-52.18677 64.53881,-52.18683 64.54009,-52.18919 64.53981,-52.18916 64.53905,-52.19093 64.53878,-52.19148 64.53775,-52.18851 64.53777))
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53777), Geo3DUtil.fromDegrees(-52.18851)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53828), Geo3DUtil.fromDegrees(-52.18853)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53829), Geo3DUtil.fromDegrees(-52.18675)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53855), Geo3DUtil.fromDegrees(-52.18676)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53855), Geo3DUtil.fromDegrees(-52.18736)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53881), Geo3DUtil.fromDegrees(-52.18737)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53881), Geo3DUtil.fromDegrees(-52.18677)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.54009), Geo3DUtil.fromDegrees(-52.18683)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53981), Geo3DUtil.fromDegrees(-52.18919)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53905), Geo3DUtil.fromDegrees(-52.18916)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53878), Geo3DUtil.fromDegrees(-52.19093)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.53775), Geo3DUtil.fromDegrees(-52.19148)));
    GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    Collections.reverse(points);
    polygon  = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
  }

  @Test
  public void testCoplanarityConvex2() throws Exception {
    //POLYGON((-3.488658 50.45564,-3.4898987 50.455627,-3.489865 50.455585,-3.489833 50.45551,-3.489808 50.455433,-3.489806 50.455406,-3.4898643 50.45525,-3.4892037 50.455162,-3.4891756 50.455166,-3.4891088 50.455147,-3.4890108 50.455166,-3.4889853 50.455166,-3.48895 50.45516,-3.488912 50.455166,-3.4889014 50.455177,-3.488893 50.455185,-3.488927 50.45523,-3.4890666 50.455456,-3.48905 50.455467,-3.488658 50.45564))
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.45564), Geo3DUtil.fromDegrees(-3.488658)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455627), Geo3DUtil.fromDegrees(-3.4898987)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455585), Geo3DUtil.fromDegrees(-3.489865)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.45551), Geo3DUtil.fromDegrees(-3.489833)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455433), Geo3DUtil.fromDegrees(-3.489808)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455406), Geo3DUtil.fromDegrees(-3.489806)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.45525), Geo3DUtil.fromDegrees(-3.4898643)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455162), Geo3DUtil.fromDegrees(-3.4892037)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455166), Geo3DUtil.fromDegrees(-3.4891756)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455147), Geo3DUtil.fromDegrees(-3.4891088)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455166), Geo3DUtil.fromDegrees(-3.4890108)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455166), Geo3DUtil.fromDegrees(-3.4889853)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.45516), Geo3DUtil.fromDegrees(-3.48895)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455166), Geo3DUtil.fromDegrees(-3.488912)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455177), Geo3DUtil.fromDegrees(-3.4889014)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455185), Geo3DUtil.fromDegrees( -3.488893)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.45523), Geo3DUtil.fromDegrees(-3.488927)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455456), Geo3DUtil.fromDegrees(-3.4890666)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(50.455467), Geo3DUtil.fromDegrees( -3.48905)));
    GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    Collections.reverse(points);
    polygon  = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
  }
  
  /*
   [lat=-0.63542308910253, lon=0.9853722928232957([X=0.4446759777403525, Y=0.6707549854468698, Z=-0.5934780737681111])], 
  [lat=0.0, lon=0.0([X=1.0011188539924791, Y=0.0, Z=0.0])], 
  [lat=0.45435018176633574, lon=3.141592653589793([X=-0.8989684544372841, Y=1.1009188402610632E-16, Z=0.4390846549572752])], 
  [lat=-0.375870856827283, lon=2.9129132647718414([X=-0.9065744420970767, Y=0.21100590938346708, Z=-0.36732668582405886])], 
  [lat=-1.2205765069413237, lon=3.141592653589793([X=-0.3424714964202101, Y=4.194066218902145E-17, Z=-0.9375649457139603])]}}
  
   [junit4]   1>       unquantized=[lat=-3.1780051348770987E-74, lon=-3.032608859187692([X=-0.9951793580358298, Y=-0.1088898762907205, Z=-3.181560858610375E-74])]
   [junit4]   1>       quantized=[X=-0.9951793580415914, Y=-0.10888987641797832, Z=-2.3309121299774915E-10]
  */
  @Test
  public void testLUCENE8227() throws Exception {
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, -0.63542308910253, 0.9853722928232957));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.0, 0.0));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.45435018176633574, 3.141592653589793));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.375870856827283, 2.9129132647718414));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.2205765069413237, 3.141592653589793));
    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    
    /*
    for (int i = 0; i < points.size(); i++) {
      System.out.println("Point "+i+": "+points.get(i));
    }
    */

    final GeoPoint unquantized = new GeoPoint(PlanetModel.WGS84, -3.1780051348770987E-74, -3.032608859187692);
    //final GeoPoint quantized = new GeoPoint(-0.9951793580415914, -0.10888987641797832, -2.3309121299774915E-10);
    
    // Construct a standard polygon first to see what that does.  This winds up being a large polygon under the covers.
    GeoPolygon standard = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pd);
    
    // This should be in-set too, but isn't!!
    assertTrue(standard.isWithin(PlanetModel.WGS84.MIN_X_POLE));
    
    final XYZBounds standardBounds = new XYZBounds();
    standard.getBounds(standardBounds);
    final XYZSolid standardSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, standardBounds);

    // If within shape, should be within bounds
    //assertTrue(standard.isWithin(quantized)?standardSolid.isWithin(quantized):true);
    assertTrue(standard.isWithin(unquantized)?standardSolid.isWithin(unquantized):true);
    
  }
  
  /*
   [junit4]   1>     doc=754 is contained by shape but is outside the returned XYZBounds
   [junit4]   1>       unquantized=[lat=2.4043303687704734E-204, lon=3.1342447995980507([X=-1.0010918284309325, Y=0.007356008974104805, Z=2.4070204634028112E-204])]
   [junit4]   1>       quantized=[X=-1.0010918285430614, Y=0.007356008812298254, Z=2.3309121299774915E-10]

   [junit4]   1>     doc=3728 is contained by shape but is outside the returned XYZBounds
   [junit4]   1>       unquantized=[lat=2.4457272005608357E-47, lon=-3.1404077424936307([X=-1.001118151199965, Y=-0.0011862365610909341, Z=2.448463612203698E-47])]
   [junit4]   1>       quantized=[X=-1.0011181510675629, Y=-0.001186236379718708, Z=2.3309121299774915E-10]
   
   [junit4]   1>   shape=GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=7969cab3, 
   testPoint=[X=-0.07416172733314662, Y=0.5686488061136892, Z=0.8178445379402641], testPointInSet=true, shapes={ {
   [lat=-1.5707963267948966, lon=-1.0755217966112058([X=2.903696886845155E-17, Y=-5.375400029710238E-17, Z=-0.997762292022105])], 
   [lat=-1.327365682666958, lon=-2.9674513704178316([X=-0.23690293696956322, Y=-0.04167672037374933, Z=-0.9685334156912658])], 
   [lat=0.32288591161895097, lon=3.141592653589793([X=-0.9490627533610154, Y=1.1622666630935417E-16, Z=0.3175519551883462])], 
   [lat=0.0, lon=0.0([X=1.0011188539924791, Y=0.0, Z=0.0])], 
   [lat=0.2839194570254642, lon=-1.2434404554202965([X=0.30893121415043073, Y=-0.9097632721627391, Z=0.2803596238536593])]}}
  */
  @Test
  public void testLUCENE8227_case2() {
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, -1.5707963267948966, -1.0755217966112058));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.327365682666958, -2.9674513704178316));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.32288591161895097, 3.141592653589793));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.0, 0.0));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.2839194570254642, -1.2434404554202965));
    GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    
    final GeoPoint unquantized = new GeoPoint(PlanetModel.WGS84, 2.4457272005608357E-47, -3.1404077424936307);
    final GeoPoint quantized = new GeoPoint(-1.0011181510675629, -0.001186236379718708, 2.3309121299774915E-10);
    
    // Is the north pole in set, or out of set?
    final GeoPoint northPole = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5, 0.0);
    final GeoPoint negativeX = new GeoPoint(PlanetModel.WGS84, 0.0, Math.PI);
    final GeoPoint negativeY = new GeoPoint(PlanetModel.WGS84, 0.0, -Math.PI * 0.5);
    final GeoPoint positiveY = new GeoPoint(PlanetModel.WGS84, 0.0, Math.PI * 0.5);
    final GeoPoint testPoint = new GeoPoint(-0.074161727332972, 0.5686488061123504, 0.8178445379383386);

    // Construct a standard polygon first to see what that does.  This winds up being a large polygon under the covers.
    GeoPolygon standard = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pd);
    
    // This should be true, by inspection, but is false.  That's the cause for the failure.
    assertTrue(standard.isWithin(negativeX));
    
    assertTrue(standard.isWithin(testPoint));
    
    // This is in-set because it's on an edge
    assertTrue(standard.isWithin(northPole));
    
    // This is in-set
    assertTrue(standard.isWithin(positiveY));
    

    final XYZBounds standardBounds = new XYZBounds();
    standard.getBounds(standardBounds);
    final XYZSolid standardSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, standardBounds);

    // If within shape, should be within bounds
    assertTrue(standard.isWithin(unquantized)?standardSolid.isWithin(unquantized):true);
    assertTrue(standard.isWithin(quantized)?standardSolid.isWithin(quantized):true);

  }
  
  @Test
  public void testLUCENE7642() {
    // Construct XYZ solid
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84,
      0.1845405855034623, 0.2730694323646922,
      -1.398547277986495E-9, 0.020766291030223535,
      0.7703937553371503, 0.9977622932859774);
    
    /*
   [junit4]   1> individual planes
   [junit4]   1>  notableMinXPoints=[
      [X=0.1845405855034623, Y=-1.398547277986495E-9, Z=0.9806642352600131], 
      [X=0.1845405855034623, Y=0.020766291030223535, Z=0.9804458120424796]] 
    notableMaxXPoints=[
      [X=0.2730694323646922, Y=-1.398547277986495E-9, Z=0.959928047174481], 
      [X=0.2730694323646922, Y=0.020766291030223535, Z=0.9597049045335464]] 
    notableMinYPoints=[
      [X=0.1845405855034623, Y=-1.398547277986495E-9, Z=0.9806642352600131], 
      [X=0.2730694323646922, Y=-1.398547277986495E-9, Z=0.959928047174481]] 
    notableMaxYPoints=[
      [X=0.1845405855034623, Y=0.020766291030223535, Z=0.9804458120424796], 
      [X=0.2730694323646922, Y=0.020766291030223535, Z=0.9597049045335464]] 
    notableMinZPoints=[] 
    notableMaxZPoints=[]
    
    [junit4]   1> All edge points=[
      [X=0.1845405855034623, Y=-1.398547277986495E-9, Z=0.9806642352600131], 
      [X=0.1845405855034623, Y=0.020766291030223535, Z=0.9804458120424796], 
      [X=0.2730694323646922, Y=-1.398547277986495E-9, Z=0.959928047174481], 
      [X=0.2730694323646922, Y=0.020766291030223535, Z=0.9597049045335464]]

    */

    final GeoPoint edge1 = new GeoPoint(0.1845405855034623, -1.398547277986495E-9, 0.9806642352600131);
    final GeoPoint edge2 = new GeoPoint(0.1845405855034623, 0.020766291030223535, 0.9804458120424796);
    final GeoPoint edge3 = new GeoPoint(0.2730694323646922, -1.398547277986495E-9, 0.959928047174481);
    final GeoPoint edge4 = new GeoPoint(0.2730694323646922, 0.020766291030223535, 0.9597049045335464);
    
    // The above says that none of these intersect the surface: minZmaxX, minZminX, minZmaxY, minZminY, or
    // maxZmaxX, maxZminX, maxZmaxY, maxZminY.
    
    // So what about minZ and maxZ all by themselves?
    //
    // [junit4]   1> Outside world: minXminYminZ=false minXminYmaxZ=true minXmaxYminZ=false minXmaxYmaxZ=true maxXminYminZ=false 
    // maxXminYmaxZ=true maxXmaxYminZ=false maxXmaxYmaxZ=true
    //
    // So the minz plane does not intersect the world because it's all inside.  The maxZ plane is all outside but may intersect the world still.
    // But it doesn't because it's too far north.
    // So it looks like these are our edge points, and they are correct.
    
    /*
  GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
    [lat=-1.2267098126036888, lon=3.141592653589793([X=-0.33671029227864785, Y=4.123511816790159E-17, Z=-0.9396354281810864])], 
    [lat=0.2892272352400239, lon=0.017453291479645996([X=0.9591279281485559, Y=0.01674163926221766, Z=0.28545251693892165])], 
    [lat=-1.5707963267948966, lon=1.6247683074702402E-201([X=6.109531986173988E-17, Y=9.926573944611206E-218, Z=-0.997762292022105])]], internalEdges={2}}, 
  GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
    [lat=-1.2267098126036888, lon=3.141592653589793([X=-0.33671029227864785, Y=4.123511816790159E-17, Z=-0.9396354281810864])], 
    [lat=-1.5707963267948966, lon=1.6247683074702402E-201([X=6.109531986173988E-17, Y=9.926573944611206E-218, Z=-0.997762292022105])], 
    [lat=0.6723906085905078, lon=-3.0261581679831E-12([X=0.7821883235431606, Y=-2.367025584191143E-12, Z=0.6227413298552851])]], internalEdges={0}}]}
    */
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, -1.2267098126036888, 3.141592653589793));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.2892272352400239, 0.017453291479645996));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.5707963267948966, 1.6247683074702402E-201));
    points.add(new GeoPoint(PlanetModel.WGS84, 0.6723906085905078, -3.0261581679831E-12));
    
    final GeoPolygonFactory.PolygonDescription pd = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, pd);
    final List<GeoPolygonFactory.PolygonDescription> pdList = new ArrayList<>(1);
    pdList.add(pd);
    final GeoPolygon largeShape = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel. WGS84, pdList);
    
    /* This is the output:
   [junit4]   1> shape = GeoCompositePolygon: {[
    GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
      [lat=-1.2267098126036888, lon=3.141592653589793([X=-0.33671029227864785, Y=4.123511816790159E-17, Z=-0.9396354281810864])], 
      [lat=0.2892272352400239, lon=0.017453291479645996([X=0.9591279281485559, Y=0.01674163926221766, Z=0.28545251693892165])], 
      [lat=-1.5707963267948966, lon=1.6247683074702402E-201([X=6.109531986173988E-17, Y=9.926573944611206E-218, Z=-0.997762292022105])]], internalEdges={2}}, 
    GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[
      [lat=-1.2267098126036888, lon=3.141592653589793([X=-0.33671029227864785, Y=4.123511816790159E-17, Z=-0.9396354281810864])], 
      [lat=-1.5707963267948966, lon=1.6247683074702402E-201([X=6.109531986173988E-17, Y=9.926573944611206E-218, Z=-0.997762292022105])], 
      [lat=0.6723906085905078, lon=-3.0261581679831E-12([X=0.7821883235431606, Y=-2.367025584191143E-12, Z=0.6227413298552851])]], internalEdges={0}}]}
    */
    
    final GeoPoint quantized = new GeoPoint(0.24162356556559528, 2.3309121299774915E-10, 0.9682657049003708);
    final GeoPoint unquantized = new GeoPoint(PlanetModel.WGS84, 1.3262481806651818, 2.4457272005608357E-47);

    // This passes; the point is definitely within the solid.
    assertTrue(solid.isWithin(unquantized));

    // This passes, so I assume that this is the correct response.
    assertFalse(largeShape.isWithin(unquantized));
    // This fails because the point is within the shape but apparently shouldn't be.
    // Instrumenting isWithin finds that the point is on three edge planes somehow:
    /*
   [junit4]   1> localIsWithin start for point [0.2416235655409041,5.90945326539883E-48,0.9682657046994557]
   [junit4]   1>  For edge [A=-1.224646799147353E-16, B=-1.0, C=-7.498798913309287E-33, D=0.0, side=1.0] the point evaluation is -2.959035261382389E-17
   [junit4]   1>  For edge [A=-3.0261581679831E-12, B=-0.9999999999999999, C=-1.8529874570670608E-28, D=0.0, side=1.0] the point evaluation is -7.31191126438807E-13
   [junit4]   1>  For edge [A=4.234084035470679E-12, B=1.0, C=-1.5172037954732973E-12, D=0.0, side=1.0] the point evaluation is -4.460019207463956E-13
    */
    // These are too close to parallel.  The only solution is to prevent the poly from being created.  Let's see if Geo3d thinks they are parallel.
    
    final Plane p1 = new Plane(-1.224646799147353E-16, -1.0, -7.498798913309287E-33, 0.0);
    final Plane p2 = new Plane(-3.0261581679831E-12, -0.9999999999999999, -1.8529874570670608E-28, 0.0);
    final Plane p3 = new Plane(4.234084035470679E-12, 1.0, -1.5172037954732973E-12, 0.0);
    
    assertFalse(shape.isWithin(unquantized));
    
    // This point is indeed outside the shape but it doesn't matter
    assertFalse(shape.isWithin(quantized));
    
    // Sanity check with different poly implementation
    assertTrue(shape.isWithin(edge1) == largeShape.isWithin(edge1));
    assertTrue(shape.isWithin(edge2) == largeShape.isWithin(edge2));
    assertTrue(shape.isWithin(edge3) == largeShape.isWithin(edge3));
    assertTrue(shape.isWithin(edge4) == largeShape.isWithin(edge4));
    
    // Verify both shapes give the same relationship
    int intersection = solid.getRelationship(shape);
    int largeIntersection = solid.getRelationship(largeShape);
    assertTrue(intersection == largeIntersection);
  }

  @Test
  public void testComplexPolygonPlaneOutsideWorld() {
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.5, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.5, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.5, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE,0.5, -0.5));
    GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    //Large polygon with arbitrary testPoint in set
    GeoPolygon largePolygon = new GeoComplexPolygon(PlanetModel.SPHERE, Collections.singletonList(points), new GeoPoint(PlanetModel.SPHERE, 0.25, 0), true);
    //This point is ok
    GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-8);
    assertTrue(polygon.isWithin(point1) == largePolygon.isWithin(point1));
    //This point is ok
    point1 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-5);
    assertTrue(polygon.isWithin(point1) == largePolygon.isWithin(point1));
    //Fails here
    point1 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-7);
    assertTrue(polygon.isWithin(point1) == largePolygon.isWithin(point1));
  }

  @Test
  public void testComplexPolygonDegeneratedVector() {
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.5, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.5, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.5, 0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE,0.5, -0.5));
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points);
    //Large polygon with test point in (0,0)
    final GeoPolygon largePolygon = new GeoComplexPolygon(PlanetModel.SPHERE, Collections.singletonList(points), new GeoPoint(PlanetModel.SPHERE, 0.0, 0), true);
    //Chooses Plane Z and succeed
    final GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-5);
    assertTrue(polygon.isWithin(point1) == largePolygon.isWithin(point1));
    //Numerically identical
    final GeoPoint point2 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-13);
    assertTrue(polygon.isWithin(point2) == largePolygon.isWithin(point2));
    //Fails here, chooses plane X
    final GeoPoint point3 = new GeoPoint(PlanetModel.SPHERE, 0, 1e-6);
    assertTrue(polygon.isWithin(point3) == largePolygon.isWithin(point3));
  }

  @Test
  public void testAboveBelowCrossingDifferentEdges() {
    //POLYGON((130.846821906638 -5.066128831305991,134.5635278421427 21.75703481126756,156.31803093908155 44.5755831677161,0.0 8.860146581178396E-33,130.846821906638 -5.066128831305991))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.066128831305991), Geo3DUtil.fromDegrees(130.846821906638)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(21.75703481126756), Geo3DUtil.fromDegrees(134.5635278421427)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(44.5755831677161), Geo3DUtil.fromDegrees(156.31803093908155)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(8.860146581178396E-33), Geo3DUtil.fromDegrees(0.0)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(-15.37308034708334 1.3353777223310798E-11)
    final GeoPoint point1 = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(1.3353777223310798E-11), Geo3DUtil.fromDegrees(-15.37308034708334));
    assertTrue(polygon.isWithin(point1) == largePolygon.isWithin(point1));
  }

  @Test
  public void testBelowCrossingTwiceEdgePoint() {
    //POLYGON((162.9024012378976 -0.17652184258966092,162.56882659034474 -0.009075185910497524,162.52932263918404 1.6235907240799453E-189,162.17731099253956 -0.2154890860855618,162.9024012378976 -0.17652184258966092))
    List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.17652184258966092), Geo3DUtil.fromDegrees(162.9024012378976)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.009075185910497524), Geo3DUtil.fromDegrees(162.56882659034474)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(1.6235907240799453E-189), Geo3DUtil.fromDegrees(162.52932263918404)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.2154890860855618), Geo3DUtil.fromDegrees(162.17731099253956)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(91.60559215160585 -6.782152464351765E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-6.782152464351765E-11), Geo3DUtil.fromDegrees(91.60559215160585));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8245() {
    //POLYGON((-70.19447784626787 -83.117346007187,0.0 2.8E-322,-139.99870438810106 7.994601469571884,-143.14292702670522 -18.500141088122664,-158.7373186858464 -35.42942085357812,-70.19447784626787 -83.117346007187))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-83.117346007187), Geo3DUtil.fromDegrees(-70.19447784626787)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.8E-322), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(7.994601469571884), Geo3DUtil.fromDegrees(-139.99870438810106)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-18.500141088122664), Geo3DUtil.fromDegrees(-143.14292702670522)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-35.42942085357812), Geo3DUtil.fromDegrees(-158.7373186858464)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(-1.91633079336513E-11 12.282452091883385)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(12.282452091883385), Geo3DUtil.fromDegrees(-1.91633079336513E-11));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8245_case2() {
    //POLYGON((5.512285089810178 -26.833721534785912,12.13983320542565 -16.085163683089583,4.868755337835201 -9.167423203860656,0.0 -5.261747514529465,-15.696549288211289 -21.362181191487718,5.512285089810178 -26.833721534785912))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-26.833721534785912), Geo3DUtil.fromDegrees(5.512285089810178)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-16.085163683089583), Geo3DUtil.fromDegrees(12.13983320542565)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-9.167423203860656), Geo3DUtil.fromDegrees(4.868755337835201)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.261747514529465), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-21.362181191487718), Geo3DUtil.fromDegrees(-15.696549288211289)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(-6.994273817216168E-11 -1.6915596606526662E-292)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-1.6915596606526662E-292), Geo3DUtil.fromDegrees(-6.994273817216168E-11));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8245_case3() {
    //POLYGON((144.76249846857021 8.828705232593283,166.00162989841027 -8.5E-322,157.03429484830787 64.92565566857392,108.64696979831984 39.10241638996957,102.54234512410089 20.471658760034586,144.76249846857021 8.828705232593283))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(8.828705232593283), Geo3DUtil.fromDegrees(144.76249846857021)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-8.5E-322), Geo3DUtil.fromDegrees(166.00162989841027)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(64.92565566857392), Geo3DUtil.fromDegrees(157.03429484830787)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(39.10241638996957), Geo3DUtil.fromDegrees(108.64696979831984)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(20.471658760034586), Geo3DUtil.fromDegrees(102.54234512410089)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(179.9999999999998 7.627654408067997E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(7.627654408067997E-11), Geo3DUtil.fromDegrees(179.9999999999998));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8245_case4() {
    //POLYGON((-3.728795716978514 -10.354090605548162,-137.97868338527985 0.05602723926521642,-113.87317441507611 -76.2471400450585,-162.64032677742279 -89.9999999991684,179.9999999999998 -89.99999999999997,-3.728795716978514 -10.354090605548162))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-10.354090605548162), Geo3DUtil.fromDegrees(-3.728795716978514)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.05602723926521642), Geo3DUtil.fromDegrees(-137.97868338527985)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-76.2471400450585), Geo3DUtil.fromDegrees(-113.87317441507611)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-89.9999999991684), Geo3DUtil.fromDegrees(-162.64032677742279)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-89.99999999999997), Geo3DUtil.fromDegrees(179.9999999999998)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));
    //POINT(-1.2862855990004445E-10 -39.178517830976105)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-39.178517830976105), Geo3DUtil.fromDegrees(-1.2862855990004445E-10));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }
  
  @Test
  public void testLUCENE8251() {
    //POLYGON((135.63207358036593 -51.43541696593334,113.00782694696038 -58.984559858566556,0.0 -3.68E-321,-66.33598777585381 -7.382056816201731,135.63207358036593 -51.43541696593334))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-51.43541696593334), Geo3DUtil.fromDegrees(135.63207358036593)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-58.984559858566556), Geo3DUtil.fromDegrees(113.00782694696038)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-3.68E-321), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-7.382056816201731), Geo3DUtil.fromDegrees(-66.33598777585381)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-51.43541696593334), Geo3DUtil.fromDegrees(135.63207358036593)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(0.005183505059185348 1.98E-321)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(1.98E-321), Geo3DUtil.fromDegrees(0.005183505059185348));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8257() {
    //POLYGON((12.9610296281349 -8.35317290232106,15.448601008878832 -3.990004427754539,22.375905319231205 0.2308875600810982,-13.473550791109867 30.10483127471788,-17.854443360411242 33.07441476406424,-3.928621142543736E-11 4.688559453373203E-11,0.0 -5.546974900361278E-104,12.9610296281349 -8.35317290232106))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-8.35317290232106), Geo3DUtil.fromDegrees(12.9610296281349)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-3.990004427754539), Geo3DUtil.fromDegrees(15.448601008878832)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(0.2308875600810982), Geo3DUtil.fromDegrees(22.375905319231205)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(30.10483127471788), Geo3DUtil.fromDegrees(-13.473550791109867)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(33.07441476406424), Geo3DUtil.fromDegrees(-17.854443360411242)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(4.688559453373203E-11), Geo3DUtil.fromDegrees(-3.928621142543736E-11)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-5.546974900361278E-104), Geo3DUtil.fromDegrees(0.0)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(-179.99999999999997 -9.638811778842766E-12)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-9.638811778842766E-12), Geo3DUtil.fromDegrees(-179.99999999999997));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8258() {
    //POLYGON((0.004541088101890366 2.457524007073783E-4,0.003771467014711204 0.0011493732122651466,0.003975546116981415 0.002208372357731988,0.0010780690991920934 0.0014120274287707404,0.0 2.8E-322,7.486881020702663E-4 -3.4191957123300967E-4,-8.981008225032098E-4 -0.0032334745041058812,0.004541088101890366 2.457524007073783E-4))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.457524007073783E-4), Geo3DUtil.fromDegrees(0.004541088101890366)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.0011493732122651466), Geo3DUtil.fromDegrees(0.003771467014711204)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.002208372357731988), Geo3DUtil.fromDegrees(0.003975546116981415)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.0014120274287707404), Geo3DUtil.fromDegrees(0.0010780690991920934)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.8E-322), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-3.4191957123300967E-4), Geo3DUtil.fromDegrees(7.486881020702663E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.0032334745041058812), Geo3DUtil.fromDegrees(-8.981008225032098E-4)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(1.413E-321 2.104316138623836E-4)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.104316138623836E-4), Geo3DUtil.fromDegrees(1.413E-321));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8266_case1() {
    //POLYGON((-6.35093158794635E-11 -4.965517818537545E-11,0.0 3.113E-321,-60.23538585411111 18.46706692248612, 162.37100340450482 -25.988383239097754,-6.35093158794635E-11 -4.965517818537545E-11))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-4.965517818537545E-11), Geo3DUtil.fromDegrees(-6.35093158794635E-11)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(3.113E-321), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(18.46706692248612), Geo3DUtil.fromDegrees(-60.23538585411111)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-25.988383239097754), Geo3DUtil.fromDegrees(162.37100340450482)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(-179.99999999999974 2.4432260684194717E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.4432260684194717E-11), Geo3DUtil.fromDegrees(-179.99999999999974));
    assertFalse(polygon.isWithin(point));
    assertFalse(largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8266_case2() {
    //POLYGON((7.885596306952593 -42.25131029665893,1.5412637897085604 -6.829581354691802,34.03338913004999 27.583811665797796,0.0 5.7E-322,-8.854664233194431E-12 7.132883127401669E-11,-40.20723013296905 15.679563923063258,7.885596306952593 -42.25131029665893))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-42.25131029665893), Geo3DUtil.fromDegrees(7.885596306952593)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-6.829581354691802), Geo3DUtil.fromDegrees(1.5412637897085604)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(27.583811665797796), Geo3DUtil.fromDegrees(34.03338913004999)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(5.7E-322), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(7.132883127401669E-11), Geo3DUtil.fromDegrees( -8.854664233194431E-12)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(15.679563923063258), Geo3DUtil.fromDegrees(-40.20723013296905)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(-179.99999999999983 -8.474427850967216E-12)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-8.474427850967216E-12), Geo3DUtil.fromDegrees(-179.99999999999983));
    assertFalse(polygon.isWithin(point));
    assertFalse(largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8266_case3() {
    //POLYGON((-98.38897266664411 7.286530349760722,-169.07259176302364 -7.410435277740526,8E-123,-179.9999999999438 -1.298973436027626E-10,66.2759716901292 -52.84327866278771,-98.38897266664411 7.286530349760722))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(7.286530349760722), Geo3DUtil.fromDegrees(-98.38897266664411)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-7.410435277740526), Geo3DUtil.fromDegrees(-169.07259176302364)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-8.136646215781618E-123), Geo3DUtil.fromDegrees(-180.0)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-1.298973436027626E-10), Geo3DUtil.fromDegrees(-179.9999999999438)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-52.84327866278771), Geo3DUtil.fromDegrees(66.2759716901292)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(3.4279315107728157E-122 2.694960611439045E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(2.694960611439045E-11), Geo3DUtil.fromDegrees(3.4279315107728157E-122));
    assertFalse(polygon.isWithin(point));
    assertFalse(largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8276_case1() {
    //POLYGON((1.0517792672527197E-4 -1.592702733911458E-5,1.0324192726355287E-4 2.5741558803919037E-5,7.879018764391666E-5 7.192932029677136E-5,0.0 9.400459451570553E-24,3.50020551583809E-5 -6.508699856255637E-5,1.0517792672527197E-4 -1.592702733911458E-5))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-1.592702733911458E-5), Geo3DUtil.fromDegrees(1.0517792672527197E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(2.5741558803919037E-5), Geo3DUtil.fromDegrees(1.0324192726355287E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(7.192932029677136E-5), Geo3DUtil.fromDegrees(7.879018764391666E-5)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(9.400459451570553E-24), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-6.508699856255637E-5), Geo3DUtil.fromDegrees(3.50020551583809E-5)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(-1.13E-321 2.83E-321)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-1.13E-321), Geo3DUtil.fromDegrees(-1.13E-321));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8276_case2() {
    //POLYGON((0.05925400271049228 -0.08922986460239596,0.07309863706879852 -0.07813330646578831,0.07411491387725304 -0.07715685640120272,0.0 -2.8E-322,-0.005013788374470427 0.06774540608427036,-0.09349862417147398 0.051577774969906794,-0.10359306491815146 -0.02537375818592368,0.05925400271049228 -0.08922986460239596))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.08922986460239596), Geo3DUtil.fromDegrees(0.05925400271049228)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.07813330646578831), Geo3DUtil.fromDegrees(0.07309863706879852)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.07715685640120272), Geo3DUtil.fromDegrees(0.07411491387725304)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-2.8E-322), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.06774540608427036), Geo3DUtil.fromDegrees(-0.005013788374470427)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(0.051577774969906794), Geo3DUtil.fromDegrees(-0.09349862417147398)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.02537375818592368), Geo3DUtil.fromDegrees(-0.10359306491815146)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(9.020991048228685E-4 -2.5357127427108625E-98)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-2.5357127427108625E-98), Geo3DUtil.fromDegrees(9.020991048228685E-4));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8276")
  public void testLUCENE8276_case3() {
    //POLYGON((2.693381024483753E-4 -0.001073608118084019,1.5848404608659423E-4 -2.6378130512803985E-4,8.981079660799132E-4 -6.4697719116416E-4,-7.934854852157693E-5 4.193687767358618E-4,0.0 8.013660459916381E-131,-3.968797970346633E-4 3.2057826073172334E-4,2.693381024483753E-4 -0.001073608118084019))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-0.001073608118084019), Geo3DUtil.fromDegrees(2.693381024483753E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-2.6378130512803985E-4), Geo3DUtil.fromDegrees(1.5848404608659423E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-6.4697719116416E-4), Geo3DUtil.fromDegrees(8.981079660799132E-4)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(4.193687767358618E-4), Geo3DUtil.fromDegrees(-7.934854852157693E-5)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(8.013660459916381E-131), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(3.2057826073172334E-4), Geo3DUtil.fromDegrees(-3.968797970346633E-4)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(-2.394808631784144E-4 5.7E-322)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(5.7E-322), Geo3DUtil.fromDegrees(-2.394808631784144E-4));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }
  
  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8281")
  public void testLUCENE8281() {
    /*
   [junit4]    > Standard polygon: GeoCompositePolygon: {[GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[[lat=-3.89514302068452E-6, lon=6.597839410815709E-6([X=1.0011188539630433, Y=6.605221429683868E-6, Z=-3.89950111699443E-6])], [lat=-2.8213942160840002E-6, lon=1.608008770581648E-5([X=1.0011188538590383, Y=1.60980789753873E-5, Z=-2.8245509442632E-6])], [lat=3.8977187534179774E-6, lon=1.9713406091526053E-5([X=1.0011188537902969, Y=1.973546251320774E-5, Z=3.902079731596721E-6])], [lat=1.980614928404974E-5, lon=4.069266235973146E-6([X=1.0011188537865057, Y=4.07381914993205E-6, Z=1.982830947192924E-5])], [lat=7.4E-323, lon=0.0([X=1.0011188539924791, Y=0.0, Z=7.4E-323])]], internalEdges={4}}, GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=[[lat=-3.89514302068452E-6, lon=6.597839410815709E-6([X=1.0011188539630433, Y=6.605221429683868E-6, Z=-3.89950111699443E-6])], [lat=7.4E-323, lon=0.0([X=1.0011188539924791, Y=0.0, Z=7.4E-323])], [lat=-1.261719663233924E-5, lon=-1.5701544210600105E-5([X=1.001118853788849, Y=-1.5719111944122703E-5, Z=-1.2631313432823314E-5])]], internalEdges={0}}]}
   [junit4]    > Large polygon: GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=d8738cf, testPoint=[lat=7.28355694648262E-7, lon=5.126509206005681E-6([X=1.0011188539790565, Y=5.13224502127445E-6, Z=7.291706183250984E-7])], testPointInSet=true, shapes={ {[lat=-1.261719663233924E-5, lon=-1.5701544210600105E-5([X=1.001118853788849, Y=-1.5719111944122703E-5, Z=-1.2631313432823314E-5])], [lat=-3.89514302068452E-6, lon=6.597839410815709E-6([X=1.0011188539630433, Y=6.605221429683868E-6, Z=-3.89950111699443E-6])], [lat=-2.8213942160840002E-6, lon=1.608008770581648E-5([X=1.0011188538590383, Y=1.60980789753873E-5, Z=-2.8245509442632E-6])], [lat=3.8977187534179774E-6, lon=1.9713406091526053E-5([X=1.0011188537902969, Y=1.973546251320774E-5, Z=3.902079731596721E-6])], [lat=1.980614928404974E-5, lon=4.069266235973146E-6([X=1.0011188537865057, Y=4.07381914993205E-6, Z=1.982830947192924E-5])], [lat=7.4E-323, lon=0.0([X=1.0011188539924791, Y=0.0, Z=7.4E-323])]}}
   [junit4]    > Point: [lat=4.983019447098944E-6, lon=-3.0E-323([X=1.0011188539799663, Y=-3.0E-323, Z=4.98859471828087E-6])]
   [junit4]    > WKT: POLYGON((
        3.7802835214482185E-4 -2.2317525568506174E-4,
        9.213211597434869E-4 -1.6165398092423463E-4,
        0.0011294949688719308 2.233228342998425E-4,
        2.3315178103634778E-4 0.0011348087623821073,
        0.0 4.244E-321,
        -8.996322151054578E-4 -7.22912116319714E-4,
        3.7802835214482185E-4 -2.2317525568506174E-4))
   [junit4]    > WKT: POINT(-1.7E-321 2.855059835503825E-4)
    */
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-2.2317525568506174E-4), Geo3DUtil.fromDegrees(3.7802835214482185E-4)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-1.6165398092423463E-4), Geo3DUtil.fromDegrees(9.213211597434869E-4)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(2.233228342998425E-4), Geo3DUtil.fromDegrees(0.0011294949688719308)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(0.0011348087623821073), Geo3DUtil.fromDegrees(2.3315178103634778E-4)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(4.244E-321), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-7.22912116319714E-4), Geo3DUtil.fromDegrees(-8.996322151054578E-4)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-2.2317525568506174E-4), Geo3DUtil.fromDegrees(3.7802835214482185E-4)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(-2.394808631784144E-4 5.7E-322)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(2.855059835503825E-4), Geo3DUtil.fromDegrees(-1.7E-321));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));

  }
  
  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8280")
  public void testLUCENE8280() {
    /*
   [junit4]   1>       unquantized=[lat=0.16367268756896675, lon=-3.141592653589793([X=-0.9876510422569805, Y=-1.2095236875745584E-16, Z=0.16311061810965483])]
   [junit4]   1>       quantized=[X=-0.9876510423773649, Y=-2.3309121299774915E-10, Z=0.16311061829120332]
   [junit4]   1>   shape=GeoComplexPolygon: {planetmodel=PlanetModel.WGS84, number of shapes=1, address=7fb785c7, 
    testPoint=[lat=-1.3164421003439726, lon=-0.3852878798825553([X=0.23270178206383424, Y=-0.09437388649617809, Z=-0.9658649833483698])], testPointInSet=true, 
    shapes={ {
    [lat=-0.914670478121684, lon=2.4457272005608357E-47([X=0.609446252447186, Y=1.4905392768899487E-47, Z=-0.7915752112532345])], 
    [lat=-0.737919215699403, lon=-1.0814374159521924([X=0.34764272191418555, Y=-0.6527705659008658, Z=-0.6724777381306498])], 
    [lat=-0.2581712131420987, lon=-3.141592653589793([X=-0.9677277372221494, Y=-1.1851246758352164E-16, Z=-0.2555423342455023])], 
    [lat=-0.40516490647074055, lon=2.4457272005608357E-47([X=0.919584346757591, Y=2.2490524500750083E-47, Z=-0.39440489992508504])], 
    [lat=2.4457272005608357E-47, lon=-0.6244585784444767([X=0.8121874885299789, Y=-0.5853122613567737, Z=2.448463612203698E-47])]}}
   [junit4]   1>   bounds=XYZBounds: [xmin=-1.0011188549924792 xmax=1.0011188549924792 ymin=-0.6616249691360604 ymax=1.0E-9 zmin=-0.9977622930221051 zmax=1.0E-9]
    */
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, -0.914670478121684, 2.4457272005608357E-47));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.737919215699403, -1.0814374159521924));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.2581712131420987, -3.141592653589793));
    points.add(new GeoPoint(PlanetModel.WGS84, -0.40516490647074055, 2.4457272005608357E-47));
    points.add(new GeoPoint(PlanetModel.WGS84, 2.4457272005608357E-47, -0.6244585784444767));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    // I think this polygon may cross itself around lat=-0.91, lon=0.  If so, this is an invalid test.
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, 0.16367268756896675, -3.141592653589793);
    assertFalse(largePolygon.isWithin(point));

    /* Confirmed that bounds is OK
    final XYZBounds xyzBounds = new XYZBounds();
    largePolygon.getBounds(xyzBounds);
    
    System.out.println("North pole is within? "+largePolygon.isWithin(PlanetModel.WGS84.NORTH_POLE));
    System.out.println("South pole is within? "+largePolygon.isWithin(PlanetModel.WGS84.SOUTH_POLE));
    
    final XYZSolid xyzSolid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, xyzBounds);
    // Failure is due either to bounds computation or multiple points having their in-set status wrongly assessed.
    // Probably it is the former because there are more than a dozen points that otherwise fail to be correct.
    assertTrue(largePolygon.isWithin(point)?xyzSolid.isWithin(point):true);
    */
    
  }
  
  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8337")  
  public void testLUCENE8337() {
    /*
   {planetmodel=PlanetModel.WGS84, number of shapes=1, address=c865f21d, 
   testPoint=[lat=2.114284741800425E-5, lon=-3.141516973708951([X=-1.0011188509002849, Y=-7.57645554894811E-5, Z=2.1166503175641402E-5])], testPointInSet=true, shapes={ 
{[lat=1.4379241972924144E-5, lon=-3.141520309370815([X=-1.0011188512685139, Y=-7.24251615257059E-5, Z=1.4395330244708275E-5])], 
[lat=-1.858900171939205E-5, lon=-3.1415059739464217([X=-1.001118850057461, Y=-8.677662511280753E-5, Z=-1.860980009708855E-5])], 
[lat=2.7071641284581073E-5, lon=-3.141469177092562([X=-1.001118845991408, Y=-1.2361464904363391E-4, Z=2.7101930495137982E-5])], 
[lat=8.285235549000288E-5, lon=-3.1414967545451287([X=-1.0011188459297669, Y=-9.600634121467467E-5, Z=8.29450550819143E-5])], 
[lat=-8.956596846349593E-303, lon=-3.1415926535897922([X=-1.0011188539924791, Y=-1.0117738616818362E-15, Z=-8.966617970490158E-303])]}}

   [junit4]    > Point: [lat=-6.499661194605612E-10, lon=-2.0286460544410216([X=-0.4425148814082194, Y=-0.8980086522698344, Z=-6.506933366482957E-10])]
    */
    
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, 1.4379241972924144E-5, -3.141520309370815));
    points.add(new GeoPoint(PlanetModel.WGS84, -1.858900171939205E-5, -3.1415059739464217));
    points.add(new GeoPoint(PlanetModel.WGS84, 2.7071641284581073E-5, -3.141469177092562));
    points.add(new GeoPoint(PlanetModel.WGS84, 8.285235549000288E-5, -3.1414967545451287));
    points.add(new GeoPoint(PlanetModel.WGS84, -8.956596846349593E-303, -3.1415926535897922));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);

    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));
    final GeoPolygon smallPolygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    
    final GeoPoint thePoint = new GeoPoint(PlanetModel.WGS84, -6.499661194605612E-10, -2.0286460544410216);
    
    System.out.println("large inset: "+largePolygon.isWithin(thePoint));
    
    assertTrue(largePolygon.isWithin(thePoint) == smallPolygon.isWithin(thePoint));
    
  }

  @Test
  public void testLUCENE8444() {
    //POLYGON((0.0 -67.68132244526963,-1.2477695347678826E-95 -88.11137674490907, 1.7059188343238906E-9 7.009654350320916,0.0 -67.68132244526963))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-67.68132244526963), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-88.11137674490907), Geo3DUtil.fromDegrees(-1.2477695347678826E-95)));
    points.add(new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(7.009654350320916), Geo3DUtil.fromDegrees(1.7059188343238906E-9)));

    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.WGS84, Collections.singletonList(description));

    //POINT(180.0 -61.73229670194638)
    final GeoPoint point = new GeoPoint(PlanetModel.WGS84, Geo3DUtil.fromDegrees(-61.73229670194638), Geo3DUtil.fromDegrees(180.0));
    //assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
    assertTrue(false == largePolygon.isWithin(point));
  }

  @Test
  public void testLUCENE8445() {
    //POLYGON((32.18017946378854 -17.397683785381247,49.51018758330871 -9.870219317504647,58.77903721991479 33.90553510354402,2.640604559432277 9.363173880050821,3.1673235739886286E-10 8.853669066894417E-11,0.0 -5.7E-322,4.820339742500488E-5 5.99784517213369E-7,32.18017946378854 -17.397683785381247))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-17.397683785381247), Geo3DUtil.fromDegrees(32.18017946378854)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-9.870219317504647), Geo3DUtil.fromDegrees(49.51018758330871)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(33.90553510354402), Geo3DUtil.fromDegrees(58.77903721991479)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(9.363173880050821), Geo3DUtil.fromDegrees(2.640604559432277)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(8.853669066894417E-11), Geo3DUtil.fromDegrees(3.1673235739886286E-10)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.7E-322), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(5.99784517213369E-7), Geo3DUtil.fromDegrees(4.820339742500488E-5)));

    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(179.99999999999983 -5.021400461974724E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.021400461974724E-11), Geo3DUtil.fromDegrees(179.99999999999983));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));
  }

  @Test
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8451")  
  public void testLUCENE8451() {
    //POLYGON((-2.5185339401969213 -24.093993739745027,0.0 8.828539494442529E-27,5.495998489568957E-11 -8.321407453133E-11,2.7174659198424288E-11 1.0260761462208114E-10,88.32137548549387 16.934529875343244,-87.97237709688223 39.919704493657484,-88.0876897472551 34.91204903885665,-2.5185339401969213 -24.093993739745027))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-24.093993739745027), Geo3DUtil.fromDegrees(-2.5185339401969213)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(8.828539494442529E-27), Geo3DUtil.fromDegrees(0.0)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-8.321407453133E-11), Geo3DUtil.fromDegrees(5.495998489568957E-11)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(1.0260761462208114E-10), Geo3DUtil.fromDegrees(2.7174659198424288E-11)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(16.934529875343244), Geo3DUtil.fromDegrees(88.32137548549387)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(39.919704493657484), Geo3DUtil.fromDegrees(-87.97237709688223)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(34.91204903885665), Geo3DUtil.fromDegrees(-88.0876897472551)));
    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(179.99999999999983 -5.021400461974724E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.021400461974724E-11), Geo3DUtil.fromDegrees(179.99999999999983));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));

  }
  
  //@AwaitsFix(bugUrl="https://issues.apache.org/jira/browse/LUCENE-8512")
  public void testLUCENE8512() {
    //POLYGON((35.4190030282028 -67.85799140154762,35.420218772379776 -67.85786846162631,35.42021877254679 -67.85786846168897,35.420218772734266 -67.85786846168025,35.4190030282028 -67.85799140154762))
    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-67.85799140154762), Geo3DUtil.fromDegrees(35.4190030282028)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-67.85786846162631), Geo3DUtil.fromDegrees(35.420218772379776)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-67.85786846168897), Geo3DUtil.fromDegrees(35.42021877254679)));
    points.add(new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-67.85786846168025), Geo3DUtil.fromDegrees(35.420218772734266)));

    final GeoPolygonFactory.PolygonDescription description = new GeoPolygonFactory.PolygonDescription(points);
    final GeoPolygon polygon = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, description);
    final GeoPolygon largePolygon = GeoPolygonFactory.makeLargeGeoPolygon(PlanetModel.SPHERE, Collections.singletonList(description));

    //POINT(179.99999999999983 -5.021400461974724E-11)
    final GeoPoint point = new GeoPoint(PlanetModel.SPHERE, Geo3DUtil.fromDegrees(-5.021400461974724E-11), Geo3DUtil.fromDegrees(179.99999999999983));
    assertTrue(polygon.isWithin(point) == largePolygon.isWithin(point));

  }
}
