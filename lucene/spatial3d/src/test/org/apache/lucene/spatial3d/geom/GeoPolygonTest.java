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
import org.junit.Ignore;

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
      final List<GeoPoint> filteredPoints =GeoPolygonFactory.filterPoints(originalPoints, 0.0);
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
      final List<GeoPoint> filteredPoints =GeoPolygonFactory.filterPoints(originalPoints, 0.0);
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
      final List<GeoPoint> filteredPoints =GeoPolygonFactory.filterPoints(originalPoints, 0.0);
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
      System.err.println("Before: "+originalPoints);
      final List<GeoPoint> filteredPoints =GeoPolygonFactory.filterPoints(originalPoints, 0.0);
      System.err.println("After: "+filteredPoints);
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

  }

  @Test
  public void testPolygonPointWithin() {
    GeoPolygon c;
    GeoPoint gp;
    List<GeoPoint> points;

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
  
}
