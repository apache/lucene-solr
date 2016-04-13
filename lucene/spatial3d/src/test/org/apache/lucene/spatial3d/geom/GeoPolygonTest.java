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
    
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, 1);
    
    point = new GeoPoint(PlanetModel.WGS84, -0.01580760332365284, -0.03956004622490505);
    assertTrue(c.isWithin(point));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(area.isWithin(point));
    
    points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, -0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.6));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.1, -0.5));
    points.add(new GeoPoint(PlanetModel.SPHERE, 0.0, -0.4));

    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points, 0);

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
    c = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, 3);
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

    final GeoPolygon p = GeoPolygonFactory.makeGeoPolygon(PlanetModel.WGS84, points, null);
  }
  
  @Test
  public void testPolygonIntersectionFailure1() {
    final PlanetModel pm = PlanetModel.WGS84;
    //[junit4]    > Throwable #1: java.lang.AssertionError: invalid hits for shape=GeoCompositeMembershipShape:
    //{[GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
    //[[lat=0.2669499069140678, lon=-0.31249902828113546([X=0.9186752334433793, Y=-0.2968103450748192, Z=0.2640238502385029])],
    //[lat=1.538559019421765, lon=0.0([X=0.03215971057004023, Y=0.0, Z=0.9972473454662941])],
    //[lat=-0.5516194571595735, lon=0.0([X=0.8518418310766115, Y=0.0, Z=-0.5241686363384119])]], internalEdges={2}},
    //GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
    //[[lat=0.0, lon=-3.141592653589793([X=-1.0011188539924791, Y=-1.226017000107956E-16, Z=0.0])],
    //[lat=-1.5707963267948966, lon=-2.2780601241431375([X=-3.9697069088211677E-17, Y=-4.644115432258393E-17, Z=-0.997762292022105])],
    //[lat=0.2669499069140678, lon=-0.31249902828113546([X=0.9186752334433793, Y=-0.2968103450748192, Z=0.2640238502385029])]], internalEdges={2}},
    //GeoConvexPolygon: {planetmodel=PlanetModel.WGS84, points=
    //[[lat=0.2669499069140678, lon=-0.31249902828113546([X=0.9186752334433793, Y=-0.2968103450748192, Z=0.2640238502385029])],
    //[lat=-0.5516194571595735, lon=0.0([X=0.8518418310766115, Y=0.0, Z=-0.5241686363384119])],
    //[lat=0.0, lon=-3.141592653589793([X=-1.0011188539924791, Y=-1.226017000107956E-16, Z=0.0])]], internalEdges={0, 2}}]}
    
    // Build the polygon
    //[[lat=-0.5516194571595735, lon=0.0([X=0.8518418310766115, Y=0.0, Z=-0.5241686363384119])],
    //[lat=0.0, lon=-3.141592653589793([X=-1.0011188539924791, Y=-1.226017000107956E-16, Z=0.0])],
    //[lat=-1.5707963267948966, lon=-2.2780601241431375([X=-3.9697069088211677E-17, Y=-4.644115432258393E-17, Z=-0.997762292022105])],
    //[lat=0.2669499069140678, lon=-0.31249902828113546([X=0.9186752334433793, Y=-0.2968103450748192, Z=0.2640238502385029])],
    //[lat=1.538559019421765, lon=0.0([X=0.03215971057004023, Y=0.0, Z=0.9972473454662941])]]
    List<GeoPoint> polyPoints = new ArrayList<>();
    polyPoints.add(new GeoPoint(pm, -0.5516194571595735, 0.0));
    polyPoints.add(new GeoPoint(pm, 0.0, -3.141592653589793));
    polyPoints.add(new GeoPoint(pm, -1.5707963267948966, -2.2780601241431375));
    polyPoints.add(new GeoPoint(pm, 0.2669499069140678, -0.31249902828113546));
    polyPoints.add(new GeoPoint(pm, 1.538559019421765, 0.0));
    // Make sure we catch the backtrack
    boolean backtracks = false;
    try {
      GeoPolygonFactory.makeGeoPolygon(pm, polyPoints, 4, null);
    } catch (IllegalArgumentException e) {
      backtracks = true;
    }
    assertTrue(backtracks);
    
    // Now make sure a legit poly with coplanar points works.
    polyPoints.clear();
    polyPoints.add(new GeoPoint(pm, -0.5516194571595735, 0.0));
    polyPoints.add(new GeoPoint(pm, -1.5707963267948966, -2.2780601241431375));
    polyPoints.add(new GeoPoint(pm, 0.2669499069140678, -0.31249902828113546));
    polyPoints.add(new GeoPoint(pm, 1.538559019421765, 0.0));
    GeoPolygonFactory.makeGeoPolygon(pm, polyPoints, 3, null);
    
  }
  
}
