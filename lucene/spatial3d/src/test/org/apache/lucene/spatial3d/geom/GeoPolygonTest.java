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
  
}
