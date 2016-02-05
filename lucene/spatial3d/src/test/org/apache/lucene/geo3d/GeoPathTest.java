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
package org.apache.lucene.geo3d;

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
    GeoPath c;
    GeoPoint point;
    GeoPoint pointApprox;
    int relationship;
    GeoArea area;
    PlanetModel planetModel;
    
    planetModel = new PlanetModel(1.151145876105594, 0.8488541238944061);
    c = new GeoPath(planetModel, 0.008726646259971648);
    c.addPoint(-0.6925658899376476, 0.6316613927914589);
    c.addPoint(0.27828548161836364, 0.6785795524104564);
    c.done();
    point = new GeoPoint(planetModel,-0.49298555067758226, 0.9892440995026406);
    pointApprox = new GeoPoint(0.5110940362119821, 0.7774603209946239, -0.49984312299556544);
    area = GeoAreaFactory.makeGeoArea(planetModel, 0.49937141144985997, 0.5161765426256085, 0.3337218719537796,0.8544419570901649, -0.6347692823688085, 0.3069696588119369);
    assertTrue(!c.isWithin(point));
    
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
    LatLonBounds b;
    XYZBounds xyzb;
    GeoPoint point;
    int relationship;
    GeoArea area;
    PlanetModel planetModel;
    
    planetModel = new PlanetModel(0.751521665790406,1.248478334209594);
    c = new GeoPath(planetModel, 0.7504915783575618);
    c.addPoint(0.10869761172400265, 0.08895880215465272);
    c.addPoint(0.22467878641991612, 0.10972973084229565);
    c.addPoint(-0.7398772468744732, -0.4465812941383364);
    c.addPoint(-0.18462055300079366, -0.6713857796763727);
    c.done();
    point = new GeoPoint(planetModel,-0.626645355125733,-1.409304625439381);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(planetModel,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.WITHIN || relationship == GeoArea.OVERLAPS);
    assertTrue(area.isWithin(point));
    // No longer true due to fixed GeoPath waypoints.
    //assertTrue(c.isWithin(point));
    
    c = new GeoPath(PlanetModel.WGS84, 0.6894050545377601);
    c.addPoint(-0.0788176065762948, 0.9431251741731624);
    c.addPoint(0.510387871458147, 0.5327078872484678);
    c.addPoint(-0.5624521609859962, 1.5398841746888388);
    c.addPoint(-0.5025171434638661, -0.5895998642788894);
    c.done();
    point = new GeoPoint(PlanetModel.WGS84, 0.023652082107211682, 0.023131910152748437);
    //System.err.println("Point.x = "+point.x+"; point.y="+point.y+"; point.z="+point.z);
    assertTrue(c.isWithin(point));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println("minx="+xyzb.getMinimumX()+" maxx="+xyzb.getMaximumX()+" miny="+xyzb.getMinimumY()+" maxy="+xyzb.getMaximumY()+" minz="+xyzb.getMinimumZ()+" maxz="+xyzb.getMaximumZ());
    //System.err.println("point.x="+point.x+" point.y="+point.y+" point.z="+point.z);
    relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.WITHIN || relationship == GeoArea.OVERLAPS);
    assertTrue(area.isWithin(point));
    
    c = new GeoPath(PlanetModel.WGS84, 0.7766715171374766);
    c.addPoint(-0.2751718361148076, -0.7786721269011477);
    c.addPoint(0.5728375851539309, -1.2700115736820465);
    c.done();
    point = new GeoPoint(PlanetModel.WGS84, -0.01580760332365284, -0.03956004622490505);
    assertTrue(c.isWithin(point));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println("minx="+xyzb.getMinimumX()+" maxx="+xyzb.getMaximumX()+" miny="+xyzb.getMinimumY()+" maxy="+xyzb.getMaximumY()+" minz="+xyzb.getMinimumZ()+" maxz="+xyzb.getMaximumZ());
    //System.err.println("point.x="+point.x+" point.y="+point.y+" point.z="+point.z);
    relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.WITHIN || relationship == GeoArea.OVERLAPS);
    assertTrue(area.isWithin(point));

    c = new GeoPath(PlanetModel.SPHERE, 0.1);
    c.addPoint(-0.3, -0.3);
    c.addPoint(0.3, 0.3);
    c.done();

    b = new LatLonBounds();
    c.getBounds(b);
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
