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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.lucene.util.LuceneTestCase;

public class GeoCircleTest extends LuceneTestCase {

  @Test
  public void testCircleDistance() {
    GeoCircle c;
    GeoPoint gp;
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
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
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, -0.5, Math.PI);
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
    LatLonBounds b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
  }

  @Test
  public void testCirclePointWithin() {
    GeoCircle c;
    GeoPoint gp;
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
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
    LatLonBounds b;
    XYZBounds xyzb;
    GeoArea area;
    GeoPoint p1;
    GeoPoint p2;
    int relationship;

    // ...
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, -0.005931145568901605, -0.001942031539653079, 1.2991918568260272E-4);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84, 1.001098377143621, 1.001100011578687, -0.00207467080358696, -0.0018136665346280983, -0.006067808248760161, -0.005807683665759485);
    p1 = new GeoPoint(PlanetModel.WGS84, -0.00591253844632244, -0.0020069187259065093);
    p2 = new GeoPoint(1.001099185736782, -0.0020091272069679327, -0.005919118245803968);
    assertTrue(c.isWithin(p1));
    assertTrue(area.isWithin(p1));
    relationship = area.getRelationship(c);
    assertTrue(relationship != GeoArea.DISJOINT);

    // Twelfth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84,-0.00824379317765984,-0.0011677469001838581,0.0011530035396910402);
    p1 = new GeoPoint(PlanetModel.WGS84,-0.006505092992723671,0.007654282718327381);
    p2 = new GeoPoint(1.0010681673665647,0.007662608264336381,-0.006512324005914593);
    assertTrue(!c.isWithin(p1));
    assertTrue(!c.isWithin(p2));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.OVERLAPS || relationship == GeoArea.WITHIN);
    // Point is actually outside the bounds, and outside the shape
    assertTrue(!area.isWithin(p1));
    // Approximate point the same
    assertTrue(!area.isWithin(p2));
    
    // Eleventh BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE,-0.004431288600558495,-0.003687846671278374,1.704543429364245E-8);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println(area);
    relationship = area.getRelationship(c);
    assertTrue(GeoArea.WITHIN == relationship || GeoArea.OVERLAPS == relationship);

    // Tenth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84,-0.0018829770647349636,-0.001969499061382591,1.3045439293158305E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println(area);
    relationship = area.getRelationship(c);
    assertTrue(GeoArea.WITHIN == relationship || GeoArea.OVERLAPS == relationship);

    // Ninth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE,-4.211990380885122E-5,-0.0022958453508173044,1.4318475623498535E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println(area);
    relationship = area.getRelationship(c);
    assertTrue(GeoArea.WITHIN == relationship || GeoArea.OVERLAPS == relationship);
    
    // Eighth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE,0.005321278689117842,-0.00216937368755372,1.5306034422500785E-4);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println(area);
    relationship = area.getRelationship(c);
    assertTrue(GeoArea.WITHIN == relationship || GeoArea.OVERLAPS == relationship);

    // Seventh BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE,-0.0021627146783861745, -0.0017298167021592304,2.0818312293195752E-4);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE, 
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    //System.err.println(area);
    relationship = area.getRelationship(c);
    assertTrue(GeoArea.WITHIN == relationship || GeoArea.OVERLAPS == relationship);

    // Sixth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84,-0.006450320645814321,0.004660694205115142,0.00489710732634323);
    //xyzb = new XYZBounds();
    //c.getBounds(xyzb);
    //System.err.println("xmin="+xyzb.getMinimumX()+", xmax="+xyzb.getMaximumX()+",ymin="+xyzb.getMinimumY()+", ymax="+xyzb.getMaximumY()+",zmin="+xyzb.getMinimumZ()+", zmax="+xyzb.getMaximumZ());
    //xmin=1.0010356621420726, xmax=1.0011141249179447,ymin=-2.5326643901354566E-4, ymax=0.009584741915757169,zmin=-0.011359874956269283, zmax=-0.0015549504447452225
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,1.0010822580620098,1.0010945779732867,0.007079167343247293,0.007541006774427837,-0.0021855011220022575,-0.001896122718181518);
    assertTrue(GeoArea.CONTAINS != area.getRelationship(c));
    /*
    p1 = new GeoPoint(1.0010893045436076,0.007380935180644008,-0.002140671370616495);
    // This has a different bounding box, so we can't use it.
    //p2 = new GeoPoint(PlanetModel.WGS84,-0.002164069780096702, 0.007505617500830066);
    p2 = new GeoPoint(PlanetModel.WGS84,p1.getLatitude(),p1.getLongitude());
    assertTrue(PlanetModel.WGS84.pointOnSurface(p2));
    assertTrue(!c.isWithin(p2));
    assertTrue(!area.isWithin(p2));
    assertTrue(!c.isWithin(p1));
    assertTrue(PlanetModel.WGS84.pointOnSurface(p1)); // This fails
    assertTrue(!area.isWithin(p1)); // This fails
    */
    
    // Fifth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.004282454525970269, -1.6739831367422277E-4, 1.959639723134033E-6);
    assertTrue(c.isWithin(c.getEdgePoints()[0]));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(GeoArea.WITHIN == area.getRelationship(c) || GeoArea.OVERLAPS == area.getRelationship(c));
    
    // Fourth BKD discovered failure
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.0048795517261255, 0.004053904306995974, 5.93699764258874E-6);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    assertTrue(GeoArea.WITHIN == area.getRelationship(c) || GeoArea.OVERLAPS == area.getRelationship(c));
    
    // Yet another test case from BKD
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, 0.006229478708446979, 0.005570196723795424, 3.840276763694387E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.WGS84,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    p1 = new GeoPoint(PlanetModel.WGS84, 0.006224927111830945, 0.005597367237251763);
    p2 = new GeoPoint(1.0010836083810235, 0.005603490759433942, 0.006231850560862502);
    assertTrue(PlanetModel.WGS84.pointOnSurface(p1));
    //assertTrue(PlanetModel.WGS84.pointOnSurface(p2));
    assertTrue(c.isWithin(p1));
    assertTrue(c.isWithin(p2));
    assertTrue(area.isWithin(p1));
    assertTrue(area.isWithin(p2));
    
    // Another test case from BKD
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.005955031040627789, -0.0029274772647399153, 1.601488279374338E-5);
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    area = GeoAreaFactory.makeGeoArea(PlanetModel.SPHERE,
      xyzb.getMinimumX(), xyzb.getMaximumX(), xyzb.getMinimumY(), xyzb.getMaximumY(), xyzb.getMinimumZ(), xyzb.getMaximumZ());
    
    relationship = area.getRelationship(c);
    assertTrue(relationship == GeoArea.WITHIN || relationship == GeoArea.OVERLAPS);
    
    // Test case from BKD
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.765816119338, 0.991848766844, 0.8153163226330487);
    p1 = new GeoPoint(0.7692262265236023, -0.055089298115534646, -0.6365973465711254);
    assertTrue(c.isWithin(p1));
    xyzb = new XYZBounds();
    c.getBounds(xyzb);
    assertTrue(p1.x >= xyzb.getMinimumX() && p1.x <= xyzb.getMaximumX());
    assertTrue(p1.y >= xyzb.getMinimumY() && p1.y <= xyzb.getMaximumY());
    assertTrue(p1.z >= xyzb.getMinimumZ() && p1.z <= xyzb.getMaximumZ());
    
    // Vertical circle cases
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.6, b.getLeftLongitude(), 0.000001);
    assertEquals(-0.4, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.4, b.getLeftLongitude(), 0.000001);
    assertEquals(0.6, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(-0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, Math.PI, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.000001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.000001);
    assertEquals(-0.1, b.getMinLatitude(), 0.000001);
    assertEquals(0.1, b.getMaxLatitude(), 0.000001);
    // Horizontal circle cases
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, Math.PI * 0.5, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertTrue(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(Math.PI * 0.5 - 0.1, b.getMinLatitude(), 0.000001);
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -Math.PI * 0.5, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertTrue(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertTrue(b.checkNoBottomLatitudeBound());
    assertEquals(-Math.PI * 0.5 + 0.1, b.getMaxLatitude(), 0.000001);

    // Now do a somewhat tilted plane, facing different directions.
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, 0.0, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, Math.PI, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI + 0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.5 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.5 + 0.1, b.getRightLongitude(), 0.00001);

    // Slightly tilted, PI/4 direction.
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, -Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.01, Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, -0.01, -Math.PI * 0.25, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.09, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.11, b.getMinLatitude(), 0.000001);
    assertEquals(-Math.PI * 0.25 - 0.1, b.getLeftLongitude(), 0.00001);
    assertEquals(-Math.PI * 0.25 + 0.1, b.getRightLongitude(), 0.00001);

    // Now do a somewhat tilted plane.
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.01, -0.5, 0.1);
    b = new LatLonBounds();
    c.getBounds(b);
    assertFalse(b.checkNoLongitudeBound());
    assertFalse(b.checkNoTopLatitudeBound());
    assertFalse(b.checkNoBottomLatitudeBound());
    assertEquals(0.11, b.getMaxLatitude(), 0.000001);
    assertEquals(-0.09, b.getMinLatitude(), 0.000001);
    assertEquals(-0.6, b.getLeftLongitude(), 0.00001);
    assertEquals(-0.4, b.getRightLongitude(), 0.00001);

  }

}
