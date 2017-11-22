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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

public class GeoCircleTest extends LuceneTestCase {

  @Test
  public void testExactCircleLUCENE8054() {
    // [junit4]    > Throwable #1: java.lang.AssertionError: circle1: GeoExactCircle: 
    // {planetmodel=PlanetModel.WGS84, center=[lat=-1.2097332228999564, lon=0.749061883738567([X=0.25823775418663625, Y=0.2401212674846636, Z=-0.9338185278804293])],
    //  radius=0.20785254459485322(11.909073566339822), accuracy=6.710701666727661E-9}
    // [junit4]    > circle2: GeoExactCircle: {planetmodel=PlanetModel.WGS84, center=[lat=-1.2097332228999564, lon=0.749061883738567([X=0.25823775418663625, Y=0.2401212674846636, Z=-0.9338185278804293])], 
    // radius=0.20701584142315682(11.861134005896407), accuracy=1.0E-5}
    final GeoCircle c1 = new GeoExactCircle(PlanetModel.WGS84, -1.2097332228999564, 0.749061883738567, 0.20785254459485322, 6.710701666727661E-9);
    final GeoCircle c2 = new GeoExactCircle(PlanetModel.WGS84, -1.2097332228999564, 0.749061883738567, 0.20701584142315682, 1.0E-5);
    assertTrue("cannot be disjoint", c1.getRelationship(c2) != GeoArea.DISJOINT);
  }
  
  @Test
  public void testExactCircle() {
    GeoCircle c;
    GeoPoint gp;
    
    // Construct a variety of circles to see how many actual planes are involved
    c = new GeoExactCircle(PlanetModel.WGS84, 0.0, 0.0, 0.1, 1e-6);
    gp = new GeoPoint(PlanetModel.WGS84, 0.0, 0.2);
    assertTrue(!c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.WGS84, 0.0, 0.0);
    assertTrue(c.isWithin(gp));
    
    c = new GeoExactCircle(PlanetModel.WGS84, 0.1, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.2, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.3, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, 0.4, 0.0, 0.1, 1e-6);

    c = new GeoExactCircle(PlanetModel.WGS84, Math.PI * 0.5, 0.0, 0.1, 1e-6);
    gp = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5 - 0.2, 0.0);
    assertTrue(!c.isWithin(gp));
    gp = new GeoPoint(PlanetModel.WGS84, Math.PI * 0.5, 0.0);
    assertTrue(c.isWithin(gp));

  }
  
  @Test
  public void testCircleDistance() {
    GeoCircle c;
    GeoPoint gp;
    c = GeoCircleFactory.makeGeoCircle(PlanetModel.SPHERE, 0.0, -0.5, 0.1);
    gp = new GeoPoint(PlanetModel.SPHERE, 0.0, 0.0);
    assertEquals(Double.POSITIVE_INFINITY, c.computeDistance(DistanceStyle.ARC,gp), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, c.computeDistance(DistanceStyle.NORMAL,gp), 0.0);
    assertEquals(Double.POSITIVE_INFINITY, c.computeDistance(DistanceStyle.NORMAL,gp), 0.0);
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

  @Test
  public void testBoundsFailureCase1() {
    // lat=2.7399499693409367E-13, lon=-3.141592653589793([X=-1.0011188539924791, Y=-1.226017000107956E-16, Z=2.743015573303327E-13])], radius=2.1814042682464985
    final GeoCircle gc = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, 2.7399499693409367E-13, -3.141592653589793, 2.1814042682464985);
    // With a circle like this, zmin should equal zmax, and xmin should be PlanetModel.minimumX.
    final GeoPoint gp = new GeoPoint(0.0054866241253590815, -0.004009749293376541, 0.997739304376186);
    final GeoPoint gpOnSurface = PlanetModel.WGS84.createSurfacePoint(gp);
    final XYZBounds bounds = new XYZBounds();
    gc.getBounds(bounds);
    //System.out.println("Bounds: "+bounds);
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(), bounds.getMinimumY(), bounds.getMaximumY(), bounds.getMinimumZ(), bounds.getMaximumZ());
    assertTrue(gc.isWithin(gpOnSurface));
    assertTrue(gc.isWithin(gp));
    assertTrue(solid.isWithin(gpOnSurface)); // This fails
    assertTrue(solid.isWithin(gp));
  }
  
  @Test
  public void testBoundsFailureCase2() {
    final GeoCircle gc = GeoCircleFactory.makeGeoCircle(PlanetModel.WGS84, -2.7574435614238194E-13, 0.0, 1.5887859182593391);
    final GeoPoint gp = new GeoPoint(PlanetModel.WGS84, 0.7980359504429014, 1.5964981068121482);
    final XYZBounds bounds = new XYZBounds();
    gc.getBounds(bounds);
    System.out.println("Bounds = "+bounds);
    System.out.println("Point = "+gp);
    final XYZSolid solid = XYZSolidFactory.makeXYZSolid(PlanetModel.WGS84, bounds.getMinimumX(), bounds.getMaximumX(), bounds.getMinimumY(), bounds.getMaximumY(), bounds.getMinimumZ(), bounds.getMaximumZ());
    
    assert gc.isWithin(gp)?solid.isWithin(gp):true;
    
  }

  @Test
  @Repeat(iterations = 100)
  public void RandomPointBearingWGS84Test(){
    PlanetModel planetModel = PlanetModel.WGS84;
    RandomGeo3dShapeGenerator generator = new RandomGeo3dShapeGenerator();
    GeoPoint center = generator.randomGeoPoint(planetModel);
    double radius = random().nextDouble() * Math.PI;
    checkBearingPoint(planetModel, center, radius, 0);
    checkBearingPoint(planetModel, center, radius, 0.5 * Math.PI);
    checkBearingPoint(planetModel, center, radius, Math.PI);
    checkBearingPoint(planetModel, center, radius, 1.5 * Math.PI);
  }

  @Test
  @Repeat(iterations = 10000)
  public void RandomPointBearingCardinalTest(){
    double ab = random().nextDouble() * 0.6 + 0.9;
    double c = random().nextDouble() * 0.6  + 0.9 ;
    PlanetModel planetModel = new PlanetModel(ab, c);
    RandomGeo3dShapeGenerator generator = new RandomGeo3dShapeGenerator();
    GeoPoint center = generator.randomGeoPoint(planetModel);
    double radius =  random().nextDouble() * 0.9 * Math.PI;
    checkBearingPoint(planetModel, center, radius, 0);
    checkBearingPoint(planetModel, center, radius, 0.5 * Math.PI);
    checkBearingPoint(planetModel, center, radius, Math.PI);
    checkBearingPoint(planetModel, center, radius, 1.5 * Math.PI);
  }

  private void checkBearingPoint(PlanetModel planetModel, GeoPoint center, double radius, double bearingAngle) {
    GeoPoint point = planetModel.surfacePointOnBearing(center, radius, bearingAngle);
    double surfaceDistance = planetModel.surfaceDistance(center, point);
    assertTrue(planetModel.toString() + " " + Double.toString(surfaceDistance - radius) + " " + Double.toString(radius), surfaceDistance - radius < Vector.MINIMUM_ANGULAR_RESOLUTION);
  }

  @Test
  public void exactCircleLargeTest(){
    boolean success = true;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(new PlanetModel(0.5, 0.7), 0.25 * Math.PI,  0,0.35 * Math.PI, 1e-12);
    } catch (IllegalArgumentException e) {
      success = false;
    }
    assertTrue(success);
    success = false;
    try {
      GeoCircle circle = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, 0.25 * Math.PI,  0,0.9996 * Math.PI, 1e-12);
    } catch (IllegalArgumentException e) {
      success = true;
    }
    assertTrue(success);
  }

  @Test
  public void testLUCENE8054(){
    GeoCircle circle1 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, -1.0394053553992673, -1.9037325881389144, 1.1546166170607672, 4.231100485201301E-4);
    GeoCircle circle2 = GeoCircleFactory.makeExactGeoCircle(PlanetModel.WGS84, -1.3165961602008989, -1.887137823746273, 1.432516663588956, 3.172052880854355E-4);
    // Relationship between circles must be different than DISJOINT as centers are closer than the radius.
    int rel = circle1.getRelationship(circle2);
    assertTrue(rel != GeoArea.DISJOINT);
  }
  
}
