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

package org.apache.lucene.spatial.base.distance;

import org.apache.lucene.spatial.RandomSeed;
import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContext;
import org.apache.lucene.spatial.base.shape.SpatialRelation;
import org.apache.lucene.spatial.base.shape.Point;
import org.apache.lucene.spatial.base.shape.Rectangle;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author David Smiley - dsmiley@mitre.org
 */
public class TestDistances {

  private final Random random = new Random(RandomSeed.seed());
  //NOTE!  These are sometimes modified by tests.
  private SpatialContext ctx;
  private double EPS;

  @Before
  public void beforeTest() {
    ctx = new SimpleSpatialContext(DistanceUnits.KILOMETERS);
    EPS = 10e-4;//delta when doing double assertions. Geo eps is not that small.
  }

  private DistanceCalculator dc() {
    return ctx.getDistCalc();
  }
  
  @Test
  public void testSomeDistances() {
    //See to verify: from http://www.movable-type.co.uk/scripts/latlong.html
    Point ctr = pLL(0,100);
    assertEquals(11100, dc().distance(ctr, pLL(10, 0)),3);
    assertEquals(11100, dc().distance(ctr, pLL(10, -160)),3);

    assertEquals(314.40338, dc().distance(pLL(1, 2), pLL(3, 4)),EPS);
  }

  @Test
  public void testCalcBoxByDistFromPt() {
    //first test regression
    {
      double d = 6894.1;
      Point pCtr = pLL(-20, 84);
      Point pTgt = pLL(-42, 15);
      assertTrue(dc().distance(pCtr, pTgt) < d);
      //since the pairwise distance is less than d, a bounding box from ctr with d should contain pTgt.
      Rectangle r = dc().calcBoxByDistFromPt(pCtr, d, ctx);
      assertEquals(SpatialRelation.CONTAINS,r.relate(pTgt, ctx));
      checkBBox(pCtr,d);
    }

    assertEquals("0 dist, horiz line",
        -45,dc().calcBoxByDistFromPtHorizAxis(ctx.makePoint(-180,-45),0,ctx),0);

    double MAXDIST = ctx.getUnits().earthCircumference() / 2;
    checkBBox(ctx.makePoint(0,0), MAXDIST);
    checkBBox(ctx.makePoint(0,0), MAXDIST *0.999999);
    checkBBox(ctx.makePoint(0,0),0);
    checkBBox(ctx.makePoint(0,0),0.000001);
    checkBBox(ctx.makePoint(0,90),0.000001);
    checkBBox(ctx.makePoint(-32.7,-5.42),9829);
    checkBBox(ctx.makePoint(0,90-20),ctx.getDistCalc().degreesToDistance(20));
    {
      double d = 0.010;//10m
      checkBBox(ctx.makePoint(0,90-ctx.getDistCalc().distanceToDegrees(d+0.001)),d);
    }

    for (int T = 0; T < 100; T++) {
      double lat = -90 + random.nextDouble()*180;
      double lon = -180 + random.nextDouble()*360;
      Point ctr = ctx.makePoint(lon, lat);
      double dist = MAXDIST*random.nextDouble();
      checkBBox(ctr, dist);
    }

  }

  private void checkBBox(Point ctr, double dist) {
    String msg = "ctr: "+ctr+" dist: "+dist;

    Rectangle r = dc().calcBoxByDistFromPt(ctr, dist, ctx);
    double horizAxisLat = dc().calcBoxByDistFromPtHorizAxis(ctr,dist, ctx);
    if (!Double.isNaN(horizAxisLat))
      assertTrue(r.relate_yRange(horizAxisLat, horizAxisLat, ctx).intersects());

    //horizontal
    if (r.getWidth() >= 180) {
      double calcDist = dc().distance(ctr,r.getMinX(), r.getMaxY() == 90 ? 90 : -90 );
      assertTrue(msg,calcDist <= dist+EPS);
      //horizAxisLat is meaningless in this context
    } else {
      Point tPt = findClosestPointOnVertToPoint(r.getMinX(), r.getMinY(), r.getMaxY(), ctr);
      double calcDist = dc().distance(ctr,tPt);
      assertEquals(msg,dist,calcDist,EPS);
      assertEquals(msg,tPt.getY(),horizAxisLat,EPS);
    }
    
    //vertical
    double topDist = dc().distance(ctr,ctr.getX(),r.getMaxY());
    if (r.getMaxY() == 90)
      assertTrue(msg,topDist <= dist+EPS);
    else
      assertEquals(msg,dist,topDist,EPS);
    double botDist = dc().distance(ctr,ctr.getX(),r.getMinY());
    if (r.getMinY() == -90)
      assertTrue(msg,botDist <= dist+EPS);
    else
      assertEquals(msg,dist,botDist,EPS);
  }

  private Point findClosestPointOnVertToPoint(double lon, double lowLat, double highLat, Point ctr) {
    //A binary search algorithm to find the point along the vertical lon between lowLat & highLat that is closest
    // to ctr, and returns the distance.
    double midLat = (highLat - lowLat)/2 + lowLat;
    double midLatDist = ctx.getDistCalc().distance(ctr,lon,midLat);
    for(int L = 0; L < 100 && (highLat - lowLat > 0.001|| L < 20); L++) {
      boolean bottom = (midLat - lowLat > highLat - midLat);
      double newMid = bottom ? (midLat - lowLat)/2 + lowLat : (highLat - midLat)/2 + midLat;
      double newMidDist = ctx.getDistCalc().distance(ctr,lon,newMid);
      if (newMidDist < midLatDist) {
        if (bottom) {
          highLat = midLat;
        } else {
          lowLat = midLat;
        }
        midLat = newMid;
        midLatDist = newMidDist;
      } else {
        if (bottom) {
          lowLat = newMid;
        } else {
          highLat = newMid;
        }
      }
    }
    return ctx.makePoint(lon,midLat);
  }

  @Test
  public void testDistCalcPointOnBearing_cartesian() {
    ctx = new SimpleSpatialContext(DistanceUnits.CARTESIAN);
    EPS = 10e-6;//tighter epsilon (aka delta)
    for(int i = 0; i < 1000; i++) {
      testDistCalcPointOnBearing(random.nextInt(100));
    }
  }

  @Test
  public void testDistCalcPointOnBearing_geo() {
    //The haversine formula has a higher error if the points are near antipodal. We adjust EPS tolerance for this case.
    //TODO Eventually we should add the Vincenty formula for improved accuracy, or try some other cleverness.

    //test known high delta
//    {
//      Point c = ctx.makePoint(-103,-79);
//      double angRAD = Math.toRadians(236);
//      double dist = 20025;
//      Point p2 = dc().pointOnBearingRAD(c, dist, angRAD, ctx);
//      //Pt(x=76.61200011750923,y=79.04946929870962)
//      double calcDist = dc().distance(c, p2);
//      assertEqualsRatio(dist, calcDist);
//    }
    double maxDist = ctx.getUnits().earthCircumference() / 2;
    for(int i = 0; i < 1000; i++) {
      int dist = random.nextInt((int) maxDist);
      EPS = (dist < maxDist*0.75 ? 10e-6 : 10e-3);
      testDistCalcPointOnBearing(dist);
    }
  }

  private void testDistCalcPointOnBearing(double dist) {
    for(int angDEG = 0; angDEG < 360; angDEG += random.nextInt(20)+1) {
      Point c = ctx.makePoint(random.nextInt(360),-90+random.nextInt(181));

      //0 distance means same point
      Point p2 = dc().pointOnBearing(c, 0, angDEG, ctx);
      assertEquals(c,p2);

      p2 = dc().pointOnBearing(c, dist, angDEG, ctx);
      double calcDist = dc().distance(c, p2);
      assertEqualsRatio(dist, calcDist);
    }
  }

  private void assertEqualsRatio(double expected, double actual) {
    double delta = Math.abs(actual - expected);
    double base = Math.min(actual, expected);
    double deltaRatio = base==0 ? delta : Math.min(delta,delta / base);
    assertEquals(0,deltaRatio, EPS);
  }

  @Test
  public void testNormLat() {
    double[][] lats = new double[][] {
        {1.23,1.23},//1.23 might become 1.2299999 after some math and we want to ensure that doesn't happen
        {-90,-90},{90,90},{0,0}, {-100,-80},
        {-90-180,90},{-90-360,-90},{90+180,-90},{90+360,90},
        {-12+180,12}};
    for (double[] pair : lats) {
      assertEquals("input "+pair[0],pair[1],ctx.normY(pair[0]),0);
    }
    Random random = new Random(RandomSeed.seed());
    for(int i = -1000; i < 1000; i += random.nextInt(10)*10) {
      double d = ctx.normY(i);
      assertTrue(i + " " + d, d >= -90 && d <= 90);
    }
  }

  @Test
  public void testNormLon() {
    double[][] lons = new double[][] {
        {1.23,1.23},//1.23 might become 1.2299999 after some math and we want to ensure that doesn't happen
        {-180,-180},{180,-180},{0,0}, {-190,170},
        {-180-360,-180},{-180-720,-180},{180+360,-180},{180+720,-180}};
    for (double[] pair : lons) {
      assertEquals("input "+pair[0],pair[1],ctx.normX(pair[0]),0);
    }
    Random random = new Random(RandomSeed.seed());
    for(int i = -1000; i < 1000; i += random.nextInt(10)*10) {
      double d = ctx.normX(i);
      assertTrue(i + " " + d, d >= -180 && d < 180);
    }
  }

  @Test
  public void testDistToRadians() {
    assertDistToRadians(0);
    assertDistToRadians(500);
    assertDistToRadians(ctx.getUnits().earthRadius());
  }

  private void assertDistToRadians(double dist) {
    double radius = ctx.getUnits().earthRadius();
    assertEquals(
        DistanceUtils.pointOnBearingRAD(0, 0, DistanceUtils.dist2Radians(dist, radius), DistanceUtils.DEG_90_AS_RADS, null)[1],
        DistanceUtils.dist2Radians(dist, radius),10e-5);
  }

  private Point pLL(double lat, double lon) {
    return ctx.makePoint(lon,lat);
  }

}
