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

package org.apache.lucene.spatial.base.shape;

import org.apache.lucene.spatial.base.context.SpatialContext;
import org.apache.lucene.spatial.base.distance.DistanceCalculator;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;

import java.util.Random;

import static org.apache.lucene.spatial.base.shape.SpatialRelation.*;


public abstract class AbstractTestShapes extends LuceneTestCase {

  protected SpatialContext ctx;
  private static final double EPS = 10e-9;

  @Before
  public void beforeClass() {
    ctx = getContext();
  }

  protected void assertRelation(String msg, SpatialRelation expected, Shape a, Shape b) {
    msg = a+" intersect "+b;//use different msg
    _assertIntersect(msg,expected,a,b);
    //check flipped a & b w/ transpose(), while we're at it
    _assertIntersect("(transposed) " + msg, expected.transpose(), b, a);
  }

  private void _assertIntersect(String msg, SpatialRelation expected, Shape a, Shape b) {
    SpatialRelation sect = a.relate(b, ctx);
    if (sect == expected)
      return;
    if (expected == WITHIN || expected == CONTAINS) {
      if (a.getClass().equals(b.getClass())) // they are the same shape type
        assertEquals(msg,a,b);
      else {
        //they are effectively points or lines that are the same location
        assertTrue(msg,!a.hasArea());
        assertTrue(msg,!b.hasArea());

        Rectangle aBBox = a.getBoundingBox();
        Rectangle bBBox = b.getBoundingBox();
        if (aBBox.getHeight() == 0 && bBBox.getHeight() == 0
            && (aBBox.getMaxY() == 90 && bBBox.getMaxY() == 90
          || aBBox.getMinY() == -90 && bBBox.getMinY() == -90))
          ;//== a point at the pole
        else
          assertEquals(msg, aBBox, bBBox);
      }
    } else {
      assertEquals(msg,expected,sect);
    }
  }

  private void assertEqualsRatio(String msg, double expected, double actual) {
    double delta = Math.abs(actual - expected);
    double base = Math.min(actual, expected);
    double deltaRatio = base==0 ? delta : Math.min(delta,delta / base);
    assertEquals(msg,0,deltaRatio, EPS);
  }

  protected void testRectangle(double minX, double width, double minY, double height) {
    Rectangle r = ctx.makeRect(minX, minX + width, minY, minY+height);
    //test equals & hashcode of duplicate
    Rectangle r2 = ctx.makeRect(minX, minX + width, minY, minY+height);
    assertEquals(r,r2);
    assertEquals(r.hashCode(),r2.hashCode());

    String msg = r.toString();

    assertEquals(msg, width != 0 && height != 0, r.hasArea());
    assertEquals(msg, width != 0 && height != 0, r.getArea() > 0);

    assertEqualsRatio(msg, height, r.getHeight());
    assertEqualsRatio(msg, width, r.getWidth());
    Point center = r.getCenter();
    msg += " ctr:"+center;
    //System.out.println(msg);
    assertRelation(msg, CONTAINS, r, center);

    DistanceCalculator dc = ctx.getDistCalc();
    double dUR = dc.distance(center, r.getMaxX(), r.getMaxY());
    double dLR = dc.distance(center, r.getMaxX(), r.getMinY());
    double dUL = dc.distance(center, r.getMinX(), r.getMaxY());
    double dLL = dc.distance(center, r.getMinX(), r.getMinY());

    assertEquals(msg,width != 0 || height != 0, dUR != 0);
    if (dUR != 0)
      assertTrue(dUR > 0 && dLL > 0);
    assertEqualsRatio(msg, dUR, dUL);
    assertEqualsRatio(msg, dLR, dLL);
    if (!ctx.isGeo() || center.getY() == 0)
      assertEqualsRatio(msg, dUR, dLL);
  }

  protected void testRectIntersect() {
    final double INCR = 45;
    final double Y = 10;
    for(double left = -180; left <= 180; left += INCR) {
      for(double right = left; right - left <= 360; right += INCR) {
        Rectangle r = ctx.makeRect(left,right,-Y,Y);

        //test contains (which also tests within)
        for(double left2 = left; left2 <= right; left2 += INCR) {
          for(double right2 = left2; right2 <= right; right2 += INCR) {
            Rectangle r2 = ctx.makeRect(left2,right2,-Y,Y);
            assertRelation(null, SpatialRelation.CONTAINS, r, r2);
          }
        }
        //test point contains
        assertRelation(null, SpatialRelation.CONTAINS, r, ctx.makePoint(left, Y));

        //test disjoint
        for(double left2 = right+INCR; left2 - left < 360; left2 += INCR) {
          for(double right2 = left2; right2 - left < 360; right2 += INCR) {
            Rectangle r2 = ctx.makeRect(left2,right2,-Y,Y);
            assertRelation(null, SpatialRelation.DISJOINT, r, r2);

            //test point disjoint
            assertRelation(null, SpatialRelation.DISJOINT, r, ctx.makePoint(left2, Y));
          }
        }
        //test intersect
        for(double left2 = left+INCR; left2 <= right; left2 += INCR) {
          for(double right2 = right+INCR; right2 - left < 360; right2 += INCR) {
            Rectangle r2 = ctx.makeRect(left2,right2,-Y,Y);
            assertRelation(null, SpatialRelation.INTERSECTS, r, r2);
          }
        }

      }
    }
  }

  protected void testCircle(double x, double y, double dist) {
    Circle c = ctx.makeCircle(x, y, dist);
    String msg = c.toString();
    final Circle c2 = ctx.makeCircle(ctx.makePoint(x, y), dist);
    assertEquals(c, c2);
    assertEquals(c.hashCode(),c2.hashCode());

    assertEquals(msg,dist > 0, c.hasArea());
    final Rectangle bbox = c.getBoundingBox();
    assertEquals(msg,dist > 0, bbox.getArea() > 0);
    if (!ctx.isGeo()) {
      //if not geo then units of dist == units of x,y
      assertEqualsRatio(msg, bbox.getHeight(), dist * 2);
      assertEqualsRatio(msg, bbox.getWidth(), dist * 2);
    }
    assertRelation(msg, CONTAINS, c, c.getCenter());
    assertRelation(msg, CONTAINS, bbox, c);
  }

  protected void testCircleIntersect() {
    //Now do some randomized tests:
    int i_C = 0, i_I = 0, i_W = 0, i_O = 0;//counters for the different intersection cases
    int laps = 0;
    int MINLAPSPERCASE = 20;
    while(i_C < MINLAPSPERCASE || i_I < MINLAPSPERCASE || i_W < MINLAPSPERCASE || i_O < MINLAPSPERCASE) {
      laps++;
      double cX = randRange(-180,179);
      double cY = randRange(-90,90);
      double cR = randRange(0, 180);
      double cR_dist = ctx.getDistCalc().distance(ctx.makePoint(0, 0), 0, cR);
      Circle c = ctx.makeCircle(cX, cY, cR_dist);

      double rX = randRange(-180,179);
      double rW = randRange(0,360);
      double rY1 = randRange(-90,90);
      double rY2 = randRange(-90,90);
      double rYmin = Math.min(rY1,rY2);
      double rYmax = Math.max(rY1,rY2);
      Rectangle r = ctx.makeRect(rX, rX+rW, rYmin, rYmax);

      SpatialRelation ic = c.relate(r, ctx);

      Point p;
      switch (ic) {
        case CONTAINS:
          i_C++;
          p = randomPointWithin(random,r,ctx);
          assertEquals(CONTAINS,c.relate(p, ctx));
          break;
        case INTERSECTS:
          i_I++;
          //hard to test anything here; instead we'll test it separately
          break;
        case WITHIN:
          i_W++;
          p = randomPointWithin(random,c,ctx);
          assertEquals(CONTAINS,r.relate(p, ctx));
          break;
        case DISJOINT:
          i_O++;
          p = randomPointWithin(random,r,ctx);
          assertEquals(DISJOINT,c.relate(p, ctx));
          break;
        default: fail(""+ic);
      }
    }
    //System.out.println("Laps: "+laps);

    //TODO deliberately test INTERSECTS based on known intersection point
  }

  /** Returns a random integer between [start, end] with a limited number of possibilities instead of end-start+1. */
  private int randRange(int start, int end) {
    //I tested this.
    double r = random.nextDouble();
    final int BUCKETS = 91;
    int ir = (int) Math.round(r*(BUCKETS-1));//put into buckets
    int result = (int)((double)((end - start) * ir) / (double)(BUCKETS-1) + (double)start);
    assert result >= start && result <= end;
    return result;
  }

  private Point randomPointWithin(Random random, Circle c, SpatialContext ctx) {
    double d = c.getDistance() * random.nextDouble();
    double angleDEG = 360*random.nextDouble();
    Point p = ctx.getDistCalc().pointOnBearing(c.getCenter(), d, angleDEG, ctx);
    assertEquals(CONTAINS,c.relate(p, ctx));
    return p;
  }

  private Point randomPointWithin(Random random, Rectangle r, SpatialContext ctx) {
    double x = r.getMinX() + random.nextDouble()*r.getWidth();
    double y = r.getMinY() + random.nextDouble()*r.getHeight();
    Point p = ctx.makePoint(x,y);
    assertEquals(CONTAINS,r.relate(p, ctx));
    return p;
  }

  protected abstract SpatialContext getContext();
}
