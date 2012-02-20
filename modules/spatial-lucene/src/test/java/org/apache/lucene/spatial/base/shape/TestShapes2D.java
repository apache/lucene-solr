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
import org.apache.lucene.spatial.base.context.simple.SimpleSpatialContext;
import org.apache.lucene.spatial.base.distance.DistanceUnits;
import org.junit.Test;

import static org.apache.lucene.spatial.base.shape.SpatialRelation.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * @author dsmiley
 */
public class TestShapes2D extends AbstractTestShapes {

  @Override
  protected SpatialContext getContext() {
    return new SimpleSpatialContext(DistanceUnits.CARTESIAN);
  }

  @Test
  public void testSimplePoint() {
    Point pt = ctx.makePoint(0,0);
    String msg = pt.toString();

    //test equals & hashcode
    Point pt2 = ctx.makePoint(0,0);
    assertEquals(msg, pt, pt2);
    assertEquals(msg, pt.hashCode(), pt2.hashCode());

    assertFalse(msg,pt.hasArea());
    assertEquals(msg,pt.getCenter(),pt);
    Rectangle bbox = pt.getBoundingBox();
    assertFalse(msg,bbox.hasArea());
    assertEquals(msg,pt,bbox.getCenter());

    assertRelation(msg, CONTAINS, pt, pt2);
    assertRelation(msg, DISJOINT, pt, ctx.makePoint(0, 1));
    assertRelation(msg, DISJOINT, pt, ctx.makePoint(1, 0));
    assertRelation(msg, DISJOINT, pt, ctx.makePoint(1, 1));
  }

  @Test
  public void testSimpleRectangle() {
    double[] minXs = new double[]{-1000,-360,-180,-20,0,20,180,1000};
    for (double minX : minXs) {
      double[] widths = new double[]{0,10,180,360,400};
      for (double width : widths) {
        testRectangle(minX, width, 0, 0);
        testRectangle(minX, width, -10, 10);
        testRectangle(minX, width, 5, 10);
      }
    }

    testRectIntersect();
  }

  @Test
  public void testSimpleCircle() {
    double[] theXs = new double[]{-10,0,10};
    for (double x : theXs) {
      double[] theYs = new double[]{-20,0,20};
      for (double y : theYs) {
        testCircle(x, y, 0);
        testCircle(x, y, 5);
      }
    }
    //INTERSECTION:
    //Start with some static tests that have shown to cause failures at some point:
    assertEquals("getX not getY",INTERSECTS,ctx.makeCircle(107,-81,147).relate(ctx.makeRect(92, 121, -89, 74), ctx));

    testCircleIntersect();
  }


}
