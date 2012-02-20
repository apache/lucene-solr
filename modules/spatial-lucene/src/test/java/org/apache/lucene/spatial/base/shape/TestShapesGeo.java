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
import org.apache.lucene.spatial.base.distance.*;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.lucene.spatial.base.shape.SpatialRelation.*;
import static org.junit.Assert.assertEquals;

/**
 * @author David Smiley - dsmiley@mitre.org
 */
public abstract class TestShapesGeo extends AbstractTestShapes {

  @Test
  public void testGeoRectangle() {
    double[] lons = new double[]{0,45,160,180,-45,-175, -180};//minX
    for (double lon : lons) {
      double[] lonWs = new double[]{0,20,180,200,355, 360};//width
      for (double lonW : lonWs) {
        testRectangle(lon, lonW, 0, 0);
        testRectangle(lon, lonW, -10, 10);
        testRectangle(lon, lonW, 80, 10);//polar cap
        testRectangle(lon, lonW, -90, 180);//full lat range
      }
    }

    //Test geo rectangle intersections
    testRectIntersect();
  }


  @Test
  public void testGeoCircle() {
    //--Start with some static tests that once failed:

    //Bug: numeric edge at pole, fails to init
    ctx.makeCircle(
        110,-12,ctx.getDistCalc().degreesToDistance(90 + 12));

    //Bug: horizXAxis not in enclosing rectangle, assertion
    ctx.makeCircle(-44,16,degToDist(106));
    ctx.makeCircle(-36,-76,degToDist(14));
    ctx.makeCircle(107,82,degToDist(172));

// TODO need to update this test to be valid
//    {
//      //Bug in which distance was being confused as being in the same coordinate system as x,y.
//      double distDeltaToPole = 0.001;//1m
//      double distDeltaToPoleDEG = ctx.getDistCalc().distanceToDegrees(distDeltaToPole);
//      double dist = 1;//1km
//      double distDEG = ctx.getDistCalc().distanceToDegrees(dist);
//      Circle c = ctx.makeCircle(0,90-distDeltaToPoleDEG-distDEG,dist);
//      Rectangle cBBox = c.getBoundingBox();
//      Rectangle r = ctx.makeRect(cBBox.getMaxX()*0.99,cBBox.getMaxX()+1,c.getCenter().getY(),c.getCenter().getY());
//      assertEquals(INTERSECTS,c.getBoundingBox().relate(r, ctx));
//      assertEquals("dist != xy space",INTERSECTS,c.relate(r,ctx));//once failed here
//    }

    assertEquals("wrong estimate", DISJOINT,ctx.makeCircle(-166,59,5226.2).relate(ctx.makeRect(36, 66, 23, 23), ctx));

    assertEquals("bad CONTAINS (dateline)",INTERSECTS,ctx.makeCircle(56,-50,12231.5).relate(ctx.makeRect(108, 26, 39, 48), ctx));

    assertEquals("bad CONTAINS (backwrap2)",INTERSECTS,
        ctx.makeCircle(112,-3,degToDist(91)).relate(ctx.makeRect(-163, 29, -38, 10), ctx));

    assertEquals("bad CONTAINS (r x-wrap)",INTERSECTS,
        ctx.makeCircle(-139,47,degToDist(80)).relate(ctx.makeRect(-180, 180, -3, 12), ctx));

    assertEquals("bad CONTAINS (pwrap)",INTERSECTS,
        ctx.makeCircle(-139,47,degToDist(80)).relate(ctx.makeRect(-180, 179, -3, 12), ctx));

    assertEquals("no-dist 1",WITHIN,
        ctx.makeCircle(135,21,0).relate(ctx.makeRect(-103, -154, -47, 52), ctx));

    assertEquals("bbox <= >= -90 bug",CONTAINS,
        ctx.makeCircle(-64,-84,degToDist(124)).relate(ctx.makeRect(-96, 96, -10, -10), ctx));

    //The horizontal axis line of a geo circle doesn't necessarily pass through c's ctr.
    assertEquals("c's horiz axis doesn't pass through ctr",INTERSECTS,
        ctx.makeCircle(71,-44,degToDist(40)).relate(ctx.makeRect(15, 27, -62, -34), ctx));

    assertEquals("pole boundary",INTERSECTS,
        ctx.makeCircle(-100,-12,degToDist(102)).relate(ctx.makeRect(143, 175, 4, 32), ctx));

    assertEquals("full circle assert",CONTAINS,
        ctx.makeCircle(-64,32,degToDist(180)).relate(ctx.makeRect(47, 47, -14, 90), ctx));

    //--Now proceed with systematic testing:

    double distToOpposeSide = ctx.getUnits().earthRadius()*Math.PI;
    assertEquals(ctx.getWorldBounds(),ctx.makeCircle(0,0,distToOpposeSide).getBoundingBox());
    //assertEquals(ctx.makeCircle(0,0,distToOpposeSide/2 - 500).getBoundingBox());

    double[] theXs = new double[]{-180,-45,90};
    for (double x : theXs) {
      double[] theYs = new double[]{-90,-45,0,45,90};
      for (double y : theYs) {
        testCircle(x, y, 0);
        testCircle(x, y, 500);
        testCircle(x, y, degToDist(90));
        testCircle(x, y, ctx.getUnits().earthRadius()*6);
      }
    }

    testCircleIntersect();
  }

  private double degToDist(int deg) {
    return ctx.getDistCalc().degreesToDistance(deg);
  }

  @Ignore
  public static class TestLawOfCosines extends TestShapesGeo {

    @Override
    protected SpatialContext getContext() {
      DistanceUnits units = DistanceUnits.KILOMETERS;
      return new SimpleSpatialContext(units,
          new GeodesicSphereDistCalc.LawOfCosines(units.earthRadius()),
          SpatialContext.GEO_WORLDBOUNDS);
    }
  }

  public static class TestHaversine extends TestShapesGeo {

    @Override
    protected SpatialContext getContext() {
      DistanceUnits units = DistanceUnits.KILOMETERS;
      return new SimpleSpatialContext(units,
          new GeodesicSphereDistCalc.Haversine(units.earthRadius()),
          SpatialContext.GEO_WORLDBOUNDS);
    }
  }

  public static class TestVincentySphere extends TestShapesGeo {

    @Override
    protected SpatialContext getContext() {
      DistanceUnits units = DistanceUnits.KILOMETERS;
      return new SimpleSpatialContext(units,
          new GeodesicSphereDistCalc.Vincenty(units.earthRadius()),
          SpatialContext.GEO_WORLDBOUNDS);
    }
  }
}
