package org.apache.lucene.spatial.spatial4j;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.spatial.spatial4j.geo3d.Bounds;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoArea;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBox;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBoxFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoCircle;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPath;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPoint;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPolygonFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoShape;
import org.junit.Rule;
import org.junit.Test;

import static com.spatial4j.core.distance.DistanceUtils.DEGREES_TO_RADIANS;

public class Geo3dShapeRectRelationTest extends RandomizedShapeTest {
  @Rule
  public final TestLog testLog = TestLog.instance;

  static Random random() {
    return RandomizedContext.current().getRandom();
  }

  {
    ctx = SpatialContext.GEO;
  }

  protected final static double RADIANS_PER_DEGREE = Math.PI/180.0;
  
  @Test
  public void testFailure() {
      /*
   [junit4]   1> S-R Rel: {}, Shape {}, Rectangle {} [WITHIN, Geo3dShape{GeoCompositeMembershipShape: {[
    GeoConvexPolygon: {points=[
      [X=0.35168818443386646, Y=-0.19637966197066342, Z=0.9152870857244183],
      [X=0.5003343189532654, Y=0.522128543226148, Z=0.6906861469771293], 
      [X=0.8344549994139991, Y=0.216175219373972, Z=0.5069054433339593]] 
    edges={
      [A=-0.6135342247741855, B=0.21504338363863665, C=0.28188192383666794, D=0.0, side=-1.0] internal? false;
      [A=0.11536057134002048, B=0.32272431860685813, C=-0.3275328920717585, D=0.0, side=-1.0] internal? false;
      [A=0.29740830615965186, B=-0.5854932295360462, C=-0.2398962611358763, D=0.0, side=-1.0] internal? false; }}]}}, 
    Rect(minX=-30.0,maxX=62.0,minY=30.0,maxY=88.0)](no slf4j subst; sorry)
   [junit4] FAILURE 1.85s J2 | Geo3dShapeRectRelationTest.testGeoPolygonRect <<<
   [junit4]    > Throwable #1: java.lang.AssertionError: Rect(minX=-30.0,maxX=62.0,minY=30.0,maxY=88.0) intersect Pt(x=82.75500168892472,y=34.2730264413182)
   [junit4]    > 	at __randomizedtesting.SeedInfo.seed([3EBD2127AF6641F7:3A64BDAC8843B64]:0)
   [junit4]    > 	at org.apache.lucene.spatial.spatial4j.RandomizedShapeTest._assertIntersect(RandomizedShapeTest.java:167)
   [junit4]    > 	at org.apache.lucene.spatial.spatial4j.RandomizedShapeTest.assertRelation(RandomizedShapeTest.java:152)
   [junit4]    > 	at org.apache.lucene.spatial.spatial4j.RectIntersectionTestHelper.testRelateWithRectangle(RectIntersectionTestHelper.java:105)
   [junit4]    > 	at org.apache.lucene.spatial.spatial4j.Geo3dShapeRectRelationTest.testGeoPolygonRect(Geo3dShapeRectRelationTest.java:219)
    */
      final GeoBBox rect = GeoBBoxFactory.makeGeoBBox(88 * RADIANS_PER_DEGREE, 30 * RADIANS_PER_DEGREE, -30 * RADIANS_PER_DEGREE, 62 * RADIANS_PER_DEGREE);
      final List<GeoPoint> points = new ArrayList<GeoPoint>();
      points.add(new GeoPoint(66.2465299717 * RADIANS_PER_DEGREE, -29.1786158537 * RADIANS_PER_DEGREE));
      points.add(new GeoPoint(43.684447915 * RADIANS_PER_DEGREE, 46.2210986329 * RADIANS_PER_DEGREE));
      points.add(new GeoPoint(30.4579218227 * RADIANS_PER_DEGREE, 14.5238410082 * RADIANS_PER_DEGREE));
      final GeoShape path = GeoPolygonFactory.makeGeoPolygon(points,0);
    
      final GeoPoint point = new GeoPoint(34.2730264413182 * RADIANS_PER_DEGREE, 82.75500168892472 * RADIANS_PER_DEGREE);
      
      System.err.println("Rectangle = "+rect+"; path = "+path+"; point = "+point);

      /*
         [junit4]   2> Rectangle = GeoRectangle: {toplat=1.53588974175501(87.99999999999999), bottomlat=0.5235987755982988(29.999999999999996), leftlon=-0.5235987755982988(-29.999999999999996), rightlon=1.0821041362364843(62.0)};
         path = GeoCompositeMembershipShape: {[GeoConvexPolygon: {points=[
         [X=0.3516881844340107, Y=-0.1963796619709742, Z=0.9152870857242963], 
         [X=0.500334318953081, Y=0.5221285432268337, Z=0.6906861469767445], 
         [X=0.8344549994140144, Y=0.21617521937373424, Z=0.5069054433340355]] 
         edges={[A=-0.6135342247748885, B=0.21504338363844255, C=0.28188192383710364, D=0.0, side=-1.0] internal? false;
         [A=0.1153605713406553, B=0.32272431860660283, C=-0.3275328920724975, D=0.0, side=-1.0] internal? false;
         [A=0.29740830615958036, B=-0.5854932295358584, C=-0.2398962611360862, D=0.0, side=-1.0] internal? false; }}]};
         point = [X=0.10421465978661167, Y=0.8197657811637465, Z=0.5631370780889439]
        */
      // Apparently the rectangle thinks the polygon is completely within it... "shape inside rectangle"
      assertTrue(GeoArea.WITHIN == rect.getRelationship(path));

      // Point is within path? Apparently not...
      assertFalse(path.isWithin(point));

      // If it is within the path, it must be within the rectangle, and similarly visa versa
      assertFalse(rect.isWithin(point));
      
  }

  protected static GeoBBox getBoundingBox(final GeoShape path) {
      Bounds bounds = path.getBounds(null);

      double leftLon;
      double rightLon;
      if (bounds.checkNoLongitudeBound()) {
        leftLon = -Math.PI;
        rightLon = Math.PI;
      } else {
        leftLon = bounds.getLeftLongitude().doubleValue();
        rightLon = bounds.getRightLongitude().doubleValue();
      }
      double minLat;
      if (bounds.checkNoBottomLatitudeBound()) {
        minLat = -Math.PI * 0.5;
      } else {
        minLat = bounds.getMinLatitude().doubleValue();
      }
      double maxLat;
      if (bounds.checkNoTopLatitudeBound()) {
        maxLat = Math.PI * 0.5;
      } else {
        maxLat = bounds.getMaxLatitude().doubleValue();
      }
      return GeoBBoxFactory.makeGeoBBox(maxLat, minLat, leftLon, rightLon);
  }

  @Test
  //@Seed("FAD1BAB12B6DCCFE")
  public void testGeoCircleRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        while (true) {
          final int circleRadius = random().nextInt(179) + 1;//no 0-radius
          final Point point = nearP;
          try {
            final GeoShape shape = new GeoCircle(point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS,
                circleRadius * DEGREES_TO_RADIANS);
            return new Geo3dShape(shape, ctx);
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }

      @Override
      protected Point randomPointInEmptyShape(Geo3dShape shape) {
        GeoPoint geoPoint = ((GeoCircle)shape.shape).center;
        return geoPointToSpatial4jPoint(geoPoint);
      }

    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoBBoxRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected boolean isRandomShapeRectangular() {
        return true;
      }

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        // (ignoring nearP)
        Point ulhcPoint = randomPoint();
        Point lrhcPoint = randomPoint();
        if (ulhcPoint.getY() < lrhcPoint.getY()) {
          //swap
          Point temp = ulhcPoint;
          ulhcPoint = lrhcPoint;
          lrhcPoint = temp;
        }
        final GeoShape shape = GeoBBoxFactory.makeGeoBBox(ulhcPoint.getY() * DEGREES_TO_RADIANS,
            lrhcPoint.getY() * DEGREES_TO_RADIANS,
            ulhcPoint.getX() * DEGREES_TO_RADIANS,
            lrhcPoint.getX() * DEGREES_TO_RADIANS);
        return new Geo3dShape(shape, ctx);
      }

      @Override
      protected Point randomPointInEmptyShape(Geo3dShape shape) {
        return shape.getBoundingBox().getCenter();
      }
    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoPolygonRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final int vertexCount = random().nextInt(3) + 3;
        while (true) {
          final List<GeoPoint> geoPoints = new ArrayList<>();
          while (geoPoints.size() < vertexCount) {
            final Point point = randomPoint();
            final GeoPoint gPt = new GeoPoint(point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS);
            geoPoints.add(gPt);
          }
          final int convexPointIndex = random().nextInt(vertexCount);       //If we get this wrong, hopefully we get IllegalArgumentException
          try {
            final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(geoPoints, convexPointIndex);
            return new Geo3dShape(shape, ctx);
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }

      @Override
      protected Point randomPointInEmptyShape(Geo3dShape shape) {
        throw new IllegalStateException("unexpected; need to finish test code");
      }

    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoPathRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final int pointCount = random().nextInt(5) + 1;
        final double width = (random().nextInt(89)+1) * DEGREES_TO_RADIANS;
        while (true) {
          try {
            final GeoPath path = new GeoPath(width);
            for (int i = 0; i < pointCount; i++) {
              final Point nextPoint = randomPoint();
              path.addPoint(nextPoint.getY() * DEGREES_TO_RADIANS, nextPoint.getX() * DEGREES_TO_RADIANS);
            }
            path.done();
            return new Geo3dShape(path, ctx);
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }

      @Override
      protected Point randomPointInEmptyShape(Geo3dShape shape) {
        throw new IllegalStateException("unexpected; need to finish test code");
      }

    }.testRelateWithRectangle();
  }

  private Point geoPointToSpatial4jPoint(GeoPoint geoPoint) {
    return ctx.makePoint(geoPoint.x * DistanceUtils.RADIANS_TO_DEGREES,
        geoPoint.y * DistanceUtils.RADIANS_TO_DEGREES);
  }

}
