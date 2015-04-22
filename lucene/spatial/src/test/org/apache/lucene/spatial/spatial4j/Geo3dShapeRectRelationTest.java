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
