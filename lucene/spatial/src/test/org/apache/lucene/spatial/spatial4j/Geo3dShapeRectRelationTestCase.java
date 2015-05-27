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
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBox;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBoxFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoCircle;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPath;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPoint;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPolygonFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoShape;
import org.apache.lucene.spatial.spatial4j.geo3d.PlanetModel;
import org.junit.Rule;
import org.junit.Test;

import static com.spatial4j.core.distance.DistanceUtils.DEGREES_TO_RADIANS;

public abstract class Geo3dShapeRectRelationTestCase extends RandomizedShapeTestCase {
  protected final static double RADIANS_PER_DEGREE = Math.PI/180.0;

  @Rule
  public final LogRule testLog = LogRule.instance;

  protected static Random random() {
    return RandomizedContext.current().getRandom();
  }

  protected final PlanetModel planetModel;

  public Geo3dShapeRectRelationTestCase(PlanetModel planetModel) {
    super(SpatialContext.GEO);
    this.planetModel = planetModel;
  }

  protected GeoBBox getBoundingBox(final GeoShape path) {
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
      return GeoBBoxFactory.makeGeoBBox(planetModel, maxLat, minLat, leftLon, rightLon);
  }

  @Test
  public void testGeoCircleRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        while (true) {
          final int circleRadius = random().nextInt(179) + 1;//no 0-radius
          final Point point = nearP;
          try {
            final GeoShape shape = new GeoCircle(planetModel, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS,
                circleRadius * DEGREES_TO_RADIANS);
            return new Geo3dShape(planetModel, shape, ctx);
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
        final GeoShape shape = GeoBBoxFactory.makeGeoBBox(planetModel, ulhcPoint.getY() * DEGREES_TO_RADIANS,
            lrhcPoint.getY() * DEGREES_TO_RADIANS,
            ulhcPoint.getX() * DEGREES_TO_RADIANS,
            lrhcPoint.getX() * DEGREES_TO_RADIANS);
        return new Geo3dShape(planetModel, shape, ctx);
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
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(160) + 20;
        final int vertexCount = random().nextInt(3) + 3;
        while (true) {
          final List<GeoPoint> geoPoints = new ArrayList<>();
          while (geoPoints.size() < vertexCount) {
            final Point point = randomPoint();
            if (ctx.getDistCalc().distance(point,centerPoint) > maxDistance)
              continue;
            final GeoPoint gPt = new GeoPoint(planetModel, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS);
            geoPoints.add(gPt);
          }
          final int convexPointIndex = random().nextInt(vertexCount);       //If we get this wrong, hopefully we get IllegalArgumentException
          try {
            final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(planetModel, geoPoints, convexPointIndex);
            return new Geo3dShape(planetModel, shape, ctx);
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

      @Override
      protected int getWithinMinimum(int laps) {
        // Long/thin so only 10% of the usual figure
        return laps/10000;
      }

    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoPathRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(160) + 20;
        final int pointCount = random().nextInt(5) + 1;
        final double width = (random().nextInt(89)+1) * DEGREES_TO_RADIANS;
        while (true) {
          try {
            final GeoPath path = new GeoPath(planetModel, width);
            int i = 0;
            while (i < pointCount) {
              final Point nextPoint = randomPoint();
              if (ctx.getDistCalc().distance(nextPoint,centerPoint) > maxDistance)
                continue;
              path.addPoint(nextPoint.getY() * DEGREES_TO_RADIANS, nextPoint.getX() * DEGREES_TO_RADIANS);
              i++;
            }
            path.done();
            return new Geo3dShape(planetModel, path, ctx);
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

      @Override
      protected int getWithinMinimum(int laps) {
        // Long/thin so only 10% of the usual figure
        return laps/10000;
      }

    }.testRelateWithRectangle();
  }

  private Point geoPointToSpatial4jPoint(GeoPoint geoPoint) {
    return ctx.makePoint(geoPoint.x * DistanceUtils.RADIANS_TO_DEGREES,
        geoPoint.y * DistanceUtils.RADIANS_TO_DEGREES);
  }
}
