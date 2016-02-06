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
package org.apache.lucene.spatial.spatial4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.spatial4j.core.TestLog;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.RectIntersectionTestHelper;
import org.apache.lucene.geo3d.LatLonBounds;
import org.apache.lucene.geo3d.GeoBBox;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.GeoStandardCircle;
import org.apache.lucene.geo3d.GeoPath;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.GeoPolygonFactory;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;
import org.junit.Rule;
import org.junit.Test;

import static com.spatial4j.core.distance.DistanceUtils.DEGREES_TO_RADIANS;

public abstract class Geo3dShapeRectRelationTestCase extends RandomizedShapeTestCase {
  protected final static double RADIANS_PER_DEGREE = Math.PI/180.0;

  @Rule
  public final TestLog testLog = TestLog.instance;

  protected final PlanetModel planetModel;

  public Geo3dShapeRectRelationTestCase(PlanetModel planetModel) {
    super(SpatialContext.GEO);
    this.planetModel = planetModel;
  }

  protected GeoBBox getBoundingBox(final GeoShape path) {
    LatLonBounds bounds = new LatLonBounds();
    path.getBounds(bounds);

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

  abstract class Geo3dRectIntersectionTestHelper extends RectIntersectionTestHelper<Geo3dShape> {

    public Geo3dRectIntersectionTestHelper(SpatialContext ctx) {
      super(ctx);
    }

    //20 times each -- should be plenty

    protected int getContainsMinimum(int laps) {
      return 20;
    }

    protected int getIntersectsMinimum(int laps) {
      return 20;
    }

    // producing "within" cases in Geo3D based on our random shapes doesn't happen often. It'd be nice to increase this.
    protected int getWithinMinimum(int laps) {
      return 2;
    }

    protected int getDisjointMinimum(int laps) {
      return 20;
    }

    protected int getBoundingMinimum(int laps) {
      return 20;
    }
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6867")
  @Test
  public void testGeoCircleRect() {
    new Geo3dRectIntersectionTestHelper(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final int circleRadius = 180 - random().nextInt(180);//no 0-radius
        final Point point = nearP;
        final GeoShape shape = new GeoStandardCircle(planetModel, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS,
            circleRadius * DEGREES_TO_RADIANS);
        return new Geo3dShape(planetModel, shape, ctx);
      }

      @Override
      protected Point randomPointInEmptyShape(Geo3dShape shape) {
        GeoPoint geoPoint = ((GeoStandardCircle)shape.shape).getCenter();
        return geoPointToSpatial4jPoint(geoPoint);
      }

    }.testRelateWithRectangle();
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6867")
  @Test
  public void testGeoBBoxRect() {
    new Geo3dRectIntersectionTestHelper(ctx) {

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

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6867")
  @Test
  public void testGeoPolygonRect() {
    new Geo3dRectIntersectionTestHelper(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(160) + 20;
        final Circle pointZone = ctx.makeCircle(centerPoint, maxDistance);
        final int vertexCount = random().nextInt(3) + 3;
        while (true) {
          final List<GeoPoint> geoPoints = new ArrayList<>();
          while (geoPoints.size() < vertexCount) {
            final Point point = randomPointIn(pointZone);
            final GeoPoint gPt = new GeoPoint(planetModel, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS);
            geoPoints.add(gPt);
          }
          final int convexPointIndex = random().nextInt(vertexCount); //If we get this wrong, hopefully we get IllegalArgumentException
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
        // Long/thin so lets just find 1.
        return 1;
      }

    }.testRelateWithRectangle();
  }

  @AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/LUCENE-6867")
  @Test
  public void testGeoPathRect() {
    new Geo3dRectIntersectionTestHelper(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(160) + 20;
        final Circle pointZone = ctx.makeCircle(centerPoint, maxDistance);
        final int pointCount = random().nextInt(5) + 1;
        final double width = (random().nextInt(89)+1) * DEGREES_TO_RADIANS;
        while (true) {
          try {
            final GeoPath path = new GeoPath(planetModel, width);
            for (int i = 0; i < pointCount; i++) {
              final Point nextPoint = randomPointIn(pointZone);
              path.addPoint(nextPoint.getY() * DEGREES_TO_RADIANS, nextPoint.getX() * DEGREES_TO_RADIANS);
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
        // Long/thin so lets just find 1.
        return 1;
      }

    }.testRelateWithRectangle();
  }

  private Point geoPointToSpatial4jPoint(GeoPoint geoPoint) {
    return ctx.makePoint(geoPoint.getLongitude() * DistanceUtils.RADIANS_TO_DEGREES,
        geoPoint.getLongitude() * DistanceUtils.RADIANS_TO_DEGREES);
  }
}
