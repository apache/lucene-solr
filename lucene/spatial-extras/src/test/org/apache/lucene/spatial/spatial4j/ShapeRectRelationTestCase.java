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

import org.junit.Rule;
import org.junit.Test;
import org.locationtech.spatial4j.TestLog;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.RectIntersectionTestHelper;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory;

import static org.locationtech.spatial4j.distance.DistanceUtils.DEGREES_TO_RADIANS;

public abstract class ShapeRectRelationTestCase extends RandomizedShapeTestCase {
  protected final static double RADIANS_PER_DEGREE = Math.PI/180.0;

  @Rule
  public final TestLog testLog = TestLog.instance;

  protected int maxRadius = 180;

  public ShapeRectRelationTestCase() {
    super(SpatialContext.GEO);
  }

  public abstract class AbstractRectIntersectionTestHelper extends RectIntersectionTestHelper<Shape> {

    public AbstractRectIntersectionTestHelper(SpatialContext ctx) {
      super(ctx);
    }

    //2 times each -- should be plenty

    protected int getContainsMinimum(int laps) {
      return 2;
    }

    protected int getIntersectsMinimum(int laps) {
      return 2;
    }

    // producing "within" cases in Geo3D based on our random shapes doesn't happen often. It'd be nice to increase this.
    protected int getWithinMinimum(int laps) {
      return 2;
    }

    protected int getDisjointMinimum(int laps) {
      return 2;
    }

    protected int getBoundingMinimum(int laps) {
      return 2;
    }
  }

  @Test
  public void testGeoCircleRect() {
    new AbstractRectIntersectionTestHelper(ctx) {

      @Override
      protected Shape generateRandomShape(Point nearP) {
        final int circleRadius = maxRadius - random().nextInt(maxRadius);//no 0-radius
        return ctx.getShapeFactory().circle(nearP, circleRadius);
      }

      @Override
      protected Point randomPointInEmptyShape(Shape shape) {
        return shape.getCenter();
      }

    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoBBoxRect() {
    new AbstractRectIntersectionTestHelper(ctx) {

      @Override
      protected boolean isRandomShapeRectangular() {
        return true;
      }

      @Override
      protected Shape generateRandomShape(Point nearP) {
        Point upperRight = randomPoint();
        Point lowerLeft = randomPoint();
        if (upperRight.getY() < lowerLeft.getY()) {
          //swap
          Point temp = upperRight;
          upperRight = lowerLeft;
          lowerLeft = temp;
        }
        return ctx.getShapeFactory().rect(lowerLeft, upperRight);
      }

      @Override
      protected Point randomPointInEmptyShape(Shape shape) {
        return shape.getCenter();
      }
    }.testRelateWithRectangle();
  }

  // very slow, and test sources are not here, so no clue how to fix
  @Test
  public void testGeoPolygonRect() {
    new AbstractRectIntersectionTestHelper(ctx) {

      @Override
      protected Shape generateRandomShape(Point nearP) {
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(maxRadius -20) + 20;
        final Circle pointZone = ctx.getShapeFactory().circle(centerPoint, maxDistance);
        final int vertexCount = random().nextInt(3) + 3;
        while (true) {
          ShapeFactory.PolygonBuilder builder = ctx.getShapeFactory().polygon();
          for (int i = 0; i < vertexCount; i++) {
            final Point point = randomPointIn(pointZone);
            builder.pointXY(point.getX(), point.getY());
          }
          try {
            return builder.build();
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }

      @Override
      protected Point randomPointInEmptyShape(Shape shape) {
        throw new IllegalStateException("unexpected; need to finish test code");
      }

      @Override
      protected int getWithinMinimum(int laps) {
        // Long/thin so lets just find 1.
        return 1;
      }

    }.testRelateWithRectangle();
  }

  @Test
  public void testGeoPathRect() {
    new AbstractRectIntersectionTestHelper(ctx) {

      @Override
      protected Shape generateRandomShape(Point nearP) {
        final Point centerPoint = randomPoint();
        final int maxDistance = random().nextInt(maxRadius -20) + 20;
        final Circle pointZone = ctx.getShapeFactory().circle(centerPoint, maxDistance);
        final int pointCount = random().nextInt(5) + 1;
        final double width = (random().nextInt(89)+1) * DEGREES_TO_RADIANS;
        final ShapeFactory.LineStringBuilder builder = ctx.getShapeFactory().lineString();
        while (true) {
          for (int i = 0; i < pointCount; i++) {
            final Point nextPoint = randomPointIn(pointZone);
            builder.pointXY(nextPoint.getX(), nextPoint.getY());
          }
          builder.buffer(width);
          try {
            return builder.build();
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }

      @Override
      protected Point randomPointInEmptyShape(Shape shape) {
        throw new IllegalStateException("unexpected; need to finish test code");
      }

      @Override
      protected int getWithinMinimum(int laps) {
        // Long/thin so lets just find 1.
        return 1;
      }

    }.testRelateWithRectangle();
  }
}
