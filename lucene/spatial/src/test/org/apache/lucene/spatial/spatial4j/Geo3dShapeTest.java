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

import java.util.Random;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoCircle;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPoint;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoShape;
import org.junit.Rule;
import org.junit.Test;

import static com.spatial4j.core.distance.DistanceUtils.DEGREES_TO_RADIANS;

public class Geo3dShapeTest extends RandomizedShapeTest {
  @Rule
  public final TestLog testLog = TestLog.instance;

  static Random random() {
    return RandomizedContext.current().getRandom();
  }

  {
    ctx = SpatialContext.GEO;
  }

  @Test
  @Seed("FAD1BAB12B6DCCFE")
  public void testGeoCircleRect() {
    new RectIntersectionTestHelper<Geo3dShape>(ctx) {

      @Override
      protected Geo3dShape generateRandomShape(Point nearP) {
        // Circles
        while (true) {
          final int circleRadius = random().nextInt(180);
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

  //TODO PORT OTHER TESTS


  private Point geoPointToSpatial4jPoint(GeoPoint geoPoint) {
    return ctx.makePoint(geoPoint.x * DistanceUtils.RADIANS_TO_DEGREES,
        geoPoint.y * DistanceUtils.RADIANS_TO_DEGREES);
  }

}
