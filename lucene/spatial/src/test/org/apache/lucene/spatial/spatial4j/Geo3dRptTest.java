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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBox;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoBBoxFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoCircle;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPath;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPoint;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoPolygonFactory;
import org.apache.lucene.spatial.spatial4j.geo3d.GeoShape;
import org.junit.Test;

import static com.spatial4j.core.distance.DistanceUtils.DEGREES_TO_RADIANS;

public class Geo3dRptTest extends RandomSpatialOpStrategyTestCase {

  private SpatialPrefixTree grid;
  private RecursivePrefixTreeStrategy rptStrategy;
  {
    this.ctx = SpatialContext.GEO;
  }

  private void setupGeohashGrid() {
    this.grid = new GeohashPrefixTree(ctx, 2);//A fairly shallow grid
    this.rptStrategy = newRPT();
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    final RecursivePrefixTreeStrategy rpt = new RecursivePrefixTreeStrategy(this.grid,
        getClass().getSimpleName() + "_rpt");
    rpt.setDistErrPct(0.10);//not too many cells
    return rpt;
  }

  @Override
  protected boolean needsDocValues() {
    return true;//due to SerializedDVStrategy
  }

  private void setupStrategy() {
    //setup
    setupGeohashGrid();

    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.strategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        rptStrategy, serializedDVStrategy);
  }

  @Test
  //@Repeat(iterations = 2000)
  @Seed("B808B88D6F8E285C")
  public void testOperations() throws IOException {
    setupStrategy();

    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  @Test
  public void testBigCircleFailure() throws IOException {
    Rectangle rect = ctx.makeRectangle(-162, 89, -46, 38);
    GeoCircle rawShape = new GeoCircle(-9 * DEGREES_TO_RADIANS, 134 * DEGREES_TO_RADIANS, 159 * DEGREES_TO_RADIANS);
    Shape shape = new Geo3dShape(rawShape, ctx);
    assertTrue(rect.relate(shape).intersects() == false);     //DWS: unsure if this is correct or not but passes
    //since they don't intersect, then the following cell rect can't be WITHIN the circle
    final Rectangle cellRect = ctx.makeRectangle(-11.25, 0, 0, 5.625);
    assert cellRect.relate(rect).intersects();
    assertTrue(cellRect.relate(shape) != SpatialRelation.WITHIN);
  }

  @Test
  public void testWideRectFailure() throws IOException {
    Rectangle rect = ctx.makeRectangle(-29, 9, 16, 25);
    final GeoBBox geoBBox = GeoBBoxFactory.makeGeoBBox(
        74 * DEGREES_TO_RADIANS, -31 * DEGREES_TO_RADIANS, -29 * DEGREES_TO_RADIANS, -45 * DEGREES_TO_RADIANS);
    Shape shape = new Geo3dShape(geoBBox, ctx);
    //Rect(minX=-22.5,maxX=-11.25,minY=11.25,maxY=16.875)
    //since they don't intersect, then the following cell rect can't be WITHIN the geo3d shape
    final Rectangle cellRect = ctx.makeRectangle(-22.5, -11.25, 11.25, 16.875);
    assert cellRect.relate(rect).intersects();
    assertTrue(rect.relate(shape).intersects() == false);
    assertTrue(cellRect.relate(shape) != SpatialRelation.WITHIN);
//    setupStrategy();
//    testOperation(rect, SpatialOperation.Intersects, shape, false);
  }

  private Shape makeTriangle(double x1, double y1, double x2, double y2, double x3, double y3) {
    final List<GeoPoint> geoPoints = new ArrayList<>();
    geoPoints.add(new GeoPoint(y1 * DEGREES_TO_RADIANS, x1 * DEGREES_TO_RADIANS));
    geoPoints.add(new GeoPoint(y2 * DEGREES_TO_RADIANS, x2 * DEGREES_TO_RADIANS));
    geoPoints.add(new GeoPoint(y3 * DEGREES_TO_RADIANS, x3 * DEGREES_TO_RADIANS));
    final int convexPointIndex = 0;
    final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(geoPoints, convexPointIndex);
    return new Geo3dShape(shape, ctx);
  }

  @Override
  protected Shape randomIndexedShape() {
    return randomRectangle();
  }

  @Override
  protected Shape randomQueryShape() {
    final int shapeType = random().nextInt(4);
    switch (shapeType) {
    case 0: {
        // Polygons
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
    case 1: {
        // Circles
        while (true) {
          final int circleRadius = random().nextInt(180);
          final Point point = randomPoint();
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
    case 2: {
        // Rectangles
        while (true) {
          Point ulhcPoint = randomPoint();
          Point lrhcPoint = randomPoint();
          if (ulhcPoint.getY() < lrhcPoint.getY()) {
            //swap
            Point temp = ulhcPoint;
            ulhcPoint = lrhcPoint;
            lrhcPoint = temp;
          }
          try {
            final GeoShape shape = GeoBBoxFactory.makeGeoBBox(ulhcPoint.getY() * DEGREES_TO_RADIANS,
              lrhcPoint.getY() * DEGREES_TO_RADIANS,
              ulhcPoint.getX() * DEGREES_TO_RADIANS,
              lrhcPoint.getX() * DEGREES_TO_RADIANS);
            return new Geo3dShape(shape, ctx);
          } catch (IllegalArgumentException e) {
            // This is what happens when we create a shape that is invalid.  Although it is conceivable that there are cases where
            // the exception is thrown incorrectly, we aren't going to be able to do that in this random test.
            continue;
          }
        }
      }
    case 3: {
        // Paths
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
    default:
      throw new IllegalStateException("Unexpected shape type");
    }
  }
}
