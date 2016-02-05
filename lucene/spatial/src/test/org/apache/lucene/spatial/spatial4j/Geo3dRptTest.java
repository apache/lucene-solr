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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.geo3d.GeoBBoxFactory;
import org.apache.lucene.geo3d.GeoStandardCircle;
import org.apache.lucene.geo3d.GeoPath;
import org.apache.lucene.geo3d.GeoPoint;
import org.apache.lucene.geo3d.GeoPolygonFactory;
import org.apache.lucene.geo3d.GeoShape;
import org.apache.lucene.geo3d.PlanetModel;
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
  public void testFailure1() throws IOException {
    setupStrategy();
    final List<GeoPoint> points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 18 * DEGREES_TO_RADIANS, -27 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -57 * DEGREES_TO_RADIANS, 146 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, 14 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -15 * DEGREES_TO_RADIANS, 153 * DEGREES_TO_RADIANS));
    
    final Shape triangle = new Geo3dShape(GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, points,0),ctx);
    final Rectangle rect = ctx.makeRectangle(-49, -45, 73, 86);
    testOperation(rect,SpatialOperation.Intersects,triangle, false);
  }

  @Test
  public void testFailureLucene6535() throws IOException {
    setupStrategy();

    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(PlanetModel.SPHERE, 18 * DEGREES_TO_RADIANS, -27 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -57 * DEGREES_TO_RADIANS, 146 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, 14 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(PlanetModel.SPHERE, -15 * DEGREES_TO_RADIANS, 153 * DEGREES_TO_RADIANS));
    final GeoPath path = new GeoPath(PlanetModel.SPHERE, 29 * DEGREES_TO_RADIANS);
    path.addPoint(55.0 * DEGREES_TO_RADIANS, -26.0 * DEGREES_TO_RADIANS);
    path.addPoint(-90.0 * DEGREES_TO_RADIANS, 0.0);
    path.addPoint(54.0 * DEGREES_TO_RADIANS, 165.0 * DEGREES_TO_RADIANS);
    path.addPoint(-90.0 * DEGREES_TO_RADIANS, 0.0);
    path.done();
    final Shape shape = new Geo3dShape(path,ctx);
    final Rectangle rect = ctx.makeRectangle(131, 143, 39, 54);
    testOperation(rect,SpatialOperation.Intersects,shape,true);
  }

  @Test
  @Repeat(iterations = 10)
  public void testOperations() throws IOException {
    setupStrategy();

    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  private Shape makeTriangle(double x1, double y1, double x2, double y2, double x3, double y3) {
    final List<GeoPoint> geoPoints = new ArrayList<>();
    geoPoints.add(new GeoPoint(PlanetModel.SPHERE, y1 * DEGREES_TO_RADIANS, x1 * DEGREES_TO_RADIANS));
    geoPoints.add(new GeoPoint(PlanetModel.SPHERE, y2 * DEGREES_TO_RADIANS, x2 * DEGREES_TO_RADIANS));
    geoPoints.add(new GeoPoint(PlanetModel.SPHERE, y3 * DEGREES_TO_RADIANS, x3 * DEGREES_TO_RADIANS));
    final int convexPointIndex = 0;
    final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, geoPoints, convexPointIndex);
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
            final GeoPoint gPt = new GeoPoint(PlanetModel.SPHERE, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS);
            geoPoints.add(gPt);
          }
          final int convexPointIndex = random().nextInt(vertexCount);       //If we get this wrong, hopefully we get IllegalArgumentException
          try {
            final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(PlanetModel.SPHERE, geoPoints, convexPointIndex);
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
          final int circleRadius = random().nextInt(179) + 1;
          final Point point = randomPoint();
          try {
            final GeoShape shape = new GeoStandardCircle(PlanetModel.SPHERE, point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS,
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
            final GeoShape shape = GeoBBoxFactory.makeGeoBBox(PlanetModel.SPHERE, ulhcPoint.getY() * DEGREES_TO_RADIANS,
              lrhcPoint.getY() * DEGREES_TO_RADIANS,
              ulhcPoint.getX() * DEGREES_TO_RADIANS,
              lrhcPoint.getX() * DEGREES_TO_RADIANS);
            //System.err.println("Trial rectangle shape: "+shape);
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
            final GeoPath path = new GeoPath(PlanetModel.SPHERE, width);
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
