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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial.spatial4j.Geo3dShape;
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
  @Repeat(iterations = 20)
  //Seed("A9C215F48F200BB0")
  public void testOperations() throws IOException {
    setupStrategy();

    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  @Test
  public void testTriangleDisjointRect() throws IOException {
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
    //random triangle
    final List<GeoPoint> geoPoints = new ArrayList<>();
    while (geoPoints.size() < 3) {
      final Point point = randomPoint();
      final GeoPoint gPt = new GeoPoint(point.getY() * DEGREES_TO_RADIANS, point.getX() * DEGREES_TO_RADIANS);
      if (geoPoints.contains(gPt) == false) {
        geoPoints.add(gPt);
      }
    }
    final int convexPointIndex = random().nextInt(3);
    final GeoShape shape = GeoPolygonFactory.makeGeoPolygon(geoPoints, convexPointIndex);
    return new Geo3dShape(shape, ctx);
  }
}
