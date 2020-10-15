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
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.spatial.SpatialTestData;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.S2PrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPath;
import org.apache.lucene.spatial3d.geom.GeoPathFactory;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.apache.lucene.spatial3d.geom.GeoPolygonFactory;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.RandomGeo3dShapeGenerator;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import static org.locationtech.spatial4j.distance.DistanceUtils.DEGREES_TO_RADIANS;

public class Geo3dRptTest extends RandomSpatialOpStrategyTestCase {

  private PlanetModel planetModel;
  private RandomGeo3dShapeGenerator shapeGenerator;
  private SpatialPrefixTree grid;
  private RecursivePrefixTreeStrategy rptStrategy;

  private void setupGrid() {
    int type = random().nextInt(4);
    if (type == 0) {
      this.grid = new GeohashPrefixTree(ctx, 2);
    } else if (type == 1) {
      this.grid = new QuadPrefixTree(ctx, 5);
    } else {
      int arity = random().nextInt(3) + 1;
      this.grid = new S2PrefixTree(ctx, 5 - arity, arity);
    }
    this.rptStrategy = newRPT();
    this.rptStrategy.setPruneLeafyBranches(random().nextBoolean());
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    final RecursivePrefixTreeStrategy rpt = new RecursivePrefixTreeStrategy(this.grid,
        getClass().getSimpleName() + "_rpt");
    rpt.setDistErrPct(0.10);//not too many cells
    return rpt;
  }

  private void setupStrategy() {
    shapeGenerator = new RandomGeo3dShapeGenerator();
    planetModel = shapeGenerator.randomPlanetModel();
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    factory.planetModel = planetModel;
    ctx = factory.newSpatialContext();

    setupGrid();

    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.strategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        rptStrategy, serializedDVStrategy);
  }

  @Test
  public void testFailure1() throws IOException {
    setupStrategy();
    final List<GeoPoint> points = new ArrayList<GeoPoint>();
    points.add(new GeoPoint(planetModel, 18 * DEGREES_TO_RADIANS, -27 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, -57 * DEGREES_TO_RADIANS, 146 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, 14 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, -15 * DEGREES_TO_RADIANS, 153 * DEGREES_TO_RADIANS));

    final Shape triangle = new Geo3dShape<>(GeoPolygonFactory.makeGeoPolygon(planetModel, points),ctx);
    final Rectangle rect = ctx.makeRectangle(-49, -45, 73, 86);
    testOperation(rect,SpatialOperation.Intersects,triangle, false);
  }

  @Test
  public void testFailureLucene6535() throws IOException {
    setupStrategy();

    final List<GeoPoint> points = new ArrayList<>();
    points.add(new GeoPoint(planetModel, 18 * DEGREES_TO_RADIANS, -27 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, -57 * DEGREES_TO_RADIANS, 146 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, 14 * DEGREES_TO_RADIANS, -180 * DEGREES_TO_RADIANS));
    points.add(new GeoPoint(planetModel, -15 * DEGREES_TO_RADIANS, 153 * DEGREES_TO_RADIANS));
    final GeoPoint[] pathPoints = new GeoPoint[] {
        new GeoPoint(planetModel, 55.0 * DEGREES_TO_RADIANS, -26.0 * DEGREES_TO_RADIANS),
        new GeoPoint(planetModel, -90.0 * DEGREES_TO_RADIANS, 0.0),
        new GeoPoint(planetModel, 54.0 * DEGREES_TO_RADIANS, 165.0 * DEGREES_TO_RADIANS),
        new GeoPoint(planetModel, -90.0 * DEGREES_TO_RADIANS, 0.0)};
    final GeoPath path = GeoPathFactory.makeGeoPath(planetModel, 29 * DEGREES_TO_RADIANS, pathPoints);
    final Shape shape = new Geo3dShape<>(path,ctx);
    final Rectangle rect = ctx.makeRectangle(131, 143, 39, 54);
    testOperation(rect,SpatialOperation.Intersects,shape,true);
  }

  @Test
  public void testOperations() throws IOException {
    setupStrategy();

    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  @Override
  protected Shape randomIndexedShape() {
    int type = shapeGenerator.randomShapeType();
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, planetModel);
    if (areaShape instanceof GeoPointShape) {
      return new Geo3dPointShape((GeoPointShape) areaShape, ctx);
    }
    return new Geo3dShape<>(areaShape, ctx);
  }

  @Override
  protected Shape randomQueryShape() {
    int type = shapeGenerator.randomShapeType();
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, planetModel);
    return new Geo3dShape<>(areaShape, ctx);
  }

  @Test
  public void testOperationsFromFile() throws IOException {
    setupStrategy();
    final Iterator<SpatialTestData> indexedSpatialData = getSampleData( "states-poly.txt");
    final List<Shape> indexedShapes = new ArrayList<>();
    while(indexedSpatialData.hasNext()) {
      indexedShapes.add(indexedSpatialData.next().shape);
    }
    final Iterator<SpatialTestData> querySpatialData = getSampleData( "states-bbox.txt");
    final List<Shape> queryShapes = new ArrayList<>();
    while(querySpatialData.hasNext()) {
      queryShapes.add(querySpatialData.next().shape);
      if (TEST_NIGHTLY) {
        queryShapes.add(randomQueryShape());
      }
    }
    queryShapes.add(randomQueryShape());
    testOperation(SpatialOperation.Intersects, indexedShapes, queryShapes, random().nextBoolean());
  }
}
