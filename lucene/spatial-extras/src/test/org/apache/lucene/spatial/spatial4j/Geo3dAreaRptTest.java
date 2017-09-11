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
import java.util.HashMap;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.RandomGeo3dShapeGenerator;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

public class Geo3dAreaRptTest extends RandomSpatialOpStrategyTestCase {

  private SpatialPrefixTree grid;
  private RecursivePrefixTreeStrategy rptStrategy;

  SpatialContext ctx;
  Geo3dSpatialContextFactory factory;
  RandomGeo3dShapeGenerator shapeGenerator = new RandomGeo3dShapeGenerator();

  private void setupGeohashGrid() {
    PlanetModel planetModel = shapeGenerator.randomPlanetModel();
    factory = new Geo3dSpatialContextFactory();
    factory.planetModel = planetModel;
    ctx = factory.newSpatialContext();
    this.grid = new GeohashPrefixTree(ctx, 2);//A fairly shallow grid
    this.rptStrategy = newRPT();
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    final RecursivePrefixTreeStrategy rpt = new RecursivePrefixTreeStrategy(this.grid,
        getClass().getSimpleName() + "_rpt");
    rpt.setDistErrPct(0.10);//not too many cells
    return rpt;
  }

  private void setupStrategy() {
    //setup
    setupGeohashGrid();

    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.strategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        rptStrategy, serializedDVStrategy);
  }

  @Test
  @Repeat(iterations = 10)
  public void testOperations() throws IOException {
    setupStrategy();

    testOperationRandomShapes(SpatialOperation.Intersects);
  }

  @Override
  protected Shape randomIndexedShape() {
    int type = shapeGenerator.randomShapeType();
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, factory.planetModel);
    return new Geo3dAreaShape<>(areaShape, ctx);
  }

  @Override
  protected Shape randomQueryShape() {
    int type = shapeGenerator.randomShapeType();
    GeoAreaShape areaShape = shapeGenerator.randomGeoAreaShape(type, factory.planetModel);
    return new Geo3dAreaShape<>(areaShape, ctx);
  }

  @Test
  public void testWKT() throws Exception {
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    SpatialContext ctx = factory.newSpatialContext();
    String wkt = "POLYGON ((20.0 -60.4, 20.1 -60.4, 20.1 -60.3, 20.0  -60.3,20.0 -60.4))";
    Shape s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "POINT (30 10)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "LINESTRING (30 10, 10 30, 40 40)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), ((20 35, 10 30, 10 10, 30 5, 45 20, 20 35),(30 20, 20 15, 20 25, 30 20)))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "GEOMETRYCOLLECTION(POINT(4 6),LINESTRING(4 6,7 10))";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "ENVELOPE(1, 2, 4, 3)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    wkt = "BUFFER(POINT(-10 30), 5.2)";
    s = ctx.getFormats().getWktReader().read(wkt);
    assertTrue(s instanceof  Geo3dAreaShape<?>);
    //wkt = "BUFFER(LINESTRING(1 2, 3 4), 0.5)";
    //s = ctx.getFormats().getWktReader().read(wkt);
    //assertTrue(s instanceof  Geo3dAreaShape<?>);
  }
}
