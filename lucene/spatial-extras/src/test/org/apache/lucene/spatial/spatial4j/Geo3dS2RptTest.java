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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.spatial.composite.CompositeSpatialStrategy;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.S2PrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoPointShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;
import org.apache.lucene.spatial3d.geom.RandomGeo3dShapeGenerator;
import org.junit.Test;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Test using S2 prefix tree. We can merge this with Geo3dRptTest and use
 * random trees.
 */
public class Geo3dS2RptTest extends RandomSpatialOpStrategyTestCase {

  private PlanetModel planetModel;
  private RandomGeo3dShapeGenerator shapeGenerator;
  private SpatialPrefixTree grid;
  private RecursivePrefixTreeStrategy rptStrategy;

  private void setupS2Grid() {
    int arity = random().nextInt(3) + 1;
    this.grid = new S2PrefixTree(ctx, 5, arity);//A fairly shallow grid
    this.rptStrategy = newRPT();
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    final RecursivePrefixTreeStrategy rpt = new RecursivePrefixTreeStrategy(this.grid,
        getClass().getSimpleName() + "_rpt");
    rpt.setDistErrPct(0.10);//not too many cells
    rpt.setPruneLeafyBranches(false);
    return rpt;
  }

  private void setupStrategy() {
    shapeGenerator = new RandomGeo3dShapeGenerator();
    planetModel = shapeGenerator.randomPlanetModel();
    Geo3dSpatialContextFactory factory = new Geo3dSpatialContextFactory();
    factory.planetModel = planetModel;
    ctx = factory.newSpatialContext();

    setupS2Grid();

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
}
