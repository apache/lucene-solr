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
package org.apache.lucene.spatial.composite;

import java.io.IOException;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.serialized.SerializedDVStrategy;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomDouble;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

public class CompositeStrategyTest extends RandomSpatialOpStrategyTestCase {

  private SpatialPrefixTree grid;
  private RecursivePrefixTreeStrategy rptStrategy;

  private void setupQuadGrid(int maxLevels) {
    //non-geospatial makes this test a little easier (in gridSnap), and using boundary values 2^X raises
    // the prospect of edge conditions we want to test, plus makes for simpler numbers (no decimals).
    SpatialContextFactory factory = new SpatialContextFactory();
    factory.geo = false;
    factory.worldBounds = new RectangleImpl(0, 256, -128, 128, null);
    this.ctx = factory.newSpatialContext();
    //A fairly shallow grid
    if (maxLevels == -1)
      maxLevels = randomIntBetween(1, 8);//max 64k cells (4^8), also 256*256
    this.grid = new QuadPrefixTree(ctx, maxLevels);
    this.rptStrategy = newRPT();
  }

  private void setupGeohashGrid(int maxLevels) {
    this.ctx = SpatialContext.GEO;
    //A fairly shallow grid
    if (maxLevels == -1)
      maxLevels = randomIntBetween(1, 3);//max 16k cells (32^3)
    this.grid = new GeohashPrefixTree(ctx, maxLevels);
    this.rptStrategy = newRPT();
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    final RecursivePrefixTreeStrategy rpt = new RecursivePrefixTreeStrategy(this.grid,
        getClass().getSimpleName() + "_rpt");
    rpt.setDistErrPct(0.10);//not too many cells
    return rpt;
  }

  @Test
  @Repeat(iterations = 20)
  public void testOperations() throws IOException {
    //setup
    if (randomBoolean()) {
      setupQuadGrid(-1);
    } else {
      setupGeohashGrid(-1);
    }
    SerializedDVStrategy serializedDVStrategy = new SerializedDVStrategy(ctx, getClass().getSimpleName() + "_sdv");
    this.strategy = new CompositeSpatialStrategy("composite_" + getClass().getSimpleName(),
        rptStrategy, serializedDVStrategy);

    //Do it!

    for (SpatialOperation pred : SpatialOperation.values()) {
      if (pred == SpatialOperation.BBoxIntersects || pred == SpatialOperation.BBoxWithin) {
        continue;
      }
      if (pred == SpatialOperation.IsDisjointTo) {//TODO
        continue;
      }
      testOperationRandomShapes(pred);
      deleteAll();
      commit();
    }
  }

  @Override
  protected Shape randomIndexedShape() {
    return randomShape();
  }

  @Override
  protected Shape randomQueryShape() {
    return randomShape();
  }

  private Shape randomShape() {
    return random().nextBoolean() ? randomCircle() : randomRectangle();
  }

  //TODO move up
  private Shape randomCircle() {
    final Point point = randomPoint();
    //TODO pick using gaussian
    double radius;
    if (ctx.isGeo()) {
      radius = randomDouble() * 100;
    } else {
      //find distance to closest edge
      final Rectangle worldBounds = ctx.getWorldBounds();
      double maxRad = point.getX() - worldBounds.getMinX();
      maxRad = Math.min(maxRad, worldBounds.getMaxX() - point.getX());
      maxRad = Math.min(maxRad, point.getY() - worldBounds.getMinY());
      maxRad = Math.min(maxRad, worldBounds.getMaxY() - point.getY());
      radius = randomDouble() * maxRad;
    }

    return ctx.makeCircle(point, radius);
  }
}
