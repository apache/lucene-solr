package org.apache.lucene.spatial.prefix;

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

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.Node;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

public class SpatialOpRecursivePrefixTreeTest extends StrategyTestCase {

  private SpatialPrefixTree grid;

  @Test
  @Repeat(iterations = 20)
  public void testIntersects() throws IOException {
    //non-geospatial makes this test a little easier
    this.ctx = new SpatialContext(false, null, new RectangleImpl(0, 256, -128, 128, null));
    //A fairly shallow grid, and default 2.5% distErrPct
    this.grid = new QuadPrefixTree(ctx, randomIntBetween(1, 8));
    this.strategy = new RecursivePrefixTreeStrategy(grid, getClass().getSimpleName());
    //((PrefixTreeStrategy) strategy).setDistErrPct(0);//fully precise to grid

    deleteAll();

    Map<String, Shape> indexedShapes = new LinkedHashMap<String, Shape>();
    Map<String, Rectangle> indexedGriddedShapes = new LinkedHashMap<String, Rectangle>();
    final int numIndexedShapes = randomIntBetween(1, 6);
    for (int i = 1; i <= numIndexedShapes; i++) {
      String id = "" + i;
      Shape indexShape = randomRectangle();
      Rectangle gridShape = gridSnapp(indexShape);
      indexedShapes.put(id, indexShape);
      indexedGriddedShapes.put(id, gridShape);
      adoc(id, indexShape);
    }

    commit();

    final int numQueryShapes = atLeast(10);
    for (int i = 0; i < numQueryShapes; i++) {
      int scanLevel = randomInt(grid.getMaxLevels());
      ((RecursivePrefixTreeStrategy) strategy).setPrefixGridScanLevel(scanLevel);
      Rectangle queryShape = randomRectangle();
      Rectangle queryGridShape = gridSnapp(queryShape);

      //Generate truth via brute force
      final SpatialOperation operation = SpatialOperation.Intersects;
      Set<String> expectedIds = new TreeSet<String>();
      Set<String> optionalIds = new TreeSet<String>();
      for (String id : indexedShapes.keySet()) {
        Shape indexShape = indexedShapes.get(id);
        Rectangle indexGridShape = indexedGriddedShapes.get(id);
        if (operation.evaluate(indexShape, queryShape))
          expectedIds.add(id);
        else if (operation.evaluate(indexGridShape, queryGridShape))
          optionalIds.add(id);
      }

      //Search and verify results
      Query query = strategy.makeQuery(new SpatialArgs(operation, queryShape));
      SearchResults got = executeQuery(query, 100);
      Set<String> remainingExpectedIds = new TreeSet<String>(expectedIds);
      String msg = queryShape.toString()+" Expect: "+expectedIds+" Opt: "+optionalIds;
      for (SearchResult result : got.results) {
        String id = result.getId();
        Object removed = remainingExpectedIds.remove(id);
        if (removed == null) {
          assertTrue("Shouldn't match " + id + " in "+msg, optionalIds.contains(id));
        }
      }
      assertTrue("Didn't match " + remainingExpectedIds + " in " + msg, remainingExpectedIds.isEmpty());
    }

  }

  protected Rectangle gridSnapp(Shape snapMe) {
    //The next 4 lines mimic PrefixTreeStrategy.createIndexableFields()
    double distErrPct = ((PrefixTreeStrategy) strategy).getDistErrPct();
    double distErr = SpatialArgs.calcDistanceFromErrPct(snapMe, distErrPct, ctx);
    int detailLevel = grid.getLevelForDistance(distErr);
    List<Node> cells = grid.getNodes(snapMe, detailLevel, false, true);

    //calc bounding box of cells.
    double minX = Double.POSITIVE_INFINITY, maxX = Double.NEGATIVE_INFINITY;
    double minY = Double.POSITIVE_INFINITY, maxY = Double.NEGATIVE_INFINITY;
    for (Node cell : cells) {
      assert cell.getLevel() <= detailLevel;
      Rectangle cellR = cell.getShape().getBoundingBox();

      minX = Math.min(minX, cellR.getMinX());
      maxX = Math.max(maxX, cellR.getMaxX());
      minY = Math.min(minY, cellR.getMinY());
      maxY = Math.max(maxY, cellR.getMaxY());
    }
    return ctx.makeRectangle(minX, maxX, minY, maxY);
  }

}
