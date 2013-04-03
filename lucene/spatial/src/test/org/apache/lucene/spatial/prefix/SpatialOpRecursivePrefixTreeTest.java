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
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

public class SpatialOpRecursivePrefixTreeTest extends StrategyTestCase {

  private SpatialPrefixTree grid;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    deleteAll();
  }

  public void mySetup() throws IOException {
    //non-geospatial makes this test a little easier (in gridSnap), and using boundary values 2^X raises
    // the prospect of edge conditions we want to test, plus makes for simpler numbers (no decimals).
    this.ctx = new SpatialContext(false, null, new RectangleImpl(0, 256, -128, 128, null));
    //A fairly shallow grid, and default 2.5% distErrPct
    this.grid = new QuadPrefixTree(ctx, randomIntBetween(1, 8));
    this.strategy = new RecursivePrefixTreeStrategy(grid, getClass().getSimpleName());
    //((PrefixTreeStrategy) strategy).setDistErrPct(0);//fully precise to grid

    System.out.println("Strategy: "+strategy.toString());
  }

  @Test
  @Repeat(iterations = 10)
  public void testIntersects() throws IOException {
    mySetup();
    doTest(SpatialOperation.Intersects);
  }

  @Test
  @Repeat(iterations = 10)
  public void testWithin() throws IOException {
    mySetup();
    doTest(SpatialOperation.IsWithin);
  }

  @Test
  @Repeat(iterations = 10)
  public void testContains() throws IOException {
    mySetup();
    doTest(SpatialOperation.Contains);
  }

  @Test
  public void testWithinDisjointParts() throws IOException {
    this.ctx = new SpatialContext(false, null, new RectangleImpl(0, 256, -128, 128, null));
    //A fairly shallow grid, and default 2.5% distErrPct
    this.grid = new QuadPrefixTree(ctx, 7);
    this.strategy = new RecursivePrefixTreeStrategy(grid, getClass().getSimpleName());

    //one shape comprised of two parts, quite separated apart
    adoc("0", new ShapePair(ctx.makeRectangle(0, 10, -120, -100), ctx.makeRectangle(220, 240, 110, 125)));
    commit();
    //query surrounds only the second part of the indexed shape
    Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.IsWithin, ctx.makeRectangle(210, 245, 105, 128)));
    SearchResults searchResults = executeQuery(query, 1);
    //we shouldn't find it because it's not completely within
    assertTrue(searchResults.numFound==0);
  }

  private void doTest(final SpatialOperation operation) throws IOException {
    Map<String, Shape> indexedShapes = new LinkedHashMap<String, Shape>();
    final int numIndexedShapes = randomIntBetween(1, 6);
    for (int i = 0; i < numIndexedShapes; i++) {
      String id = ""+i;
      Shape indexedShape;
      if (random().nextInt(4) == 0) {
        indexedShape = new ShapePair( gridSnapp(randomRectangle()), gridSnapp(randomRectangle()) );
      } else {
        indexedShape = gridSnapp(randomRectangle());
      }
      indexedShapes.put(id, indexedShape);
      adoc(id, indexedShape);
      if (random().nextInt(10) == 0)
        commit();
    }

    //delete some
    Iterator<String> idIter = indexedShapes.keySet().iterator();
    while (idIter.hasNext()) {
      String id = idIter.next();
      if (random().nextInt(10) == 0) {
        deleteDoc(id);
        idIter.remove();
      }
    }

    commit();

    final int numQueryShapes = atLeast(20);
    for (int i = 0; i < numQueryShapes; i++) {
      int scanLevel = randomInt(grid.getMaxLevels());
      ((RecursivePrefixTreeStrategy) strategy).setPrefixGridScanLevel(scanLevel);
      Shape queryShape = gridSnapp(randomRectangle());

      //Generate truth via brute force
      Set<String> expectedIds = new TreeSet<String>();
      for (Map.Entry<String, Shape> entry : indexedShapes.entrySet()) {
        if (operation.evaluate(entry.getValue(), queryShape))
          expectedIds.add(entry.getKey());
      }

      //Search and verify results
      Query query = strategy.makeQuery(new SpatialArgs(operation, queryShape));
      SearchResults got = executeQuery(query, 100);
      Set<String> remainingExpectedIds = new TreeSet<String>(expectedIds);
      String msg = queryShape.toString()+" Expect: "+expectedIds;
      for (SearchResult result : got.results) {
        String id = result.getId();
        Object removed = remainingExpectedIds.remove(id);
        if (removed == null) {
          fail("Shouldn't match " + id + " ("+ indexedShapes.get(id) +") in " + msg);
        }
      }
      if (!remainingExpectedIds.isEmpty()) {
        Shape firstFailedMatch = indexedShapes.get(remainingExpectedIds.iterator().next());
        fail("Didn't match " + firstFailedMatch + " in " + msg +" (of "+remainingExpectedIds.size()+")");
      }
    }
  }

  protected Rectangle gridSnapp(Shape snapMe) {
    //The next 4 lines mimic PrefixTreeStrategy.createIndexableFields()
    double distErrPct = ((PrefixTreeStrategy) strategy).getDistErrPct();
    double distErr = SpatialArgs.calcDistanceFromErrPct(snapMe, distErrPct, ctx);
    int detailLevel = grid.getLevelForDistance(distErr);
    List<Cell> cells = grid.getCells(snapMe, detailLevel, false, true);

    //calc bounding box of cells.
    double minX = Double.POSITIVE_INFINITY, maxX = Double.NEGATIVE_INFINITY;
    double minY = Double.POSITIVE_INFINITY, maxY = Double.NEGATIVE_INFINITY;
    for (Cell cell : cells) {
      assert cell.getLevel() <= detailLevel;
      Rectangle cellR = cell.getShape().getBoundingBox();

      minX = Math.min(minX, cellR.getMinX());
      maxX = Math.max(maxX, cellR.getMaxX());
      minY = Math.min(minY, cellR.getMinY());
      maxY = Math.max(maxY, cellR.getMaxY());
    }
    return ctx.makeRectangle(minX, maxX, minY, maxY);
  }

  /** An aggregate of 2 shapes. Only implements what's necessary for the test here.
   * TODO replace with Spatial4j trunk ShapeCollection. */
  private class ShapePair implements Shape {

    Shape shape1, shape2;

    public ShapePair(Shape shape1, Shape shape2) {
      this.shape1 = shape1;
      this.shape2 = shape2;
    }

    @Override
    public SpatialRelation relate(Shape other) {
      //easy to observe is correct; not an optimal code path but this is a test
      if (shape1.relate(other) == SpatialRelation.CONTAINS || shape2.relate(other) == SpatialRelation.CONTAINS)
        return SpatialRelation.CONTAINS;
      if (shape1.relate(other) == SpatialRelation.WITHIN && shape2.relate(other) == SpatialRelation.WITHIN)
        return SpatialRelation.WITHIN;
      if (shape1.relate(other).intersects() || shape2.relate(other).intersects())
        return SpatialRelation.INTERSECTS;
      return SpatialRelation.DISJOINT;
    }

    @Override
    public Rectangle getBoundingBox() {
      return ctx.getWorldBounds();//good enough
    }

    @Override
    public boolean hasArea() {
      return true;
    }

    @Override
    public double getArea(SpatialContext ctx) {
      throw new UnsupportedOperationException("TODO unimplemented");//TODO
    }

    @Override
    public Point getCenter() {
      throw new UnsupportedOperationException("TODO unimplemented");//TODO
    }
  }

}
