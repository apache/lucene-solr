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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static com.spatial4j.core.shape.SpatialRelation.CONTAINS;
import static com.spatial4j.core.shape.SpatialRelation.DISJOINT;
import static com.spatial4j.core.shape.SpatialRelation.INTERSECTS;
import static com.spatial4j.core.shape.SpatialRelation.WITHIN;

public class SpatialOpRecursivePrefixTreeTest extends StrategyTestCase {

  private SpatialPrefixTree grid;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    deleteAll();
  }

  public void mySetup(int maxLevels) throws IOException {
    //non-geospatial makes this test a little easier (in gridSnap), and using boundary values 2^X raises
    // the prospect of edge conditions we want to test, plus makes for simpler numbers (no decimals).
    this.ctx = new SpatialContext(false, null, new RectangleImpl(0, 256, -128, 128, null));
    //A fairly shallow grid, and default 2.5% distErrPct
    if (maxLevels == -1)
      maxLevels = randomIntBetween(1, 8);
    this.grid = new QuadPrefixTree(ctx, maxLevels);
    this.strategy = new RecursivePrefixTreeStrategy(grid, getClass().getSimpleName());
    //((PrefixTreeStrategy) strategy).setDistErrPct(0);//fully precise to grid

    System.out.println("Strategy: " + strategy.toString());
  }

  @Test
  @Repeat(iterations = 10)
  public void testIntersects() throws IOException {
    mySetup(-1);
    doTest(SpatialOperation.Intersects);
  }

  @Test
  @Repeat(iterations = 10)
  public void testWithin() throws IOException {
    mySetup(-1);
    doTest(SpatialOperation.IsWithin);
  }

  @Test
  @Repeat(iterations = 10)
  public void testContains() throws IOException {
    mySetup(-1);
    doTest(SpatialOperation.Contains);
  }

  @Test
  @Repeat(iterations = 10)
  public void testDisjoint() throws IOException {
    mySetup(-1);
    doTest(SpatialOperation.IsDisjointTo);
  }

  @Test
  public void testWithinDisjointParts() throws IOException {
    mySetup(7);
    //one shape comprised of two parts, quite separated apart
    adoc("0", new ShapePair(ctx.makeRectangle(0, 10, -120, -100), ctx.makeRectangle(220, 240, 110, 125), false));
    commit();
    //query surrounds only the second part of the indexed shape
    Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.IsWithin,
        ctx.makeRectangle(210, 245, 105, 128)));
    SearchResults searchResults = executeQuery(query, 1);
    //we shouldn't find it because it's not completely within
    assertTrue(searchResults.numFound == 0);
  }

  @Test /** LUCENE-4916 */
  public void testWithinLeafApproxRule() throws IOException {
    mySetup(2);//4x4 grid
    //indexed shape will simplify to entire right half (2 top cells)
    adoc("0", ctx.makeRectangle(192, 204, -128, 128));
    commit();

    ((RecursivePrefixTreeStrategy) strategy).setPrefixGridScanLevel(randomInt(2));

    //query does NOT contain it; both indexed cells are leaves to the query, and
    // when expanded to the full grid cells, the top one's top row is disjoint
    // from the query and thus not a match.
    assertTrue(executeQuery(strategy.makeQuery(
        new SpatialArgs(SpatialOperation.IsWithin, ctx.makeRectangle(38, 192, -72, 56))
    ), 1).numFound==0);//no-match

    //this time the rect is a little bigger and is considered a match. It's a
    // an acceptable false-positive because of the grid approximation.
    assertTrue(executeQuery(strategy.makeQuery(
        new SpatialArgs(SpatialOperation.IsWithin, ctx.makeRectangle(38, 192, -72, 80))
    ), 1).numFound==1);//match
  }

  //Override so we can index parts of a pair separately, resulting in the detailLevel
  // being independent for each shape vs the whole thing
  @Override
  protected Document newDoc(String id, Shape shape) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    if (shape != null) {
      Collection<Shape> shapes;
      if (shape instanceof ShapePair) {
        shapes = new ArrayList<Shape>(2);
        shapes.add(((ShapePair)shape).shape1);
        shapes.add(((ShapePair)shape).shape2);
      } else {
        shapes = Collections.singleton(shape);
      }
      for (Shape shapei : shapes) {
        for (Field f : strategy.createIndexableFields(shapei)) {
          doc.add(f);
        }
      }
      if (storeShape)
        doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));
    }
    return doc;
  }

  private void doTest(final SpatialOperation operation) throws IOException {
    final boolean biasContains = (operation == SpatialOperation.Contains);

    Map<String, Shape> indexedShapes = new LinkedHashMap<String, Shape>();
    Map<String, Shape> indexedShapesGS = new LinkedHashMap<String, Shape>();
    final int numIndexedShapes = randomIntBetween(1, 6);
    for (int i = 0; i < numIndexedShapes; i++) {
      String id = "" + i;
      Shape indexedShape;
      Shape indexedShapeGS; //(grid-snapped)
      int R = random().nextInt(12);
      if (R == 0) {//1 in 10
        indexedShape = null; //no shape for this doc
        indexedShapeGS = null;
      } else if (R % 4 == 0) {//3 in 12
        //comprised of more than one shape
        Rectangle shape1 = randomRectangle();
        Rectangle shape2 = randomRectangle();
        indexedShape = new ShapePair(shape1, shape2, biasContains);
        indexedShapeGS = new ShapePair(gridSnap(shape1), gridSnap(shape2), biasContains);
      } else {
        //just one shape
        indexedShape = randomRectangle();
        indexedShapeGS = gridSnap(indexedShape);
      }
      indexedShapes.put(id, indexedShape);
      indexedShapesGS.put(id, indexedShapeGS);

      adoc(id, indexedShape);

      if (random().nextInt(10) == 0)
        commit();//intermediate commit, produces extra segments

    }
    Iterator<String> idIter = indexedShapes.keySet().iterator();
    while (idIter.hasNext()) {
      String id = idIter.next();
      if (random().nextInt(10) == 0) {
        deleteDoc(id);
        idIter.remove();
        indexedShapesGS.remove(id);
      }
    }

    commit();

    final int numQueryShapes = atLeast(20);
    for (int i = 0; i < numQueryShapes; i++) {
      int scanLevel = randomInt(grid.getMaxLevels());
      ((RecursivePrefixTreeStrategy) strategy).setPrefixGridScanLevel(scanLevel);
      final Shape queryShape = randomRectangle();

      final boolean DISJOINT = operation.equals(SpatialOperation.IsDisjointTo);

      //Generate truth via brute force:
      // We really try to ensure true-positive matches (if the predicate on the raw shapes match
      //  then the search should find those same matches).
      // approximations, false-positive matches
      Set <String> expectedIds = new LinkedHashSet<String>();//true-positives
      Set<String> secondaryIds = new LinkedHashSet<String>();//false-positives (unless disjoint)
      for (Map.Entry<String, Shape> entry : indexedShapes.entrySet()) {
        Shape indexedShapeCompare = entry.getValue();
        if (indexedShapeCompare == null)
          continue;
        Shape queryShapeCompare = queryShape;
        String id = entry.getKey();
        if (operation.evaluate(indexedShapeCompare, queryShapeCompare)) {
          expectedIds.add(id);
          if (DISJOINT) {
            //if no longer intersect after buffering them, for disjoint, remember this
            indexedShapeCompare = indexedShapesGS.get(entry.getKey());
            queryShapeCompare = gridSnap(queryShape);
            if (!operation.evaluate(indexedShapeCompare, queryShapeCompare))
              secondaryIds.add(id);
          }
        } else if (!DISJOINT) {
          //buffer either the indexed or query shape (via gridSnap) and try again
          if (operation.equals(SpatialOperation.Intersects)) {
            indexedShapeCompare = indexedShapesGS.get(entry.getKey());
            queryShapeCompare = gridSnap(queryShape);
          } else if (operation.equals(SpatialOperation.Contains)) {
            indexedShapeCompare = indexedShapesGS.get(entry.getKey());
          } else if (operation.equals(SpatialOperation.IsWithin)) {
            queryShapeCompare = gridSnap(queryShape);
          }
          if (operation.evaluate(indexedShapeCompare, queryShapeCompare))
            secondaryIds.add(id);
        }
      }

      //Search and verify results
      SpatialArgs args = new SpatialArgs(operation, queryShape);
      Query query = strategy.makeQuery(args);
      SearchResults got = executeQuery(query, 100);
      Set<String> remainingExpectedIds = new LinkedHashSet<String>(expectedIds);
      for (SearchResult result : got.results) {
        String id = result.getId();
        boolean removed = remainingExpectedIds.remove(id);
        if (!removed && (!DISJOINT && !secondaryIds.contains(id))) {
          fail("Shouldn't match", id, indexedShapes, indexedShapesGS, queryShape);
        }
      }
      if (DISJOINT)
        remainingExpectedIds.removeAll(secondaryIds);
      if (!remainingExpectedIds.isEmpty()) {
        String id = remainingExpectedIds.iterator().next();
        fail("Should have matched", id, indexedShapes, indexedShapesGS, queryShape);
      }
    }
  }

  private void fail(String label, String id, Map<String, Shape> indexedShapes, Map<String, Shape> indexedShapesGS, Shape queryShape) {
    System.err.println("Ig:" + indexedShapesGS.get(id) + " Qg:" + gridSnap(queryShape));
    fail(label + " I #" + id + ":" + indexedShapes.get(id) + " Q:" + queryShape);
  }


//  private Rectangle inset(Rectangle r) {
//    //typically inset by 1 (whole numbers are easy to read)
//    double d = Math.min(1.0, grid.getDistanceForLevel(grid.getMaxLevels()) / 4);
//    return ctx.makeRectangle(r.getMinX() + d, r.getMaxX() - d, r.getMinY() + d, r.getMaxY() - d);
//  }

  protected Rectangle gridSnap(Shape snapMe) {
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

  /**
   * An aggregate of 2 shapes. Only implements what's necessary for the test
   * here. TODO replace with Spatial4j trunk ShapeCollection.
   */
  private class ShapePair implements Shape {

    final Rectangle shape1, shape2;
    final boolean biasContainsThenWithin;//a hack

    public ShapePair(Rectangle shape1, Rectangle shape2, boolean containsThenWithin) {
      this.shape1 = shape1;
      this.shape2 = shape2;
      biasContainsThenWithin = containsThenWithin;
    }

    @Override
    public SpatialRelation relate(Shape other) {
      SpatialRelation r = relateApprox(other);
      if (r != INTERSECTS)
        return r;
      //See if the correct answer is actually Contains
      Rectangle oRect = (Rectangle)other;
      boolean pairTouches = shape1.relate(shape2).intersects();
      if (!pairTouches)
        return r;
      //test all 4 corners
      if (relate(ctx.makePoint(oRect.getMinX(), oRect.getMinY())) == CONTAINS
          && relate(ctx.makePoint(oRect.getMinX(), oRect.getMaxY())) == CONTAINS
          && relate(ctx.makePoint(oRect.getMaxX(), oRect.getMinY())) == CONTAINS
          && relate(ctx.makePoint(oRect.getMaxX(), oRect.getMaxY())) == CONTAINS)
        return CONTAINS;
      return r;
    }

    private SpatialRelation relateApprox(Shape other) {
      if (biasContainsThenWithin) {
        if (shape1.relate(other) == CONTAINS || shape1.equals(other)
            || shape2.relate(other) == CONTAINS || shape2.equals(other)) return CONTAINS;

        if (shape1.relate(other) == WITHIN && shape2.relate(other) == WITHIN) return WITHIN;

      } else {
        if ((shape1.relate(other) == WITHIN || shape1.equals(other))
            && (shape2.relate(other) == WITHIN || shape2.equals(other))) return WITHIN;

        if (shape1.relate(other) == CONTAINS || shape2.relate(other) == CONTAINS) return CONTAINS;
      }

      if (shape1.relate(other).intersects() || shape2.relate(other).intersects())
        return INTERSECTS;//might actually be 'CONTAINS' if these 2 are adjacent
      return DISJOINT;
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

    @Override
    public String toString() {
      return "ShapePair(" + shape1 + " , " + shape2 + ")";
    }
  }

}
