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
package org.apache.lucene.spatial.prefix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.Cell;
import org.apache.lucene.spatial.prefix.tree.CellIterator;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.PackedQuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.SpatialRelation;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomInt;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;
import static org.locationtech.spatial4j.shape.SpatialRelation.CONTAINS;
import static org.locationtech.spatial4j.shape.SpatialRelation.DISJOINT;
import static org.locationtech.spatial4j.shape.SpatialRelation.INTERSECTS;
import static org.locationtech.spatial4j.shape.SpatialRelation.WITHIN;

/** Randomized PrefixTree test that considers the fuzziness of the
 * results introduced by grid approximation. */
public class RandomSpatialOpFuzzyPrefixTreeTest extends StrategyTestCase {

  static final int ITERATIONS = 10;

  protected SpatialPrefixTree grid;
  private SpatialContext ctx2D;

  public void setupGrid(int maxLevels) throws IOException {
    if (randomBoolean())
      setupQuadGrid(maxLevels, randomBoolean());
    else
      setupGeohashGrid(maxLevels);
    setupCtx2D(ctx);

    // set prune independently on strategy & grid randomly; should work
    ((RecursivePrefixTreeStrategy)strategy).setPruneLeafyBranches(randomBoolean());
    if (this.grid instanceof PackedQuadPrefixTree) {
      ((PackedQuadPrefixTree) this.grid).setPruneLeafyBranches(randomBoolean());
    }

    if (maxLevels == -1 && rarely()) {
      ((PrefixTreeStrategy) strategy).setPointsOnly(true);
    }

    log.info("Strategy: " +  strategy.toString()); // nowarn
  }

  private void setupCtx2D(SpatialContext ctx) {
    if (!ctx.isGeo())
      ctx2D = ctx;
    //A non-geo version of ctx.
    SpatialContextFactory ctxFactory = new SpatialContextFactory();
    ctxFactory.geo = false;
    ctxFactory.worldBounds = ctx.getWorldBounds();
    ctx2D = ctxFactory.newSpatialContext();
  }

  private void setupQuadGrid(int maxLevels, boolean packedQuadPrefixTree) {
    //non-geospatial makes this test a little easier (in gridSnap), and using boundary values 2^X raises
    // the prospect of edge conditions we want to test, plus makes for simpler numbers (no decimals).
    SpatialContextFactory factory = new SpatialContextFactory();
    factory.geo = false;
    factory.worldBounds = new RectangleImpl(0, 256, -128, 128, null);
    this.ctx = factory.newSpatialContext();
    //A fairly shallow grid, and default 2.5% distErrPct
    if (maxLevels == -1)
      maxLevels = randomIntBetween(1, 8);//max 64k cells (4^8), also 256*256
    if (packedQuadPrefixTree) {
      this.grid = new PackedQuadPrefixTree(ctx, maxLevels);
    } else {
      this.grid = new QuadPrefixTree(ctx, maxLevels);
    }
    this.strategy = newRPT();
  }

  public void setupGeohashGrid(int maxLevels) {
    this.ctx = SpatialContext.GEO;
    //A fairly shallow grid, and default 2.5% distErrPct
    if (maxLevels == -1)
      maxLevels = randomIntBetween(1, 3);//max 16k cells (32^3)
    this.grid = new GeohashPrefixTree(ctx, maxLevels);
    this.strategy = newRPT();
  }

  protected RecursivePrefixTreeStrategy newRPT() {
    return new RecursivePrefixTreeStrategy(this.grid, getClass().getSimpleName());
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testIntersects() throws IOException {
    setupGrid(-1);
    doTest(SpatialOperation.Intersects);
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testWithin() throws IOException {
    setupGrid(-1);
    doTest(SpatialOperation.IsWithin);
  }

  @Test
  @Repeat(iterations = ITERATIONS)
  public void testContains() throws IOException {
    setupGrid(-1);
    doTest(SpatialOperation.Contains);
  }

  @Test
  public void testPackedQuadPointsOnlyBug() throws IOException {
    setupQuadGrid(1, true); // packed quad.  maxLevels doesn't matter.
    setupCtx2D(ctx);
    ((PrefixTreeStrategy) strategy).setPointsOnly(true);
    Point point = ctx.makePoint(169.0, 107.0);
    adoc("0", point);
    commit();
    Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.Intersects, point));
    assertEquals(1, executeQuery(query, 1).numFound);
  }

  @Test
  public void testPointsOnlyOptBug() throws IOException {
    setupQuadGrid(8, false);
    setupCtx2D(ctx);
    ((PrefixTreeStrategy) strategy).setPointsOnly(true);
    Point point = ctx.makePoint(86, -127.44362190053255);
    adoc("0", point);
    commit();
    Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.Intersects,
        ctx.makeRectangle(point, point)));
    assertEquals(1, executeQuery(query, 1).numFound);
  }

  /** See LUCENE-5062, {@link ContainsPrefixTreeQuery#multiOverlappingIndexedShapes}. */
  @Test
  public void testContainsPairOverlap() throws IOException {
    setupQuadGrid(3, randomBoolean());
    adoc("0", new ShapePair(ctx.makeRectangle(0, 33, -128, 128), ctx.makeRectangle(33, 128, -128, 128), true));
    commit();
    Query query = strategy.makeQuery(new SpatialArgs(SpatialOperation.Contains,
        ctx.makeRectangle(0, 128, -16, 128)));
    SearchResults searchResults = executeQuery(query, 1);
    assertEquals(1, searchResults.numFound);
  }

  @Test
  public void testWithinDisjointParts() throws IOException {
    setupQuadGrid(7, randomBoolean());
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
    setupQuadGrid(2, randomBoolean());//4x4 grid
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

    //this time the rect is a little bigger and is considered a match. It's
    // an acceptable false-positive because of the grid approximation.
    assertTrue(executeQuery(strategy.makeQuery(
        new SpatialArgs(SpatialOperation.IsWithin, ctx.makeRectangle(38, 192, -72, 80))
    ), 1).numFound==1);//match
  }

  @Test
  public void testShapePair() {
    ctx = SpatialContext.GEO;
    setupCtx2D(ctx);

    Shape leftShape = new ShapePair(ctx.makeRectangle(-74, -56, -8, 1), ctx.makeRectangle(-180, 134, -90, 90), true);
    Shape queryShape = ctx.makeRectangle(-180, 180, -90, 90);
    assertEquals(SpatialRelation.WITHIN, leftShape.relate(queryShape));
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
        shapes = new ArrayList<>(2);
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
      if (storeShape)//just for diagnostics
        doc.add(new StoredField(strategy.getFieldName(), shape.toString()));
    }
    return doc;
  }

  @SuppressWarnings("fallthrough")
  private void doTest(final SpatialOperation operation) throws IOException {
    //first show that when there's no data, a query will result in no results
    {
      Query query = strategy.makeQuery(new SpatialArgs(operation, randomRectangle()));
      SearchResults searchResults = executeQuery(query, 1);
      assertEquals(0, searchResults.numFound);
    }

    final boolean biasContains = (operation == SpatialOperation.Contains);

    //Main index loop:
    Map<String, Shape> indexedShapes = new LinkedHashMap<>();
    Map<String, Shape> indexedShapesGS = new LinkedHashMap<>();//grid snapped
    final int numIndexedShapes = randomIntBetween(1, 6);
    boolean indexedAtLeastOneShapePair = false;
    final boolean pointsOnly = ((PrefixTreeStrategy) strategy).isPointsOnly();
    for (int i = 0; i < numIndexedShapes; i++) {
      String id = "" + i;
      Shape indexedShape;
      int R = random().nextInt(12);
      if (R == 0) {//1 in 12
        indexedShape = null;
      } else if (R == 1 || pointsOnly) {//1 in 12
        indexedShape = randomPoint();//just one point
      } else if (R <= 4) {//3 in 12
        //comprised of more than one shape
        indexedShape = randomShapePairRect(biasContains);
        indexedAtLeastOneShapePair = true;
      } else {
        indexedShape = randomRectangle();//just one rect
      }

      indexedShapes.put(id, indexedShape);
      indexedShapesGS.put(id, gridSnap(indexedShape));

      adoc(id, indexedShape);

      if (random().nextInt(10) == 0)
        commit();//intermediate commit, produces extra segments

    }
    //delete some documents randomly
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

    //Main query loop:
    final int numQueryShapes = atLeast(20);
    for (int i = 0; i < numQueryShapes; i++) {
      int scanLevel = randomInt(grid.getMaxLevels());
      ((RecursivePrefixTreeStrategy) strategy).setPrefixGridScanLevel(scanLevel);

      final Shape queryShape;
      switch (randomInt(10)) {
        case 0: queryShape = randomPoint(); break;
// LUCENE-5549
//TODO debug: -Dtests.method=testWithin -Dtests.multiplier=3 -Dtests.seed=5F5294CE2E075A3E:AAD2F0F79288CA64
//        case 1:case 2:case 3:
//          if (!indexedAtLeastOneShapePair) { // avoids ShapePair.relate(ShapePair), which isn't reliable
//            queryShape = randomShapePairRect(!biasContains);//invert biasContains for query side
//            break;
//          }

        case 4:
          //choose an existing indexed shape
          if (!indexedShapes.isEmpty()) {
            Shape tmp = indexedShapes.values().iterator().next();
            if (tmp instanceof Point || tmp instanceof Rectangle) {//avoids null and shapePair
              queryShape = tmp;
              break;
            }
          }//else fall-through

        default: queryShape = randomRectangle();
      }
      final Shape queryShapeGS = gridSnap(queryShape);

      final boolean opIsDisjoint = operation == SpatialOperation.IsDisjointTo;

      //Generate truth via brute force:
      // We ensure true-positive matches (if the predicate on the raw shapes match
      //  then the search should find those same matches).
      // approximations, false-positive matches
      Set<String> expectedIds = new LinkedHashSet<>();//true-positives
      Set<String> secondaryIds = new LinkedHashSet<>();//false-positives (unless disjoint)
      for (Map.Entry<String, Shape> entry : indexedShapes.entrySet()) {
        String id = entry.getKey();
        Shape indexedShapeCompare = entry.getValue();
        if (indexedShapeCompare == null)
          continue;
        Shape queryShapeCompare = queryShape;

        if (operation.evaluate(indexedShapeCompare, queryShapeCompare)) {
          expectedIds.add(id);
          if (opIsDisjoint) {
            //if no longer intersect after buffering them, for disjoint, remember this
            indexedShapeCompare = indexedShapesGS.get(id);
            queryShapeCompare = queryShapeGS;
            if (!operation.evaluate(indexedShapeCompare, queryShapeCompare))
              secondaryIds.add(id);
          }
        } else if (!opIsDisjoint) {
          //buffer either the indexed or query shape (via gridSnap) and try again
          if (operation == SpatialOperation.Intersects) {
            indexedShapeCompare = indexedShapesGS.get(id);
            queryShapeCompare = queryShapeGS;
            //TODO Unfortunately, grid-snapping both can result in intersections that otherwise
            // wouldn't happen when the grids are adjacent. Not a big deal but our test is just a
            // bit more lenient.
          } else if (operation == SpatialOperation.Contains) {
            indexedShapeCompare = indexedShapesGS.get(id);
          } else if (operation == SpatialOperation.IsWithin) {
            queryShapeCompare = queryShapeGS;
          }
          if (operation.evaluate(indexedShapeCompare, queryShapeCompare))
            secondaryIds.add(id);
        }
      }

      //Search and verify results
      SpatialArgs args = new SpatialArgs(operation, queryShape);
      if (queryShape instanceof ShapePair)
        args.setDistErrPct(0.0);//a hack; we want to be more detailed than gridSnap(queryShape)
      Query query = strategy.makeQuery(args);
      SearchResults got = executeQuery(query, 100);
      Set<String> remainingExpectedIds = new LinkedHashSet<>(expectedIds);
      for (SearchResult result : got.results) {
        String id = result.getId();
        boolean removed = remainingExpectedIds.remove(id);
        if (!removed && (!opIsDisjoint && !secondaryIds.contains(id))) {
          fail("Shouldn't match", id, indexedShapes, indexedShapesGS, queryShape);
        }
      }
      if (opIsDisjoint)
        remainingExpectedIds.removeAll(secondaryIds);
      if (!remainingExpectedIds.isEmpty()) {
        String id = remainingExpectedIds.iterator().next();
        fail("Should have matched", id, indexedShapes, indexedShapesGS, queryShape);
      }
    }
  }

  private Shape randomShapePairRect(boolean biasContains) {
    Rectangle shape1 = randomRectangle();
    Rectangle shape2 = randomRectangle();
    return new ShapePair(shape1, shape2, biasContains);
  }

  private void fail(String label, String id, Map<String, Shape> indexedShapes, Map<String, Shape> indexedShapesGS, Shape queryShape) {
    System.err.println("Ig:" + indexedShapesGS.get(id) + " Qg:" + gridSnap(queryShape));
    fail(label + " I#" + id + ":" + indexedShapes.get(id) + " Q:" + queryShape);
  }

//  private Rectangle inset(Rectangle r) {
//    //typically inset by 1 (whole numbers are easy to read)
//    double d = Math.min(1.0, grid.getDistanceForLevel(grid.getMaxLevels()) / 4);
//    return ctx.makeRectangle(r.getMinX() + d, r.getMaxX() - d, r.getMinY() + d, r.getMaxY() - d);
//  }

  protected Shape gridSnap(Shape snapMe) {
    if (snapMe == null)
      return null;
    if (snapMe instanceof ShapePair) {
      ShapePair me = (ShapePair) snapMe;
      return new ShapePair(gridSnap(me.shape1), gridSnap(me.shape2), me.biasContainsThenWithin);
    }
    if (snapMe instanceof Point) {
      snapMe = snapMe.getBoundingBox();
    }
    //The next 4 lines mimic PrefixTreeStrategy.createIndexableFields()
    double distErrPct = ((PrefixTreeStrategy) strategy).getDistErrPct();
    double distErr = SpatialArgs.calcDistanceFromErrPct(snapMe, distErrPct, ctx);
    int detailLevel = grid.getLevelForDistance(distErr);
    CellIterator cells = grid.getTreeCellIterator(snapMe, detailLevel);

    //calc bounding box of cells.
    List<Shape> cellShapes = new ArrayList<>(1024);
    while (cells.hasNext()) {
      Cell cell = cells.next();
      if (!cell.isLeaf())
        continue;
      cellShapes.add(cell.getShape());
    }
    return new ShapeCollection<>(cellShapes, ctx).getBoundingBox();
  }

  /**
   * An aggregate of 2 shapes. Unfortunately we can't simply use a ShapeCollection because:
   * (a) ambiguity between CONTAINS and WITHIN for equal shapes, and
   * (b) adjacent pairs could as a whole contain the input shape.
   * The tests here are sensitive to these matters, although in practice ShapeCollection
   * is fine.
   */
  private class ShapePair extends ShapeCollection<Shape> {

    final Shape shape1, shape2;
    final Shape shape1_2D, shape2_2D;//not geo (bit of a hack)
    final boolean biasContainsThenWithin;

    public ShapePair(Shape shape1, Shape shape2, boolean containsThenWithin) {
      super(Arrays.asList(shape1, shape2), RandomSpatialOpFuzzyPrefixTreeTest.this.ctx);
      this.shape1 = shape1;
      this.shape2 = shape2;
      this.shape1_2D = toNonGeo(shape1);
      this.shape2_2D = toNonGeo(shape2);
      biasContainsThenWithin = containsThenWithin;
    }

    private Shape toNonGeo(Shape shape) {
      if (!ctx.isGeo())
        return shape;//already non-geo
      if (shape instanceof Rectangle) {
        Rectangle rect = (Rectangle) shape;
        if (rect.getCrossesDateLine()) {
          return new ShapePair(
              ctx2D.makeRectangle(rect.getMinX(), 180, rect.getMinY(), rect.getMaxY()),
              ctx2D.makeRectangle(-180, rect.getMaxX(), rect.getMinY(), rect.getMaxY()),
              biasContainsThenWithin);
        } else {
          return ctx2D.makeRectangle(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
        }
      }
      //no need to do others; this addresses the -180/+180 ambiguity corner test problem
      return shape;
    }

    @Override
    public SpatialRelation relate(Shape other) {
      SpatialRelation r = relateApprox(other);
      if (r == DISJOINT)
        return r;
      if (r == CONTAINS)
        return r;
      if (r == WITHIN && !biasContainsThenWithin)
        return r;

      //See if the correct answer is actually Contains, when the indexed shapes are adjacent,
      // creating a larger shape that contains the input shape.
      boolean pairTouches = shape1.relate(shape2).intersects();
      if (!pairTouches)
        return r;
      //test all 4 corners
      // Note: awkwardly, we use a non-geo context for this because in geo, -180 & +180 are the same place, which means
      //  that "other" might wrap the world horizontally and yet all its corners could be in shape1 (or shape2) even
      //  though shape1 is only adjacent to the dateline. I couldn't think of a better way to handle this.
      Rectangle oRect = (Rectangle)other;
      if (cornerContainsNonGeo(oRect.getMinX(), oRect.getMinY())
          && cornerContainsNonGeo(oRect.getMinX(), oRect.getMaxY())
          && cornerContainsNonGeo(oRect.getMaxX(), oRect.getMinY())
          && cornerContainsNonGeo(oRect.getMaxX(), oRect.getMaxY()) )
        return CONTAINS;
      return r;
    }

    private boolean cornerContainsNonGeo(double x, double y) {
      Shape pt = ctx2D.makePoint(x, y);
      return shape1_2D.relate(pt).intersects() || shape2_2D.relate(pt).intersects();
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
        return INTERSECTS;//might actually be 'CONTAINS' if the pair are adjacent but we handle that later
      return DISJOINT;
    }

    @Override
    public String toString() {
      return "ShapePair(" + shape1 + " , " + shape2 + ")";
    }
  }

}
