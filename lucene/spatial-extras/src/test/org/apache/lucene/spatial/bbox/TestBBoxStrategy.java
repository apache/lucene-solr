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
package org.apache.lucene.spatial.bbox;

import java.io.IOException;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.prefix.RandomSpatialOpStrategyTestCase;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.util.ShapeAreaValueSource;
import org.junit.Ignore;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

public class TestBBoxStrategy extends RandomSpatialOpStrategyTestCase {

  @Override
  protected Shape randomIndexedShape() {
    Rectangle world = ctx.getWorldBounds();
    if (random().nextInt(10) == 0) // increased chance of getting one of these
      return world;

    int worldWidth = (int) Math.round(world.getWidth());
    int deltaLeft = nextIntInclusive(worldWidth);
    int deltaRight = nextIntInclusive(worldWidth - deltaLeft);
    int worldHeight = (int) Math.round(world.getHeight());
    int deltaTop = nextIntInclusive(worldHeight);
    int deltaBottom = nextIntInclusive(worldHeight - deltaTop);
    if (ctx.isGeo() && (deltaLeft != 0 || deltaRight != 0)) {
      //if geo & doesn't world-wrap, we shift randomly to potentially cross dateline
      int shift = nextIntInclusive(360);
      return ctx.makeRectangle(
          DistanceUtils.normLonDEG(world.getMinX() + deltaLeft + shift),
          DistanceUtils.normLonDEG(world.getMaxX() - deltaRight + shift),
          world.getMinY() + deltaBottom, world.getMaxY() - deltaTop);
    } else {
      return ctx.makeRectangle(
          world.getMinX() + deltaLeft, world.getMaxX() - deltaRight,
          world.getMinY() + deltaBottom, world.getMaxY() - deltaTop);
    }
  }

  /** next int, inclusive, rounds to multiple of 10 if given evenly divisible. */
  private int nextIntInclusive(int toInc) {
    final int DIVIS = 10;
    if (toInc % DIVIS == 0) {
      return random().nextInt(toInc/DIVIS + 1) * DIVIS;
    } else {
      return random().nextInt(toInc + 1);
    }
  }

  @Override
  protected Shape randomQueryShape() {
    return randomIndexedShape();
  }

  @Test
  public void testOperations() throws IOException {
    //setup
    if (random().nextInt(4) > 0) {//75% of the time choose geo (more interesting to test)
      this.ctx = SpatialContext.GEO;
    } else {
      SpatialContextFactory factory = new SpatialContextFactory();
      factory.geo = false;
      factory.worldBounds = new RectangleImpl(-300, 300, -100, 100, null);
      this.ctx = factory.newSpatialContext();
    }
    this.strategy = BBoxStrategy.newInstance(ctx, "bbox");
    //test we can disable docValues for predicate tests
    if (random().nextBoolean()) {
      FieldType fieldType = new FieldType(((BBoxStrategy)strategy).getFieldType());
      fieldType.setDocValuesType(DocValuesType.NONE);
      strategy = new BBoxStrategy(ctx, strategy.getFieldName(), fieldType);
    }
    for (SpatialOperation operation : SpatialOperation.values()) {
      if (operation == SpatialOperation.Overlaps)
        continue;//unsupported
      testOperationRandomShapes(operation);

      deleteAll();
      commit();
    }
  }

  @Test
  public void testIntersectsBugDatelineEdge() throws IOException {
    setupGeo();
    testOperation(
        ctx.makeRectangle(160, 180, -10, 10),
        SpatialOperation.Intersects,
        ctx.makeRectangle(-180, -160, -10, 10), true);
  }

  @Test
  public void testIntersectsWorldDatelineEdge() throws IOException {
    setupGeo();
    testOperation(
        ctx.makeRectangle(-180, 180, -10, 10),
        SpatialOperation.Intersects,
        ctx.makeRectangle(180, 180, -10, 10), true);
  }

  @Test
  public void testWithinBugDatelineEdge() throws IOException {
    setupGeo();
    testOperation(
        ctx.makeRectangle(180, 180, -10, 10),
        SpatialOperation.IsWithin,
        ctx.makeRectangle(-180, -100, -10, 10), true);
  }

  @Test
  public void testContainsBugDatelineEdge() throws IOException {
    setupGeo();
    testOperation(
        ctx.makeRectangle(-180, -150, -10, 10),
        SpatialOperation.Contains,
        ctx.makeRectangle(180, 180, -10, 10), true);
  }

  @Test
  public void testWorldContainsXDL() throws IOException {
    setupGeo();
    testOperation(
        ctx.makeRectangle(-180, 180, -10, 10),
        SpatialOperation.Contains,
        ctx.makeRectangle(170, -170, -10, 10), true);
  }

  /** See https://github.com/spatial4j/spatial4j/issues/85 */
  @Test
  public void testAlongDatelineOppositeSign() throws IOException {
    // Due to Spatial4j bug #85, we can't simply do:
    //    testOperation(indexedShape,
    //        SpatialOperation.IsWithin,
    //        queryShape, true);

    //both on dateline but expressed using opposite signs
    setupGeo();
    final Rectangle indexedShape = ctx.makeRectangle(180, 180, -10, 10);
    final Rectangle queryShape = ctx.makeRectangle(-180, -180, -20, 20);
    final SpatialOperation operation = SpatialOperation.IsWithin;
    final boolean match = true;//yes it is within

    //the rest is super.testOperation without leading assert:

    adoc("0", indexedShape);
    commit();
    Query query = strategy.makeQuery(new SpatialArgs(operation, queryShape));
    SearchResults got = executeQuery(query, 1);
    assert got.numFound <= 1 : "unclean test env";
    if ((got.numFound == 1) != match)
      fail(operation+" I:" + indexedShape + " Q:" + queryShape);
    deleteAll();//clean up after ourselves
  }

  private void setupGeo() {
    this.ctx = SpatialContext.GEO;
    this.strategy = BBoxStrategy.newInstance(ctx, "bbox");
  }

  // OLD STATIC TESTS (worthless?)

  @Test @Ignore("Overlaps not supported")
  public void testBasicOperaions() throws IOException {
    setupGeo();
    getAddAndVerifyIndexedDocuments(DATA_SIMPLE_BBOX);

    executeQueries(SpatialMatchConcern.EXACT, QTEST_Simple_Queries_BBox);
  }

  @Test
  public void testStatesBBox() throws IOException {
    setupGeo();
    getAddAndVerifyIndexedDocuments(DATA_STATES_BBOX);

    executeQueries(SpatialMatchConcern.FILTER, QTEST_States_IsWithin_BBox);
    executeQueries(SpatialMatchConcern.FILTER, QTEST_States_Intersects_BBox);
  }

  @Test
  public void testCitiesIntersectsBBox() throws IOException {
    setupGeo();
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);

    executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_Intersects_BBox);
  }

  /* Convert DATA_WORLD_CITIES_POINTS to bbox */
  @Override
  protected Shape convertShapeFromGetDocuments(Shape shape) {
    return shape.getBoundingBox();
  }

  private BBoxStrategy setupNeedsDocValuesOnly() throws IOException {
    this.ctx = SpatialContext.GEO;
    FieldType fieldType;
    // random  legacy or not legacy
    String FIELD_PREFIX = "bbox";
    fieldType = new FieldType(BBoxStrategy.DEFAULT_FIELDTYPE);
    if (random().nextBoolean()) {
      fieldType.setDimensions(0, 0);
    }

    strategy = new BBoxStrategy(ctx, FIELD_PREFIX, fieldType);
    return (BBoxStrategy)strategy;
  }

  public void testOverlapRatio() throws IOException {
    setupNeedsDocValuesOnly();

    //Simply assert null shape results in 0
    adoc("999", (Shape) null);
    commit();
    BBoxStrategy bboxStrategy = (BBoxStrategy) strategy;
    checkValueSource(bboxStrategy.makeOverlapRatioValueSource(randomRectangle(), 0.0), new float[]{0f}, 0f);

    //we test raw BBoxOverlapRatioValueSource without actual indexing
    for (int SHIFT = 0; SHIFT < 360; SHIFT += 10) {
      Rectangle queryBox = shiftedRect(0, 40, -20, 20, SHIFT);//40x40, 1600 area

      final boolean MSL = random().nextBoolean();
      final double minSideLength = MSL ? 0.1 : 0.0;
      BBoxOverlapRatioValueSource sim = new BBoxOverlapRatioValueSource(null, true, queryBox, 0.5, minSideLength);
      int nudge = SHIFT == 0 ? 0 : random().nextInt(3) * 10 - 10;//-10, 0, or 10.  Keep 0 on first round.

      final double EPS = 0.0000001;

      assertEquals("within", (200d/1600d * 0.5) + (0.5), sim.score(shiftedRect(10, 30, 0, 10, SHIFT + nudge), null), EPS);

      assertEquals("in25%", 0.25, sim.score(shiftedRect(30, 70, -20, 20, SHIFT), null), EPS);

      assertEquals("wrap", 0.2794117, sim.score(shiftedRect(30, 10, -20, 20, SHIFT + nudge), null), EPS);

      assertEquals("no intersection H", 0.0, sim.score(shiftedRect(-10, -10, -20, 20, SHIFT), null), EPS);
      assertEquals("no intersection V", 0.0, sim.score(shiftedRect(0, 20, -30, -30, SHIFT), null), EPS);

      assertEquals("point", 0.5 + (MSL?(0.1*0.1/1600.0/2.0):0), sim.score(shiftedRect(0, 0, 0, 0, SHIFT), null), EPS);

      assertEquals("line 25% intersection", 0.25/2 + (MSL?(10.0*0.1/1600.0/2.0):0.0), sim.score(shiftedRect(-30, 10, 0, 0, SHIFT), null), EPS);

      //test with point query
      sim = new BBoxOverlapRatioValueSource(null, true, shiftedRect(0, 0, 0, 0, SHIFT), 0.5, minSideLength);
      assertEquals("same", 1.0, sim.score(shiftedRect(0, 0, 0, 0, SHIFT), null), EPS);
      assertEquals("contains", 0.5 + (MSL?(0.1*0.1/(30*10)/2.0):0.0), sim.score(shiftedRect(0, 30, 0, 10, SHIFT), null), EPS);

      //test with line query (vertical this time)
      sim = new BBoxOverlapRatioValueSource(null, true, shiftedRect(0, 0, 20, 40, SHIFT), 0.5, minSideLength);
      assertEquals("line 50%", 0.5, sim.score(shiftedRect(0, 0, 10, 30, SHIFT), null), EPS);
      assertEquals("point", 0.5 + (MSL?(0.1*0.1/(20*0.1)/2.0):0.0), sim.score(shiftedRect(0, 0, 30, 30, SHIFT), null), EPS);
    }

  }

  private Rectangle shiftedRect(double minX, double maxX, double minY, double maxY, int xShift) {
    return ctx.makeRectangle(
        DistanceUtils.normLonDEG(minX + xShift),
        DistanceUtils.normLonDEG(maxX + xShift),
        minY, maxY);
  }

  public void testAreaValueSource() throws IOException {
    BBoxStrategy bboxStrategy = setupNeedsDocValuesOnly();

    adoc("100", ctx.makeRectangle(0, 20, 40, 80));
    adoc("999", (Shape) null);
    commit();
    checkValueSource(new ShapeAreaValueSource(bboxStrategy.makeShapeValueSource(), ctx, false, 1.0),
        new float[]{800f, 0f}, 0f);
    checkValueSource(new ShapeAreaValueSource(bboxStrategy.makeShapeValueSource(), ctx, true, 1.0),//geo
        new float[]{391.93f, 0f}, 0.01f);
    checkValueSource(new ShapeAreaValueSource(bboxStrategy.makeShapeValueSource(), ctx, true, 2.0),
        new float[]{783.86f, 0f}, 0.01f); // testing with a different multiplier
  }

}
