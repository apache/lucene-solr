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
import java.util.List;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.util.Bits;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import static com.carrotsearch.randomizedtesting.RandomizedTest.atMost;
import static com.carrotsearch.randomizedtesting.RandomizedTest.randomIntBetween;

@SuppressWarnings("deprecation")
public class HeatmapFacetCounterTest extends StrategyTestCase {

  SpatialPrefixTree grid;

  int cellsValidated;
  int cellValidatedNonZero;

  @Before
  public void setUp() throws Exception {
    super.setUp();
    cellsValidated = cellValidatedNonZero = 0;
    ctx = SpatialContext.GEO;
    grid = new QuadPrefixTree(ctx, randomIntBetween(1, 8));
    strategy = new RecursivePrefixTreeStrategy(grid, getTestClass().getSimpleName());
    if (rarely()) {
      ((PrefixTreeStrategy) strategy).setPointsOnly(true);
    }
  }

  @After
  public void after() {
    log.info("Validated " + cellsValidated + " cells, " + cellValidatedNonZero + " non-zero");
  }

  @Test
  public void testStatic() throws IOException {
    //Some specific tests (static, not random).
    adoc("0", ctx.makeRectangle(179.8, -170, -90, -80));//barely crosses equator
    adoc("1", ctx.makePoint(-180, -85));//a pt within the above rect
    adoc("2", ctx.makePoint(172, -85));//a pt to left of rect
    commit();

    validateHeatmapResultLoop(ctx.makeRectangle(+170, +180, -90, -85), 1, 100);
    validateHeatmapResultLoop(ctx.makeRectangle(-180, -160, -89, -50), 1, 100);
    validateHeatmapResultLoop(ctx.makeRectangle(179, 179, -89, -50), 1, 100);//line
    // We could test anything and everything at this point... I prefer we leave that to random testing and then
    // add specific tests if we find a bug.
  }

  @Test
  public void testLucene7291Dateline() throws IOException {
    grid = new QuadPrefixTree(ctx, 2); // only 2, and we wind up with some big leaf cells
    strategy = new RecursivePrefixTreeStrategy(grid, getTestClass().getSimpleName());
    adoc("0", ctx.makeRectangle(-102, -83, 43, 52));
    commit();
    validateHeatmapResultLoop(ctx.makeRectangle(179, -179, 62, 63), 2, 100);// HM crosses dateline
  }

  @Test
  public void testQueryCircle() throws IOException {
    //overwrite setUp; non-geo bounds is more straight-forward; otherwise 88,88 would actually be practically north,
    final SpatialContextFactory spatialContextFactory = new SpatialContextFactory();
    spatialContextFactory.geo = false;
    spatialContextFactory.worldBounds = new RectangleImpl(-90, 90, -90, 90, null);
    ctx = spatialContextFactory.newSpatialContext();
    final int LEVEL = 4;
    grid = new QuadPrefixTree(ctx, LEVEL);
    strategy = new RecursivePrefixTreeStrategy(grid, getTestClass().getSimpleName());
    Circle circle = ctx.makeCircle(0, 0, 89);
    adoc("0", ctx.makePoint(88, 88));//top-right, inside bbox of circle but not the circle
    adoc("1", ctx.makePoint(0, 0));//clearly inside; dead center in fact
    commit();
    final HeatmapFacetCounter.Heatmap heatmap = HeatmapFacetCounter.calcFacets(
        (PrefixTreeStrategy) strategy, indexSearcher.getTopReaderContext(), null,
        circle, LEVEL, 1000);
    //assert that only one point is found, not 2
    boolean foundOne = false;
    for (int count : heatmap.counts) {
      switch (count) {
        case 0: break;
        case 1:
          assertFalse(foundOne);//this is the first
          foundOne = true;
          break;
        default:
          fail("counts should be 0 or 1: " + count);
      }
    }
    assertTrue(foundOne);
  }

  /** Recursively facet & validate at higher resolutions until we've seen enough. We assume there are
   * some non-zero cells. */
  private void validateHeatmapResultLoop(Rectangle inputRange, int facetLevel, int cellCountRecursThreshold)
      throws IOException {
    if (facetLevel > grid.getMaxLevels()) {
      return;
    }
    final int maxCells = 10_000;
    final HeatmapFacetCounter.Heatmap heatmap = HeatmapFacetCounter.calcFacets(
        (PrefixTreeStrategy) strategy, indexSearcher.getTopReaderContext(), null, inputRange, facetLevel, maxCells);
    int preNonZero = cellValidatedNonZero;
    validateHeatmapResult(inputRange, facetLevel, heatmap);
    assert cellValidatedNonZero - preNonZero > 0;//we validated more non-zero cells
    if (heatmap.counts.length < cellCountRecursThreshold) {
      validateHeatmapResultLoop(inputRange, facetLevel + 1, cellCountRecursThreshold);
    }
  }

  @Test
  @Repeat(iterations = 20)
  public void testRandom() throws IOException {
    // Tests using random index shapes & query shapes. This has found all sorts of edge case bugs (e.g. dateline,
    // cell border, overflow(?)).

    final int numIndexedShapes = 1 + atMost(9);
    List<Shape> indexedShapes = new ArrayList<>(numIndexedShapes);
    for (int i = 0; i < numIndexedShapes; i++) {
      indexedShapes.add(randomIndexedShape());
    }

    //Main index loop:
    for (int i = 0; i < indexedShapes.size(); i++) {
      Shape shape = indexedShapes.get(i);
      adoc("" + i, shape);

      if (random().nextInt(10) == 0)
        commit();//intermediate commit, produces extra segments
    }
    //delete some documents randomly
    for (int id = 0; id < indexedShapes.size(); id++) {
      if (random().nextInt(10) == 0) {
        deleteDoc("" + id);
        indexedShapes.set(id, null);
      }
    }

    commit();

    // once without dateline wrap
    final Rectangle rect = randomRectangle();
    queryHeatmapRecursive(usually() ? ctx.getWorldBounds() : rect, 1);
    // and once with dateline wrap
    if (rect.getWidth() > 0) {
      double shift = random().nextDouble() % rect.getWidth();
      queryHeatmapRecursive(ctx.makeRectangle(
              DistanceUtils.normLonDEG(rect.getMinX() - shift),
              DistanceUtils.normLonDEG(rect.getMaxX() - shift),
              rect.getMinY(), rect.getMaxY()),
          1);
    }
  }

  /** Build heatmap, validate results, then descend recursively to another facet level. */
  private boolean queryHeatmapRecursive(Rectangle inputRange, int facetLevel) throws IOException {
    if (!inputRange.hasArea()) {
      // Don't test line inputs. It's not that we don't support it but it is more challenging to test if per-chance it
      // coincides with a grid line due due to edge overlap issue for some grid implementations (geo & quad).
      return false;
    }
    Bits filter = null; //FYI testing filtering of underlying PrefixTreeFacetCounter is done in another test
    //Calculate facets
    final int maxCells = 10_000;
    final HeatmapFacetCounter.Heatmap heatmap = HeatmapFacetCounter.calcFacets(
        (PrefixTreeStrategy) strategy, indexSearcher.getTopReaderContext(), filter, inputRange, facetLevel, maxCells);

    validateHeatmapResult(inputRange, facetLevel, heatmap);

    boolean foundNonZeroCount = false;
    for (int count : heatmap.counts) {
      if (count > 0) {
        foundNonZeroCount = true;
        break;
      }
    }

    //Test again recursively to higher facetLevel (more detailed cells)
    if (foundNonZeroCount && cellsValidated <= 500 && facetLevel != grid.getMaxLevels() && inputRange.hasArea()) {
      for (int i = 0; i < 5; i++) {//try multiple times until we find non-zero counts
        if (queryHeatmapRecursive(randomRectangle(inputRange), facetLevel + 1)) {
          break;//we found data here so we needn't try again
        }
      }
    }
    return foundNonZeroCount;
  }

  private void validateHeatmapResult(Rectangle inputRange, int facetLevel, HeatmapFacetCounter.Heatmap heatmap)
      throws IOException {
    final Rectangle heatRect = heatmap.region;
    assertTrue(heatRect.relate(inputRange) == SpatialRelation.CONTAINS || heatRect.equals(inputRange));
    final double cellWidth = heatRect.getWidth() / heatmap.columns;
    final double cellHeight = heatRect.getHeight() / heatmap.rows;
    for (int c = 0; c < heatmap.columns; c++) {
      for (int r = 0; r < heatmap.rows; r++) {
        final int facetCount = heatmap.getCount(c, r);
        double x = DistanceUtils.normLonDEG(heatRect.getMinX() + c * cellWidth + cellWidth / 2);
        double y = DistanceUtils.normLatDEG(heatRect.getMinY() + r * cellHeight + cellHeight / 2);
        Point pt =  ctx.makePoint(x, y);
        assertEquals(countMatchingDocsAtLevel(pt, facetLevel), facetCount);
      }
    }
  }

  private int countMatchingDocsAtLevel(Point pt, int facetLevel) throws IOException {
    // we use IntersectsPrefixTreeFilter directly so that we can specify the level to go to exactly.
    RecursivePrefixTreeStrategy strategy = (RecursivePrefixTreeStrategy) this.strategy;
    Query filter = new IntersectsPrefixTreeQuery(
        pt, strategy.getFieldName(), grid, facetLevel, grid.getMaxLevels());
    final TotalHitCountCollector collector = new TotalHitCountCollector();
    indexSearcher.search(filter, collector);
    cellsValidated++;
    if (collector.getTotalHits() > 0) {
      cellValidatedNonZero++;
    }
    return collector.getTotalHits();
  }

  private Shape randomIndexedShape() {
    if (((PrefixTreeStrategy) strategy).isPointsOnly() || random().nextBoolean()) {
      return randomPoint();
    } else {
      return randomRectangle();
    }
  }
}