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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestRecursivePrefixTreeStrategy extends StrategyTestCase {

  private int maxLength;

  //Tests should call this first.
  private void init(int maxLength) {
    this.maxLength = maxLength;
    this.ctx = SpatialContext.GEO;
    GeohashPrefixTree grid = new GeohashPrefixTree(ctx, maxLength);
    this.strategy = new RecursivePrefixTreeStrategy(grid, getClass().getSimpleName());
  }

  @Test
  public void testFilterWithVariableScanLevel() throws IOException {
    init(GeohashPrefixTree.getMaxLevelsPossible());
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);

    //execute queries for each prefix grid scan level
    for(int i = 0; i <= maxLength; i++) {
      ((RecursivePrefixTreeStrategy)strategy).setPrefixGridScanLevel(i);
      executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_Intersects_BBox);
    }
  }

  @Test
  public void testOneMeterPrecision() {
    init(GeohashPrefixTree.getMaxLevelsPossible());
    GeohashPrefixTree grid = (GeohashPrefixTree) ((RecursivePrefixTreeStrategy) strategy).getGrid();
    //DWS: I know this to be true.  11 is needed for one meter
    double degrees = DistanceUtils.dist2Degrees(0.001, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    assertEquals(11, grid.getLevelForDistance(degrees));
  }

  @Test
  public void testPrecision() throws IOException{
    init(GeohashPrefixTree.getMaxLevelsPossible());

    Point iPt = ctx.makePoint(2.8028712999999925, 48.3708044);//lon, lat
    addDocument(newDoc("iPt", iPt));
    commit();

    Point qPt = ctx.makePoint(2.4632387000000335, 48.6003516);

    final double KM2DEG = DistanceUtils.dist2Degrees(1, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    final double DEG2KM = 1 / KM2DEG;

    final double DIST = 35.75;//35.7499...
    assertEquals(DIST, ctx.getDistCalc().distance(iPt, qPt) * DEG2KM, 0.001);

    //distErrPct will affect the query shape precision. The indexed precision
    // was set to nearly zilch via init(GeohashPrefixTree.getMaxLevelsPossible());
    final double distErrPct = 0.025; //the suggested default, by the way
    final double distMult = 1+distErrPct;

    assertTrue(35.74*distMult >= DIST);
    checkHits(q(qPt, 35.74 * KM2DEG, distErrPct), 1, null);

    assertTrue(30*distMult < DIST);
    checkHits(q(qPt, 30 * KM2DEG, distErrPct), 0, null);

    assertTrue(33*distMult < DIST);
    checkHits(q(qPt, 33 * KM2DEG, distErrPct), 0, null);

    assertTrue(34*distMult < DIST);
    checkHits(q(qPt, 34 * KM2DEG, distErrPct), 0, null);
  }

  private SpatialArgs q(Point pt, double distDEG, double distErrPct) {
    Shape shape = ctx.makeCircle(pt, distDEG);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,shape);
    args.setDistErrPct(distErrPct);
    return args;
  }

  private void checkHits(SpatialArgs args, int assertNumFound, int[] assertIds) {
    SearchResults got = executeQuery(strategy.makeQuery(args), 100);
    assertEquals("" + args, assertNumFound, got.numFound);
    if (assertIds != null) {
      Set<Integer> gotIds = new HashSet<>();
      for (SearchResult result : got.results) {
        gotIds.add(Integer.valueOf(result.document.get("id")));
      }
      for (int assertId : assertIds) {
        assertTrue("has "+assertId,gotIds.contains(assertId));
      }
    }
  }

}
