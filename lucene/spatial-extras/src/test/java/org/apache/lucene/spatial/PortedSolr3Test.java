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
package org.apache.lucene.spatial;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.vector.PointVectorStrategy;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

/**
 * Based off of Solr 3's SpatialFilterTest.
 */
public class PortedSolr3Test extends StrategyTestCase {

  @ParametersFactory(argumentFormatting = "strategy=%s")
  public static Iterable<Object[]> parameters() {
    List<Object[]> ctorArgs = new ArrayList<>();

    SpatialContext ctx = SpatialContext.GEO;
    SpatialPrefixTree grid;
    SpatialStrategy strategy;

    grid = new GeohashPrefixTree(ctx,12);
    strategy = new RecursivePrefixTreeStrategy(grid, "recursive_geohash");
    ctorArgs.add(new Object[]{strategy.getFieldName(), strategy});

    grid = new QuadPrefixTree(ctx,25);
    strategy = new RecursivePrefixTreeStrategy(grid, "recursive_quad");
    ctorArgs.add(new Object[]{strategy.getFieldName(), strategy});

    grid = new GeohashPrefixTree(ctx,12);
    strategy = new TermQueryPrefixTreeStrategy(grid, "termquery_geohash");
    ctorArgs.add(new Object[]{strategy.getFieldName(), strategy});

    strategy = PointVectorStrategy.newInstance(ctx, "pointvector");
    ctorArgs.add(new Object[]{strategy.getFieldName(), strategy});

    strategy = PointVectorStrategy.newInstance(ctx, "pointvector_legacy");
    ctorArgs.add(new Object[]{strategy.getFieldName(), strategy});

    return ctorArgs;
  }

  public PortedSolr3Test(String suiteName, SpatialStrategy strategy) {
    this.ctx = strategy.getSpatialContext();
    this.strategy = strategy;
  }

  private void setupDocs() throws Exception {
    super.deleteAll();
    adoc("1", ctx.makePoint(-79.9289094, 32.7693246));
    adoc("2", ctx.makePoint(-80.9289094, 33.7693246));
    adoc("3", ctx.makePoint(50.9289094, -32.7693246));
    adoc("4", ctx.makePoint(60.9289094, -50.7693246));
    adoc("5", ctx.makePoint(0, 0));
    adoc("6", ctx.makePoint(0.1, 0.1));
    adoc("7", ctx.makePoint(-0.1, -0.1));
    adoc("8", ctx.makePoint(179.9, 0));
    adoc("9", ctx.makePoint(-179.9, 0));
    adoc("10", ctx.makePoint(50, 89.9));
    adoc("11", ctx.makePoint(-130, 89.9));
    adoc("12", ctx.makePoint(50, -89.9));
    adoc("13", ctx.makePoint(-130, -89.9));
    commit();
  }


  @Test
  public void testIntersections() throws Exception {
    setupDocs();
    //Try some edge cases
      //NOTE: 2nd arg is distance in kilometers
    checkHitsCircle(ctx.makePoint(1, 1), 175, 3, 5, 6, 7);
    checkHitsCircle(ctx.makePoint(179.8, 0), 200, 2, 8, 9);
    checkHitsCircle(ctx.makePoint(50, 89.8), 200, 2, 10, 11);//this goes over the north pole
    checkHitsCircle(ctx.makePoint(50, -89.8), 200, 2, 12, 13);//this goes over the south pole
    //try some normal cases
    checkHitsCircle(ctx.makePoint(-80.0, 33.0), 300, 2);
    //large distance
    checkHitsCircle(ctx.makePoint(1, 1), 5000, 3, 5, 6, 7);
    //Because we are generating a box based on the west/east longitudes and the south/north latitudes, which then
    //translates to a range query, which is slightly more inclusive.  Thus, even though 0.0 is 15.725 kms away,
    //it will be included, b/c of the box calculation.
    checkHitsBBox(ctx.makePoint(0.1, 0.1), 15, 2, 5, 6);
    //try some more
    deleteAll();
    adoc("14", ctx.makePoint(5, 0));
    adoc("15", ctx.makePoint(15, 0));
    //3000KM from 0,0, see http://www.movable-type.co.uk/scripts/latlong.html
    adoc("16", ctx.makePoint(19.79750, 18.71111));
    adoc("17", ctx.makePoint(-95.436643, 44.043900));
    commit();

    checkHitsCircle(ctx.makePoint(0, 0), 1000, 1, 14);
    checkHitsCircle(ctx.makePoint(0, 0), 2000, 2, 14, 15);
    checkHitsBBox(ctx.makePoint(0, 0), 3000, 3, 14, 15, 16);
    checkHitsCircle(ctx.makePoint(0, 0), 3001, 3, 14, 15, 16);
    checkHitsCircle(ctx.makePoint(0, 0), 3000.1, 3, 14, 15, 16);

    //really fine grained distance and reflects some of the vagaries of how we are calculating the box
    checkHitsCircle(ctx.makePoint(-96.789603, 43.517030), 109, 0);

    // falls outside of the real distance, but inside the bounding box
    checkHitsCircle(ctx.makePoint(-96.789603, 43.517030), 110, 0);
    checkHitsBBox(ctx.makePoint(-96.789603, 43.517030), 110, 1, 17);
  }

  //---- these are similar to Solr test methods

  private void checkHitsCircle(Point pt, double distKM, int assertNumFound, int... assertIds) {
    _checkHits(false, pt, distKM, assertNumFound, assertIds);
  }
  private void checkHitsBBox(Point pt, double distKM, int assertNumFound, int... assertIds) {
    _checkHits(true, pt, distKM, assertNumFound, assertIds);
  }

  private void _checkHits(boolean bbox, Point pt, double distKM, int assertNumFound, int... assertIds) {
    SpatialOperation op = SpatialOperation.Intersects;
    double distDEG = DistanceUtils.dist2Degrees(distKM, DistanceUtils.EARTH_MEAN_RADIUS_KM);
    Shape shape = ctx.makeCircle(pt, distDEG);
    if (bbox)
      shape = shape.getBoundingBox();

    SpatialArgs args = new SpatialArgs(op,shape);
    //args.setDistPrecision(0.025);
    Query query = strategy.makeQuery(args);
    SearchResults results = executeQuery(query, 100);
    assertEquals(""+shape,assertNumFound,results.numFound);
    if (assertIds != null) {
      Set<Integer> resultIds = new HashSet<>();
      for (SearchResult result : results.results) {
        resultIds.add(Integer.valueOf(result.document.get("id")));
      }
      for (int assertId : assertIds) {
        assertTrue("has " + assertId, resultIds.contains(assertId));
      }
    }
  }

}
