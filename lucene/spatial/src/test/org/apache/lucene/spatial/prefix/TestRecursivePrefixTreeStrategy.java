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

import com.spatial4j.core.context.simple.SimpleSpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.simple.PointImpl;
import com.spatial4j.core.util.GeohashUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.Math.toRadians;

public class TestRecursivePrefixTreeStrategy extends StrategyTestCase {

  private int maxLength;

  //Tests should call this first.
  private void init(int maxLength) {
    this.maxLength = maxLength;
    this.ctx = SimpleSpatialContext.GEO_KM;
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
      executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_IsWithin_BBox);
    }
  }

  @Test
  public void testOneMeterPrecision() {
    init(GeohashPrefixTree.getMaxLevelsPossible());
    GeohashPrefixTree grid = (GeohashPrefixTree) ((RecursivePrefixTreeStrategy) strategy).getGrid();
    //DWS: I know this to be true.  11 is needed for one meter
    assertEquals(11, grid.getLevelForDistance(ctx.getDistCalc().distanceToDegrees(0.001)));
  }

  @Test
  public void testPrecision() throws IOException{
    init(GeohashPrefixTree.getMaxLevelsPossible());

    Point iPt = ctx.makePoint(2.8028712999999925, 48.3708044);//lon, lat
    addDocument(newDoc("iPt", iPt));
    commit();

    Point qPt = ctx.makePoint(2.4632387000000335, 48.6003516);

    final double DIST = 35.75;//35.7499...
    assertEquals(DIST, ctx.getDistCalc().distance(iPt, qPt), 0.001);

    //distPrec will affect the query shape precision. The indexed precision
    // was set to nearly zilch via init(GeohashPrefixTree.getMaxLevelsPossible());
    final double distPrec = 0.025; //the suggested default, by the way
    final double distMult = 1+distPrec;

    assertTrue(35.74*distMult >= DIST);
    checkHits(q(qPt, 35.74, distPrec), 1, null);

    assertTrue(30*distMult < DIST);
    checkHits(q(qPt, 30, distPrec), 0, null);

    assertTrue(33*distMult < DIST);
    checkHits(q(qPt, 33, distPrec), 0, null);

    assertTrue(34*distMult < DIST);
    checkHits(q(qPt, 34, distPrec), 0, null);
  }

  @Test
  public void geohashRecursiveRandom() throws IOException {
    init(12);

    //1. Iterate test with the cluster at some worldly point of interest
    Point[] clusterCenters = new Point[]{new PointImpl(0,0), new PointImpl(0,90),new PointImpl(0,-90)};
    for (Point clusterCenter : clusterCenters) {
      //2. Iterate on size of cluster (a really small one and a large one)
      String hashCenter = GeohashUtils.encodeLatLon(clusterCenter.getY(), clusterCenter.getX(), maxLength);
      //calculate the number of degrees in the smallest grid box size (use for both lat & lon)
      String smallBox = hashCenter.substring(0,hashCenter.length()-1);//chop off leaf precision
      Rectangle clusterDims = GeohashUtils.decodeBoundary(smallBox,ctx);
      double smallDegrees = Math.max(clusterDims.getMaxX()-clusterDims.getMinX(),clusterDims.getMaxY()-clusterDims.getMinY());
      assert smallDegrees < 1;
      double largeDegrees = 20d;//good large size; don't use >=45 for this test code to work
      double[] sideDegrees = {largeDegrees,smallDegrees};
      for (double sideDegree : sideDegrees) {
        //3. Index random points in this cluster box
        deleteAll();
        List<Point> points = new ArrayList<Point>();
        for(int i = 0; i < 20; i++) {
          double x = random().nextDouble()*sideDegree - sideDegree/2 + clusterCenter.getX();
          double y = random().nextDouble()*sideDegree - sideDegree/2 + clusterCenter.getY();
          final Point pt = normPointXY(x, y);
          points.add(pt);
          addDocument(newDoc("" + i, pt));
        }
        commit();

        //3. Use 4 query centers. Each is radially out from each corner of cluster box by twice distance to box edge.
        for(double qcXoff : new double[]{sideDegree,-sideDegree}) {//query-center X offset from cluster center
          for(double qcYoff : new double[]{sideDegree,-sideDegree}) {//query-center Y offset from cluster center
            Point queryCenter = normPointXY(qcXoff + clusterCenter.getX(),
                qcYoff + clusterCenter.getY());
            double[] distRange = calcDistRange(queryCenter,clusterCenter,sideDegree);
            //4.1 query a small box getting nothing
            checkHits(q(queryCenter, distRange[0]*0.99), 0, null);
            //4.2 Query a large box enclosing the cluster, getting everything
            checkHits(q(queryCenter, distRange[1]*1.01), points.size(), null);
            //4.3 Query a medium box getting some (calculate the correct solution and verify)
            double queryDist = distRange[0] + (distRange[1]-distRange[0])/2;//average

            //Find matching points.  Put into int[] of doc ids which is the same thing as the index into points list.
            int[] ids = new int[points.size()];
            int ids_sz = 0;
            for (int i = 0; i < points.size(); i++) {
              Point point = points.get(i);
              if (ctx.getDistCalc().distance(queryCenter, point) <= queryDist)
                ids[ids_sz++] = i;
            }
            ids = Arrays.copyOf(ids, ids_sz);
            //assert ids_sz > 0 (can't because randomness keeps us from being able to)

            checkHits(q(queryCenter, queryDist), ids.length, ids);
          }
        }

      }//for sideDegree

    }//for clusterCenter

  }//randomTest()

  private SpatialArgs q(Point pt, double dist) {
    return q(pt, dist, 0.0);
  }

  private SpatialArgs q(Point pt, double dist, double distPrec) {
    Shape shape = ctx.makeCircle(pt,dist);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects,shape);
    args.setDistPrecision(distPrec);
    return args;
  }

  private void checkHits(SpatialArgs args, int assertNumFound, int[] assertIds) {
    SearchResults got = executeQuery(strategy.makeQuery(args), 100);
    assertEquals("" + args, assertNumFound, got.numFound);
    if (assertIds != null) {
      Set<Integer> gotIds = new HashSet<Integer>();
      for (SearchResult result : got.results) {
        gotIds.add(Integer.valueOf(result.document.get("id")));
      }
      for (int assertId : assertIds) {
        assertTrue("has "+assertId,gotIds.contains(assertId));
      }
    }
  }

  private Document newDoc(String id, Shape shape) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    for (IndexableField f : strategy.createIndexableFields(shape)) {
      doc.add(f);
    }
    if (storeShape)
      doc.add(new StoredField(strategy.getFieldName(), ctx.toString(shape)));
    return doc;
  }

  private double[] calcDistRange(Point startPoint, Point targetCenter, double targetSideDegrees) {
    double min = Double.MAX_VALUE;
    double max = Double.MIN_VALUE;
    for(double xLen : new double[]{targetSideDegrees,-targetSideDegrees}) {
      for(double yLen : new double[]{targetSideDegrees,-targetSideDegrees}) {
        Point p2 = normPointXY(targetCenter.getX() + xLen / 2, targetCenter.getY() + yLen / 2);
        double d = ctx.getDistCalc().distance(startPoint, p2);
        min = Math.min(min,d);
        max = Math.max(max,d);
      }
    }
    return new double[]{min,max};
  }

  /** Normalize x & y (put in lon-lat ranges) & ensure geohash round-trip for given precision. */
  private Point normPointXY(double x, double y) {
    //put x,y as degrees into double[] as radians
    double[] latLon = {y*DistanceUtils.DEG_180_AS_RADS, toRadians(x)};
    DistanceUtils.normLatRAD(latLon);
    DistanceUtils.normLatRAD(latLon);
    double x2 = Math.toDegrees(latLon[1]);
    double y2 = Math.toDegrees(latLon[0]);
    //overwrite latLon, units is now degrees

    return GeohashUtils.decode(GeohashUtils.encodeLatLon(y2, x2, maxLength),ctx);
  }
}
