package org.apache.lucene.spatial;

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

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.TermQueryPrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.QuadPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.apache.lucene.spatial.vector.PointVectorStrategy;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistanceStrategyTest extends StrategyTestCase {

  @ParametersFactory
  public static Iterable<Object[]> parameters() {
    List<Object[]> ctorArgs = new ArrayList<Object[]>();

    SpatialContext ctx = SpatialContext.GEO;
    SpatialPrefixTree grid;
    SpatialStrategy strategy;

    grid = new QuadPrefixTree(ctx,25);
    strategy = new RecursivePrefixTreeStrategy(grid, "recursive_quad");
    ctorArgs.add(new Object[]{new Param(strategy)});

    grid = new GeohashPrefixTree(ctx,12);
    strategy = new TermQueryPrefixTreeStrategy(grid, "termquery_geohash");
    ctorArgs.add(new Object[]{new Param(strategy)});

    strategy = new PointVectorStrategy(ctx, "pointvector");
    ctorArgs.add(new Object[]{new Param(strategy)});

    return ctorArgs;
  }

  // this is a hack for clover!
  static class Param {
    SpatialStrategy strategy;

    Param(SpatialStrategy strategy) {
      this.strategy = strategy;
    }

    @Override
    public String toString() {
      return strategy.getFieldName();
    }
  }

//  private String fieldName;

  public DistanceStrategyTest(@Name("strategy") Param param) {
    SpatialStrategy strategy = param.strategy;
    this.ctx = strategy.getSpatialContext();
    this.strategy = strategy;
  }

  @Test
  public void testDistanceOrder() throws IOException {
    adoc("100", ctx.makePoint(2,1));
    adoc("101", ctx.makePoint(-1,4));
    adoc("103", (Shape)null);//test score for nothing
    commit();
    //FYI distances are in docid order
    checkDistValueSource("3,4", 2.8274937f, 5.0898066f, 180f);
    checkDistValueSource("4,0", 3.6043684f, 0.9975641f, 180f);
  }

  @Test
  public void testRecipScore() throws IOException {
    Point p100 = ctx.makePoint(2, 1);
    adoc("100", p100);
    Point p101 = ctx.makePoint(-1, 4);
    adoc("101", p101);
    adoc("103", (Shape)null);//test score for nothing
    commit();

    double dist = ctx.getDistCalc().distance(p100, p101);
    Shape queryShape = ctx.makeCircle(2.01, 0.99, dist);
    checkValueSource(strategy.makeRecipDistanceValueSource(queryShape),
        new float[]{1.00f, 0.10f, 0f}, 0.09f);
  }

  // @Override
  // protected Document newDoc(String id, Shape shape) {
  //   //called by adoc().  Make compatible with BBoxStrategy.
  //   if (shape != null && strategy instanceof BBoxStrategy)
  //     shape = ctx.makeRectangle(shape.getCenter(), shape.getCenter());
  //   return super.newDoc(id, shape);
  // }

  void checkDistValueSource(String ptStr, float... distances) throws IOException {
    Point pt = (Point) ctx.readShape(ptStr);
    checkValueSource(strategy.makeDistanceValueSource(pt), distances, 1.0e-4f);
  }
}
