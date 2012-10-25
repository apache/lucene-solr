package org.apache.lucene.spatial.vector;

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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Circle;
import com.spatial4j.core.shape.Point;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestPointVectorStrategy extends StrategyTestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.ctx = SpatialContext.GEO;
    this.strategy = new PointVectorStrategy(ctx, getClass().getSimpleName());
  }

  @Test
  public void testCircleShapeSupport() {
    Circle circle = ctx.makeCircle(ctx.makePoint(0, 0), 10);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
    Query query = this.strategy.makeQuery(args);

    assertNotNull(query);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidQueryShape() {
    Point point = ctx.makePoint(0, 0);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, point);
    this.strategy.makeQuery(args);
  }

  @Test
  public void testCitiesIntersectsBBox() throws IOException {
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);
    executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_Intersects_BBox);
  }
}
