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
package org.apache.lucene.spatial.serialized;

import java.io.IOException;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;

public class TestSerializedStrategy extends StrategyTestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.ctx = SpatialContext.GEO;
    this.strategy = new SerializedDVStrategy(ctx, "serialized");
  }

  @Test
  public void testBasicOperaions() throws IOException {
    getAddAndVerifyIndexedDocuments(DATA_SIMPLE_BBOX);

    executeQueries(SpatialMatchConcern.EXACT, QTEST_Simple_Queries_BBox);
  }

  @Test
  public void testStatesBBox() throws IOException {
    getAddAndVerifyIndexedDocuments(DATA_STATES_BBOX);

    executeQueries(SpatialMatchConcern.FILTER, QTEST_States_IsWithin_BBox);
    executeQueries(SpatialMatchConcern.FILTER, QTEST_States_Intersects_BBox);
  }

  @Test
  public void testCitiesIntersectsBBox() throws IOException {
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);

    executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_Intersects_BBox);
  }

  // sorting is tested in DistanceStrategyTest

}
