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
package org.apache.lucene.spatial.vector;

import java.io.IOException;
import java.text.ParseException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.SpatialMatchConcern;
import org.apache.lucene.spatial.StrategyTestCase;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;

@SuppressWarnings("deprecation")
public class TestPointVectorStrategy extends StrategyTestCase {

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.ctx = SpatialContext.GEO;
  }

  @Test
  public void testCircleShapeSupport() {
    this.strategy = PointVectorStrategy.newInstance(ctx, getClass().getSimpleName());
    Circle circle = ctx.makeCircle(ctx.makePoint(0, 0), 10);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, circle);
    Query query = this.strategy.makeQuery(args);

    assertNotNull(query);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidQueryShape() {
    this.strategy = PointVectorStrategy.newInstance(ctx, getClass().getSimpleName());
    Point point = ctx.makePoint(0, 0);
    SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, point);
    this.strategy.makeQuery(args);
  }

  @Test
  public void testCitiesIntersectsBBox() throws IOException {
    // note: does not require docValues
    this.strategy = PointVectorStrategy.newInstance(ctx, getClass().getSimpleName());
    getAddAndVerifyIndexedDocuments(DATA_WORLD_CITIES_POINTS);
    executeQueries(SpatialMatchConcern.FILTER, QTEST_Cities_Intersects_BBox);
  }

  @Test
  public void testFieldOptions() throws IOException, ParseException {
    // It's not stored; test it isn't.
    this.strategy = PointVectorStrategy.newInstance(ctx, getClass().getSimpleName());
    adoc("99", "POINT(-5.0 8.2)");
    commit();
    SearchResults results = executeQuery(new MatchAllDocsQuery(), 1);
    Document document = results.results.get(0).document;
    assertNull("not stored", document.getField(strategy.getFieldName() + PointVectorStrategy.SUFFIX_X));
    assertNull("not stored", document.getField(strategy.getFieldName() + PointVectorStrategy.SUFFIX_Y));
    deleteAll();

    // Now we mark it stored.  We also disable pointvalues...
    FieldType fieldType = new FieldType(PointVectorStrategy.DEFAULT_FIELDTYPE);
    fieldType.setStored(true);
    fieldType.setDimensions(0, 0);//disable point values
    this.strategy = new PointVectorStrategy(ctx, getClass().getSimpleName(), fieldType);
    adoc("99", "POINT(-5.0 8.2)");
    commit();
    results = executeQuery(new MatchAllDocsQuery(), 1);
    document = results.results.get(0).document;
    assertEquals("stored", -5.0, document.getField(strategy.getFieldName() + PointVectorStrategy.SUFFIX_X).numericValue());
    assertEquals("stored", 8.2,  document.getField(strategy.getFieldName() + PointVectorStrategy.SUFFIX_Y).numericValue());

    // Test a query fails without point values
    expectThrows(UnsupportedOperationException.class, () -> {
      SpatialArgs args = new SpatialArgs(SpatialOperation.Intersects, ctx.makeRectangle(-10.0, 10.0, -5.0, 5.0));
      this.strategy.makeQuery(args);
    });
  }
}
