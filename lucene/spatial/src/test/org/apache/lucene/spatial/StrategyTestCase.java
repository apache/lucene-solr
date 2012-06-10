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


import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.io.sample.SampleData;
import com.spatial4j.core.io.sample.SampleDataReader;
import com.spatial4j.core.query.SpatialArgsParser;
import com.spatial4j.core.shape.Shape;
import org.junit.Assert;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.logging.Logger;

public abstract class StrategyTestCase<T extends SpatialFieldInfo> extends SpatialTestCase {

  public static final String DATA_STATES_POLY = "states-poly.txt";
  public static final String DATA_STATES_BBOX = "states-bbox.txt";
  public static final String DATA_COUNTRIES_POLY = "countries-poly.txt";
  public static final String DATA_COUNTRIES_BBOX = "countries-bbox.txt";
  public static final String DATA_WORLD_CITIES_POINTS = "world-cities-points.txt";

  public static final String QTEST_States_IsWithin_BBox   = "states-IsWithin-BBox.txt";
  public static final String QTEST_States_Intersects_BBox = "states-Intersects-BBox.txt";

  public static final String QTEST_Cities_IsWithin_BBox = "cities-IsWithin-BBox.txt";

  private Logger log = Logger.getLogger(getClass().getName());

  protected final SpatialArgsParser argsParser = new SpatialArgsParser();

  protected SpatialStrategy<T> strategy;
  protected SpatialContext ctx;
  protected T fieldInfo;
  protected boolean storeShape = true;

  protected void executeQueries(SpatialMatchConcern concern, String... testQueryFile) throws IOException {
    log.info("testing queried for strategy "+strategy);
    for( String path : testQueryFile ) {
      Iterator<SpatialTestQuery> testQueryIterator = getTestQueries(path, ctx);
      runTestQueries(testQueryIterator, concern);
    }
  }

  protected void getAddAndVerifyIndexedDocuments(String testDataFile) throws IOException {
    List<Document> testDocuments = getDocuments(testDataFile);
    addDocumentsAndCommit(testDocuments);
    verifyDocumentsIndexed(testDocuments.size());
  }

  protected List<Document> getDocuments(String testDataFile) throws IOException {
    Iterator<SampleData> sampleData = getSampleData(testDataFile);
    List<Document> documents = new ArrayList<Document>();
    while (sampleData.hasNext()) {
      SampleData data = sampleData.next();
      Document document = new Document();
      document.add(new StringField("id", data.id, Field.Store.YES));
      document.add(new StringField("name", data.name, Field.Store.YES));
      Shape shape = ctx.readShape(data.shape);
      for (IndexableField f : strategy.createFields(fieldInfo, shape, true, storeShape)) {
        if( f != null ) { // null if incompatibleGeometry && ignore
          document.add(f);
        }
      }
      documents.add(document);
    }
    return documents;
  }

  protected Iterator<SampleData> getSampleData(String testDataFile) throws IOException {
    return new SampleDataReader(
        getClass().getClassLoader().getResourceAsStream("data/"+testDataFile) );
  }

  protected Iterator<SpatialTestQuery> getTestQueries(String testQueryFile, SpatialContext ctx) throws IOException {
    InputStream in = getClass().getClassLoader().getResourceAsStream(testQueryFile);
    return SpatialTestQuery.getTestQueries(
        argsParser, ctx, testQueryFile, in );
  }

  public void runTestQueries(
      Iterator<SpatialTestQuery> queries,
      SpatialMatchConcern concern) {
    while (queries.hasNext()) {
      SpatialTestQuery q = queries.next();

      String msg = q.line; //"Query: " + q.args.toString(ctx);
      SearchResults got = executeQuery(strategy.makeQuery(q.args, fieldInfo), 100);
      if (concern.orderIsImportant) {
        Iterator<String> ids = q.ids.iterator();
        for (SearchResult r : got.results) {
          String id = r.document.get("id");
          Assert.assertEquals( "out of order: " + msg, ids.next(), id);
        }
        if (ids.hasNext()) {
          Assert.fail(msg + " :: expect more results then we got: " + ids.next());
        }
      } else {
        // We are looking at how the results overlap
        if( concern.resultsAreSuperset ) {
          Set<String> found = new HashSet<String>();
          for (SearchResult r : got.results) {
            found.add(r.document.get("id"));
          }
          for( String s : q.ids ) {
            if( !found.contains( s ) ) {
              Assert.fail( "Results are mising id: "+s + " :: " + found );
            }
          }
        }
        else {
          List<String> found = new ArrayList<String>();
          for (SearchResult r : got.results) {
            found.add(r.document.get("id"));
          }

          // sort both so that the order is not important
          Collections.sort(q.ids);
          Collections.sort(found);
          Assert.assertEquals(msg, q.ids.toString(), found.toString());
        }
      }
    }
  }
}
