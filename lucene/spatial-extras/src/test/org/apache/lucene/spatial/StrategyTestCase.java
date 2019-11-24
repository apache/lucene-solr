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


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialArgsParser;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

public abstract class StrategyTestCase extends SpatialTestCase {

  public static final String DATA_SIMPLE_BBOX = "simple-bbox.txt";
  public static final String DATA_STATES_POLY = "states-poly.txt";
  public static final String DATA_STATES_BBOX = "states-bbox.txt";
  public static final String DATA_COUNTRIES_POLY = "countries-poly.txt";
  public static final String DATA_COUNTRIES_BBOX = "countries-bbox.txt";
  public static final String DATA_WORLD_CITIES_POINTS = "world-cities-points.txt";

  public static final String QTEST_States_IsWithin_BBox   = "states-IsWithin-BBox.txt";
  public static final String QTEST_States_Intersects_BBox = "states-Intersects-BBox.txt";
  public static final String QTEST_Cities_Intersects_BBox = "cities-Intersects-BBox.txt";
  public static final String QTEST_Simple_Queries_BBox = "simple-Queries-BBox.txt";

  protected Logger log = Logger.getLogger(getClass().getName());

  protected final SpatialArgsParser argsParser = new SpatialArgsParser();

  protected SpatialStrategy strategy;
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
    return getDocuments(getSampleData(testDataFile));
  }

  protected List<Document> getDocuments(Iterator<SpatialTestData> sampleData) {
    List<Document> documents = new ArrayList<>();
    while (sampleData.hasNext()) {
      SpatialTestData data = sampleData.next();
      Document document = new Document();
      document.add(new StringField("id", data.id, Field.Store.YES));
      document.add(new StringField("name", data.name, Field.Store.YES));
      Shape shape = data.shape;
      shape = convertShapeFromGetDocuments(shape);
      if (shape != null) {
        for (Field f : strategy.createIndexableFields(shape)) {
          document.add(f);
        }
        if (storeShape)//just for diagnostics
          document.add(new StoredField(strategy.getFieldName(), shape.toString()));
      }

      documents.add(document);
    }
    return documents;
  }

  /** Subclasses may override to transform or remove a shape for indexing */
  protected Shape convertShapeFromGetDocuments(Shape shape) {
    return shape;
  }

  protected Iterator<SpatialTestData> getSampleData(String testDataFile) throws IOException {
    String path = "data/" + testDataFile;
    InputStream stream = getClass().getClassLoader().getResourceAsStream(path);
    if (stream == null)
      throw new FileNotFoundException("classpath resource not found: "+path);
    return SpatialTestData.getTestData(stream, ctx);//closes the InputStream
  }

  protected Iterator<SpatialTestQuery> getTestQueries(String testQueryFile, SpatialContext ctx) throws IOException {
    InputStream in = getClass().getClassLoader().getResourceAsStream(testQueryFile);
    return SpatialTestQuery.getTestQueries(
        argsParser, ctx, testQueryFile, in );//closes the InputStream
  }

  public void runTestQueries(
      Iterator<SpatialTestQuery> queries,
      SpatialMatchConcern concern) {
    while (queries.hasNext()) {
      SpatialTestQuery q = queries.next();
      runTestQuery(concern, q);
    }
  }

  public void runTestQuery(SpatialMatchConcern concern, SpatialTestQuery q) {
    String msg = q.toString(); //"Query: " + q.args.toString(ctx);
    SearchResults got = executeQuery(makeQuery(q), Math.max(100, q.ids.size()+1));
    if (storeShape && got.numFound > 0) {
      //check stored value is there
      assertNotNull(got.results.get(0).document.get(strategy.getFieldName()));
    }
    if (concern.orderIsImportant) {
      Iterator<String> ids = q.ids.iterator();
      for (SearchResult r : got.results) {
        String id = r.document.get("id");
        if (!ids.hasNext()) {
          fail(msg + " :: Did not get enough results.  Expect" + q.ids + ", got: " + got.toDebugString());
        }
        assertEquals("out of order: " + msg, ids.next(), id);
      }

      if (ids.hasNext()) {
        fail(msg + " :: expect more results then we got: " + ids.next());
      }
    } else {
      // We are looking at how the results overlap
      if (concern.resultsAreSuperset) {
        Set<String> found = new HashSet<>();
        for (SearchResult r : got.results) {
          found.add(r.document.get("id"));
        }
        for (String s : q.ids) {
          if (!found.contains(s)) {
            fail("Results are mising id: " + s + " :: " + found);
          }
        }
      } else {
        List<String> found = new ArrayList<>();
        for (SearchResult r : got.results) {
          found.add(r.document.get("id"));
        }

        // sort both so that the order is not important
        Collections.sort(q.ids);
        Collections.sort(found);
        assertEquals(msg, q.ids.toString(), found.toString());
      }
    }
  }

  protected Query makeQuery(SpatialTestQuery q) {
    return strategy.makeQuery(q.args);
  }

  @SuppressWarnings("deprecation")
  protected void adoc(String id, String shapeStr) throws IOException, ParseException {
    Shape shape = shapeStr==null ? null : ctx.readShapeFromWkt(shapeStr);
    addDocument(newDoc(id, shape));
  }
  protected void adoc(String id, Shape shape) throws IOException {
    addDocument(newDoc(id, shape));
  }

  protected Document newDoc(String id, Shape shape) {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    if (shape != null) {
      for (Field f : strategy.createIndexableFields(shape)) {
        doc.add(f);
      }
      if (storeShape)
        doc.add(new StoredField(strategy.getFieldName(), shape.toString()));//not to be parsed; just for debug
    }
    return doc;
  }

  protected void deleteDoc(String id) throws IOException {
    indexWriter.deleteDocuments(new TermQuery(new Term("id", id)));
  }

  /** scores[] are in docId order */
  protected void checkValueSource(DoubleValuesSource vs, float scores[], float delta) throws IOException {

    for (LeafReaderContext ctx : indexSearcher.getTopReaderContext().leaves()) {
      DoubleValues v = vs.getValues(ctx, null);
      int count = ctx.reader().maxDoc();
      for (int i = 0; i < count; i++) {
        assertTrue(v.advanceExact(i));
        int doc = i + ctx.docBase;
        assertEquals("Not equal for doc " + doc, v.doubleValue(), (double) scores[doc], delta);
      }
    }

  }

  protected void testOperation(Shape indexedShape, SpatialOperation operation,
                               Shape queryShape, boolean match) throws IOException {
    assertTrue("Faulty test",
        operation.evaluate(indexedShape, queryShape) == match ||
            indexedShape.equals(queryShape) &&
              (operation == SpatialOperation.Contains || operation == SpatialOperation.IsWithin));
    adoc("0", indexedShape);
    commit();
    Query query = strategy.makeQuery(new SpatialArgs(operation, queryShape));
    SearchResults got = executeQuery(query, 1);
    assert got.numFound <= 1 : "unclean test env";
    if ((got.numFound == 1) != match)
      fail(operation+" I:" + indexedShape + " Q:" + queryShape);
    deleteAll();//clean up after ourselves
  }

}
