package org.apache.lucene.server;

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

import java.util.Locale;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestFacets extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    clearDir();
    startServer();
    createAndStartIndex();
    registerFields();
    commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
    System.clearProperty("sun.nio.ch.bugLevel"); // hack WTF
  }

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "price", "{type: float, sort: true, index: true, store: true}");
    put(o, "longField", "{type: long, index: true, facet: numericRange}");
    put(o, "id", "{type: int, store: true, postingsFormat: Memory}");
    put(o, "date", "{type: atom, index: false, store: true}");
    put(o, "dateFacet", "{type: atom, index: false, store: false, facet: hierarchy}");
    put(o, "author", "{type: text, index: false, facet: flat, group: true}");
    JSONObject o2 = new JSONObject();
    o2.put("indexName", "index");
    o2.put("fields", o);
    send("registerFields", o2);
  }

  // Returns gen for the added document
  private long addDocument(int id, String author, String body, float price, String date) throws Exception {
    JSONObject o = new JSONObject();
    o.put("body", body);
    o.put("author", author);
    o.put("price", price);
    o.put("id", id);
    o.put("date", date);
    JSONArray path = new JSONArray();
    o.put("dateFacet", path);
    for(String part : date.split("/")) {
      path.add(part);
    }

    JSONObject o2 = new JSONObject();
    o2.put("indexName", "index");
    o2.put("fields", o);
    JSONObject result = send("addDocument", o2);
    return getLong(result, "indexGen");
  }

  private JSONObject search(String query, long indexGen, String sortField, boolean reversed, boolean snippets, String groupField, String groupSortField) throws Exception {
    JSONObject o = new JSONObject();
    o.put("indexName", "index");
    o.put("queryText", query);
    if (indexGen != -1) {
      JSONObject o2 = new JSONObject();
      o.put("searcher", o2);
      o2.put("indexGen", indexGen);
    }

    if (sortField != null) {
      JSONObject sort = new JSONObject();
      o.put("sort", sort);
      sort.put("doDocScores", true);

      JSONArray sortFields = new JSONArray();
      sort.put("fields", sortFields);

      JSONObject o2 = new JSONObject();
      sortFields.add(o2);

      o2.put("field", sortField);
      o2.put("reverse", reversed);
    }

    if (groupField != null) {
      String s = "{field: '" + groupField + "'";
      if (groupSortField != null) {
        s += ", sort: [{field: '" + groupSortField + "'}]";
      }
      s += "}";
      put(o, "grouping", s);
    }

    put(o, "facets", "[{path: 'dateFacet', topN: 10}]");
    put(o, "retrieveFields", "['id', 'date', 'price', {field: 'body', highlight: " + (snippets ? "snippets" : "whole") + "}]");

    return send("search", o);
  }

  public void testFacets() throws Exception {
    deleteAllDocs();
    addDocument(0, "Bob", "this is a test", 10.99f, "2012/10/17");
    addDocument(1, "Lisa", "this is a another test", 11.99f, "2012/10/1");
    long gen = addDocument(2, "Frank", "this is a third test", 12.99f, "2010/10/1");
    JSONObject o = search("test", gen, "price", false, true, null, null);
    assertEquals(3, ((Number) o.get("totalHits")).intValue());

    JSONArray hits = (JSONArray) o.get("hits");
    assertEquals(3, hits.size());

    JSONObject hit = (JSONObject) hits.get(0);
    assertEquals(0, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2012/10/17", ((JSONObject) hit.get("fields")).get("date"));

    hit = (JSONObject) hits.get(1);
    assertEquals(1, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2012/10/1", ((JSONObject) hit.get("fields")).get("date"));

    hit = (JSONObject) hits.get(2);
    assertEquals(2, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2010/10/1", ((JSONObject) hit.get("fields")).get("date"));
    JSONArray facets = getArray(o, "facets[0].counts");
    assertEquals(3, facets.size());
    assertEquals("[\"top\",3]", facets.get(0).toString());
    assertEquals("[\"2012\",2]", facets.get(1).toString());
    assertEquals("[\"2010\",1]", facets.get(2).toString());
  }    

  public void testFacetsReopen() throws Exception {
    deleteAllDocs();
    addDocument(0, "Bob", "this is a test", 10.99f, "2012/10/17");
    addDocument(1, "Lisa", "this is a another test", 11.99f, "2012/10/1");
    commit();

    long gen = addDocument(2, "Frank", "this is a third test", 12.99f, "2010/10/1");
    JSONObject o = search("test", gen, "price", false, true, null, null);
    assertEquals(3, ((Number) o.get("totalHits")).intValue());

    JSONArray hits = (JSONArray) o.get("hits");
    assertEquals(3, hits.size());

    JSONObject hit = (JSONObject) hits.get(0);
    assertEquals(0, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2012/10/17", ((JSONObject) hit.get("fields")).get("date"));

    hit = (JSONObject) hits.get(1);
    assertEquals(1, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2012/10/1", ((JSONObject) hit.get("fields")).get("date"));

    hit = (JSONObject) hits.get(2);
    assertEquals(2, ((JSONObject) hit.get("fields")).get("id"));
    assertEquals("2010/10/1", ((JSONObject) hit.get("fields")).get("date"));

    JSONArray facets = getArray(o, "facets[0].counts");
    assertEquals(3, facets.size());
    assertEquals("[\"top\",3]", facets.get(0).toString());
    assertEquals("[\"2012\",2]", facets.get(1).toString());
    assertEquals("[\"2010\",1]", facets.get(2).toString());
  }    


  public void testDrillSideways() throws Exception {
    deleteAllDocs();
    send("addDocument", "{indexName: index, fields: {author: Bob}}");
    send("addDocument", "{indexName: index, fields: {author: Lisa}}");
    send("addDocument", "{indexName: index, fields: {author: Lisa}}");
    send("addDocument", "{indexName: index, fields: {author: Tom}}");
    send("addDocument", "{indexName: index, fields: {author: Tom}}");
    long indexGen = getLong(send("addDocument", "{indexName: index, fields: {author: Tom}}"), "indexGen");

    // Initial query:
    JSONObject o = send("search", String.format(Locale.ROOT, "{indexName: index, query: MatchAllDocsQuery, facets: [{path: [author], topN: 10}], searcher: {indexGen: %d}}", indexGen));
    assertEquals(6, o.get("totalHits"));
    assertEquals("[[\"top\",0],[\"Tom\",3],[\"Lisa\",2],[\"Bob\",1]]", getArray(o, "facets[0].counts").toString());

    // Now, single drill down:
    o = send("search", String.format(Locale.ROOT, "{indexName: index, drillDowns: [{field: author, values: [Bob]}], query: MatchAllDocsQuery, facets: [{path: [author], topN: 10}], searcher: {indexGen: %d}}", indexGen));
    assertEquals(1, o.get("totalHits"));
    assertEquals("[[\"top\",0],[\"Tom\",3],[\"Lisa\",2],[\"Bob\",1]]", getArray(o, "facets[0].counts").toString());

    // Multi drill down:
    o = send("search", String.format(Locale.ROOT, "{indexName: index, drillDowns: [{field: author, values: [Bob, Lisa]}], query: MatchAllDocsQuery, facets: [{path: [author], topN: 10}], searcher: {indexGen: %d}}", indexGen));
    assertEquals(3, o.get("totalHits"));
    assertEquals("[[\"top\",0],[\"Tom\",3],[\"Lisa\",2],[\"Bob\",1]]", getArray(o, "facets[0].counts").toString());
  }

  public void testRangeFacets() throws Exception {
    deleteAllDocs();    
    long gen = -1;
    for(int i=0;i<100;i++) {
      gen = getLong(send("addDocument", "{indexName: index, fields: {longField: " + i + "}}"), "indexGen");
    }
    JSONObject o = send("search", "{indexName: index, facets: [{path: longField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}], searcher: {indexGen: " + gen + "}}");
    assertEquals("All", getString(o, "facets[0].counts[1][0]"));
    assertEquals(100, getInt(o, "facets[0].counts[1][1]"));
    assertEquals("Half", getString(o, "facets[0].counts[2][0]"));
    assertEquals(50, getInt(o, "facets[0].counts[2][1]"));
  }
}

