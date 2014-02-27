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

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestFacets extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    curIndexName = "index";
    startServer();
    createAndStartIndex();
    registerFields();
    commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  static String indexFacetField;

  // nocommit need test showing how to change the DVF for
  // the "underlying" facet index field ($facets by default)

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "price", "{type: float, sort: true, search: true, store: true}");
    put(o, "longField", "{type: long, search: true, facet: numericRange}");
    put(o, "doubleField", "{type: double, search: true, facet: numericRange}");
    put(o, "floatField", "{type: float, search: true, facet: numericRange}");
    put(o, "id", "{type: int, store: true, postingsFormat: Memory}");
    put(o, "date", "{type: atom, search: false, store: true}");
    if (random().nextBoolean()) {
      // Send facets to two different random fields:
      String name = "x" + TestUtil.randomSimpleString(random(), 1, 10);
      put(o, "dateFacet", "{type: atom, search: false, store: false, facet: hierarchy, facetIndexFieldName: " + name + "}");
      if (VERBOSE) {
        System.out.println("NOTE: send dateFacet to facetIndexFieldName=" + name);
      }
      name = "y" + TestUtil.randomSimpleString(random(), 1, 10);
      put(o, "author", "{type: text, search: false, facet: flat, group: true, facetIndexFieldName: " + name + "}");
      if (VERBOSE) {
        System.out.println("NOTE: send author to facetIndexFieldName=" + name);
      }

    } else if (random().nextBoolean()) {
      // Send facets to the same random field:
      indexFacetField = "x" + TestUtil.randomSimpleString(random(), 1, 10);
      put(o, "dateFacet", "{type: atom, search: false, store: false, facet: hierarchy, facetIndexFieldName: " + indexFacetField + "}");
      put(o, "author", "{type: text, search: false, facet: flat, group: true, facetIndexFieldName: " + indexFacetField + "}");
      if (VERBOSE) {
        System.out.println("NOTE: send dateFacet to facetIndexFieldName=" + indexFacetField);
        System.out.println("NOTE: send author to facetIndexFieldName=" + indexFacetField);
      }
    } else {
      // Use default $facets field:
      put(o, "dateFacet", "{type: atom, search: false, store: false, facet: hierarchy}");
      put(o, "author", "{type: text, search: false, facet: flat, group: true}");
    }
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

    put(o, "facets", "[{dim: dateFacet, topN: 10}]");
    put(o, "retrieveFields", "[id, date, price, {field: body, highlight: " + (snippets ? "snippets" : "whole") + "}]");

    return send("search", o);
  }

  public void testFacets() throws Exception {
    deleteAllDocs();
    addDocument(0, "Bob", "this is a test", 10.99f, "2012/10/17");
    addDocument(1, "Lisa", "this is a another test", 11.99f, "2012/10/1");
    addDocument(2, "Frank", "this is a third test", 12.99f, "2010/10/1");
    search("test", -1, "price", false, true, null, null);
    assertEquals(3, getInt("totalHits"));
    assertEquals(3, getInt("hits.length"));

    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals("2012/10/17", getString("hits[0].fields.date"));

    assertEquals(1, getInt("hits[1].fields.id"));
    assertEquals("2012/10/1", getString("hits[1].fields.date"));

    assertEquals(2, getInt("hits[2].fields.id"));
    assertEquals("2010/10/1", getString("hits[2].fields.date"));

    assertEquals("top: 3, 2012: 2, 2010: 1", formatFacetCounts(getObject("facets[0]")));
  }    

  public void testFacetsReopen() throws Exception {
    deleteAllDocs();
    addDocument(0, "Bob", "this is a test", 10.99f, "2012/10/17");
    addDocument(1, "Lisa", "this is a another test", 11.99f, "2012/10/1");
    commit();

    addDocument(2, "Frank", "this is a third test", 12.99f, "2010/10/1");
    search("test", -1, "price", false, true, null, null);
    assertEquals(3, getInt("totalHits"));
    assertEquals(3, getInt("hits.length"));

    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals("2012/10/17", getString("hits[0].fields.date"));

    assertEquals(1, getInt("hits[1].fields.id"));
    assertEquals("2012/10/1", getString("hits[1].fields.date"));

    assertEquals(2, getInt("hits[2].fields.id"));
    assertEquals("2010/10/1", getString("hits[2].fields.date"));

    assertEquals("top: 3, 2012: 2, 2010: 1", formatFacetCounts(getObject("facets[0]")));
  }    

  public void testDrillSideways() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {author: Bob}}");
    send("addDocument", "{fields: {author: Lisa}}");
    send("addDocument", "{fields: {author: Lisa}}");
    send("addDocument", "{fields: {author: Tom}}");
    send("addDocument", "{fields: {author: Tom}}");
    send("addDocument", "{fields: {author: Tom}}");

    // Initial query:
    send("search", "{query: MatchAllDocsQuery, facets: [{dim: author, topN: 10}]}");
    assertEquals(6, getInt("totalHits"));
    assertEquals("top: 6, Tom: 3, Lisa: 2, Bob: 1", formatFacetCounts(getObject("facets[0]")));

    // Now, single drill down:
    send("search", "{drillDowns: [{field: author, value: Bob}], query: MatchAllDocsQuery, facets: [{dim: author, topN: 10}]}");
    assertEquals(1, getInt("totalHits"));
    assertEquals("top: 6, Tom: 3, Lisa: 2, Bob: 1", formatFacetCounts(getObject("facets[0]")));

    // Multi (OR'd) drill down:
    send("search", "{drillDowns: [{field: author, value: Bob}, {field: author, value: Lisa}], query: MatchAllDocsQuery, facets: [{dim: author, topN: 10}]}");
    assertEquals(3, getInt("totalHits"));
    assertEquals("top: 6, Tom: 3, Lisa: 2, Bob: 1", formatFacetCounts(getObject("facets[0]")));
  }

  public void testCustomLabels() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {author: Bob}}");
    send("addDocument", "{fields: {author: Lisa}}");
    send("addDocument", "{fields: {author: Lisa}}");
    send("addDocument", "{fields: {author: Tom}}");
    send("addDocument", "{fields: {author: Tom}}");
    send("addDocument", "{fields: {author: Tom}}");

    send("search", "{query: MatchAllDocsQuery, facets: [{dim: author, labels: [Bob, Lisa, Tom]}]}");
    assertEquals("top: -1, Bob: 1, Lisa: 2, Tom: 3", formatFacetCounts(getObject("facets[0]")));
  }

  public void testLongRangeFacets() throws Exception {
    deleteAllDocs();    
    for(int i=0;i<100;i++) {
      send("addDocument", "{fields: {longField: " + i + "}}");
    }
    send("search", "{facets: [{dim: longField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}]}");
    assertEquals("top: 100, All: 100, Half: 50", formatFacetCounts(getObject("facets[0]")));

    send("search", "{drillDowns: [{field: longField, numericRange: {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}}], facets: [{dim: longField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}]}");
    assertEquals("top: 100, All: 100, Half: 50", formatFacetCounts(getObject("facets[0]")));
    assertEquals(50, getInt("totalHits"));
  }

  public void testDoubleRangeFacets() throws Exception {
    deleteAllDocs();    
    for(int i=0;i<100;i++) {
      send("addDocument", "{fields: {doubleField: " + i + "}}");
    }
    send("search", "{facets: [{dim: doubleField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}]}");
    assertEquals("top: 100, All: 100, Half: 50", formatFacetCounts(getObject("facets[0]")));

    send("search", "{drillDowns: [{field: doubleField, numericRange: {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}}], facets: [{dim: doubleField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}]}");
    assertEquals("top: 100, All: 100, Half: 50", formatFacetCounts(getObject("facets[0]")));
    assertEquals(50, getInt("totalHits"));
  }

  // nocommit fails ... we need to add FloatRangeFacetCounts
  // to lucene???

  /*
  public void testFloatRangeFacets() throws Exception {
    deleteAllDocs();    
    long gen = -1;
    for(int i=0;i<100;i++) {
      gen = getLong(send("addDocument", "{fields: {floatField: " + i + "}}"), "indexGen");
    }
    JSONObject o = send("search", "{facets: [{dim: floatField, numericRanges: [{label: All, min: 0, max: 99, minInclusive: true, maxInclusive: true}, {label: Half, min: 0, max: 49, minInclusive: true, maxInclusive: true}]}], searcher: {indexGen: " + gen + "}}");
    System.out.println("got" + get(o, "facets[0]"));
    assertEquals("All", getString(o, "facets[0].counts[1][0]"));
    assertEquals(100, getInt(o, "facets[0].counts[1][1]"));
    assertEquals("Half", getString(o, "facets[0].counts[2][0]"));
    assertEquals(50, getInt(o, "facets[0].counts[2][1]"));
  }
  */

  public void testSortedSetDocValuesFacets() throws Exception {
    curIndexName = "ssdvFacets";
    TestUtil.rmDir(new File(curIndexName));
    send("createIndex", "{rootDir: " + curIndexName + "}");
    send("settings", "{directory: FSDirectory, matchVersion: LUCENE_46}");
    send("startIndex");

    if (indexFacetField != null && random().nextBoolean()) {
      // Send SSDV facets to same field as the taxo facets:
      send("registerFields", "{fields: {ssdv: {type: atom, search: false, store: false, facet: sortedSetDocValues, facetIndexFieldName: " + indexFacetField + "}}}");
    } else if (random().nextBoolean()) {
      // Send SSDV facets to a random index field:
      String name = TestUtil.randomSimpleString(random(), 1, 10);
      send("registerFields", "{fields: {ssdv: {type: atom, search: false, store: false, facet: sortedSetDocValues, facetIndexFieldName: " + name + "}}}");
    } else {
      // Send SSDV facets to default field:
      send("registerFields", "{fields: {ssdv: {type: atom, search: false, store: false, facet: sortedSetDocValues}}}");
    }

    // Verify error message:
    try {
      send("search", "{query: MatchAllDocsQuery, facets: [{dim: ssdv}]}");
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      // nocommit we could/should make this NOT be an error?
      // you should just get back empty facets?
      assertTrue(ioe.getMessage().contains("search > facets: field \"ssdv\" was properly registered with facet=\"sortedSetDocValues\", however no documents were indexed as of this searcher"));
    }

    send("addDocument", "{fields: {ssdv: one}}");
    send("addDocument", "{fields: {ssdv: two}}");
    send("commit");
    send("addDocument", "{fields: {ssdv: two}}");
    send("addDocument", "{fields: {ssdv: three}}");
    send("commit");
    send("addDocument", "{fields: {ssdv: one}}");
    send("addDocument", "{fields: {ssdv: one}}");

    for(int i=0;i<2;i++) {
      // nocommit if i remove indexGen from here, the error
      // message is bad: it says "each element in the array
      // my have these params:..." when it shouldn't
      send("search", "{query: MatchAllDocsQuery, facets: [{dim: ssdv}]}");
      assertEquals(6, getInt("totalHits"));
      assertEquals("top: 6, one: 3, two: 2, three: 1", formatFacetCounts(getObject("facets[0]")));

      // Make sure suggest survives server restart:    
      shutdownServer();
      startServer();
      send("startIndex");
    }
  }

  public static String formatFacetCounts(JSONObject facets) {
    StringBuilder sb = new StringBuilder();
    JSONArray arr = getArray(facets, "counts");
    for(Object o : arr) {
      JSONArray facet = (JSONArray) o;
      sb.append(facet.get(0));
      sb.append(": ");
      sb.append(facet.get(1));
      sb.append(", ");
    }
    String s = sb.toString();
    // remove last ', ':
    return s.substring(0, s.length()-2);
  }
}

