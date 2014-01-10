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

import java.io.IOException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestHighlight extends ServerBaseTestCase {

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

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "price", "{type: float, sort: true, index: true, store: true}");
    put(o, "id", "{type: int, store: true, postingsFormat: Memory}");
    put(o, "date", "{type: atom, index: false, store: true}");
    put(o, "dateFacet", "{type: atom, index: false, store: false, facet: hierarchy}");
    put(o, "author", "{type: text, index: false, facet: flat, group: true}");
    // Register multi-valued field:
    put(o, "authors", "{type: text, highlight: true, facet: flat, multiValued: true, analyzer: {matchVersion: LUCENE_43, class: StandardAnalyzer}}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    o2.put("indexName", "index");

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
    o2.put("fields", o);
    o2.put("indexName", "index");
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

    put(o, "facets", "[{dim: 'dateFacet', topN: 10}]");
    put(o, "retrieveFields", "['id', 'date', 'price', {field: 'body', highlight: " + (snippets ? "snippets" : "whole") + "}]");

    return send("search", o);
  }

  public void testHighlightSnippet() throws Exception {
    deleteAllDocs();
    long gen = addDocument(0, "Melanie", "this is a test.  here is a random sentence.  here is another sentence with test in it.", 10.99f, "2012/10/17");
    JSONObject o = search("test", gen, null, false, true, null, null);

    assertEquals("this is a <b>test</b>.  ...here is another sentence with <b>test</b> in it.",
                 renderHighlight(getArray(o, "hits[0].fields.body")));
  }

  /** Highlight entire value as a single passage (eg good
   *  for title fields). */
  public void testWholeHighlight() throws Exception {
    deleteAllDocs();
    long gen = addDocument(0, "Lisa", "this is a test.  here is a random sentence.  here is another sentence with test in it.", 10.99f, "2012/10/17");
    JSONObject o = search("test", gen, null, false, false, null, null);
    assertEquals("this is a <b>test</b>.  here is a random sentence.  here is another sentence with <b>test</b> in it.",
                 renderHighlight(getArray(o, "hits[0].fields.body")));
  }

  /** Make sure we can index a field with 3 values,
   *  highlight it, and get back 3 values, each of them
   *  separately highlighted (not a single value with the 3
   *  values appended). */
  public void testMultiValuedWholeHighlight() throws Exception {
    deleteAllDocs();

    long gen = addDocument("{fields: {authors: ['Dr. Seuss', 'Bob Smith', 'Seuss is Fun.  Some extra content.']}}");
    JSONObject result = send("search", "{queryText: 'authors:seuss', retrieveFields: [{field: authors, highlight: whole}], searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
    JSONArray fields = getArray(result, "hits[0].fields.authors");
    assertEquals(3, fields.size());
    assertEquals("Dr. <b>Seuss</b>", renderSingleHighlight((JSONArray) fields.get(0)));
    assertEquals("Bob Smith", renderSingleHighlight((JSONArray) fields.get(1)));
    assertEquals("<b>Seuss</b> is Fun.  Some extra content.", renderSingleHighlight((JSONArray) fields.get(2)));
  }

  public void testMultiValuedSnippetHighlight() throws Exception {
    deleteAllDocs();

    long gen = addDocument("{fields: {authors: ['Dr. Seuss', 'Bob Smith', 'Seuss is Fun.  Some extra content.']}}");
    JSONObject result = send("search", "{queryText: 'authors:seuss', retrieveFields: [{field: authors, highlight: snippets, maxPassages: 1}], searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.authors.length"));
    assertEquals("<b>Seuss</b> Bob Smith <b>Seuss</b> is Fun.  ", renderSingleHighlight(getArray(result, "hits[0].fields.authors[0].parts")));
  }
  
  /** Make sure we can use a different maxPassages per field */
  public void testPerFieldMaxPassages() throws Exception {
    deleteAllDocs();
    long gen = addDocument("{fields: {body: 'This sentence has test.  This one does not.  Here is test again.', authors: ['This sentence has test.  This one does not.  Here is test again.']}}");
    JSONObject result = send("search", "{queryText: 'test', retrieveFields: [{field: authors, highlight: snippets, maxPassages: 1}, {field: body, highlight: snippets, maxPassages: 2}], searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));

    // Author has just 1 passage:
    assertEquals(1, getInt(result, "hits[0].fields.authors.length"));
    assertEquals("Here is <b>test</b> again.", renderHighlight(getArray(result, "hits[0].fields.authors")));

    // Body has 2 passages:
    assertEquals(2, getInt(result, "hits[0].fields.body.length"));
    assertEquals("This sentence has <b>test</b>.  ...Here is <b>test</b> again.", renderHighlight(getArray(result, "hits[0].fields.body")));
  }

  /** We don't allow INFO_SEP (U+001F) to appear in
   *  multi-valued highlight fields. */
  public void testContentWithSep() throws Exception {
    deleteAllDocs();
    try {
      addDocument("{fields: {authors: ['Dr. Seuss', 'Bob \u001F Smith', 'Seuss is Fun']}}");
      fail("didn't hit exception");
    } catch (IOException ioe) {
      // expected
    }
  }

  // nocommit fixme
  /*
  public void testNonDefaultOffsetGap() throws Exception {
    // nocommit add test infra to create a randomly named new index?
    _TestUtil.rmDir(new File("offsetgap"));
    curIndexName = "offsetgap";
    send("createIndex", "{rootDir: offsetgap}");
    // Wait at most 1 msec for a searcher to reopen; this
    // value is too low for a production site but for
    // testing we want to minimize sleep time:
    send("liveSettings", "{minRefreshSec: 0.001}");
    send("startIndex", "{}");
    JSONObject o = new JSONObject();

    put(o, "body", "{type: text, multiValued: true, highlight: true, store: true, analyzer: {tokenizer: StandardTokenizer, offsetGap: 100}}");

    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);

    // Index one document:
    long indexGen = getLong(send("addDocument", "{fields: {body: ['highlight me', 'highlight me too']}}"), "indexGen");

    // Search w/ highlight:
    JSONObject result = send("search", "{queryText: highlight, retrieveFields: [{field: 'body', highlight: 'whole'}]}");

    JSONArray parts = getArray(result, "hits[0].fields.body");
    assertEquals(2, parts.size());
    assertEquals("<b>highlight</b> me", renderSingleHighlight(getArray(parts, 0)));
    // nocommit this fails when offsetGap != 1 ... debug!
    //assertEquals("<b>highlight</b> me too", renderSingleHighlight(getArray(parts, 1)));
  }
  */
}

