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
import java.io.RandomAccessFile;
import java.util.Locale;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestIndexing extends ServerBaseTestCase {
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
    put(o, "id", "{type: atom, store: true, postingsFormat: Memory}");
    put(o, "price", "{type: float, sort: true, index: true, store: true}");
    put(o, "date", "{type: atom, index: false, store: true}");
    put(o, "dateFacet", "{type: atom, index: false, store: false, facet: hierarchy}");
    put(o, "author", "{type: text, index: false, facet: flat, store: true, group: true}");
    put(o, "charCount", "{type: int, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("indexName", "index");
    o2.put("fields", o);
    send("registerFields", o2);
  }

  public void testUpdateDocument() throws Exception {
    send("addDocument", "{indexName: index, fields: {body: 'here is a test', id: '0'}}");
    long gen = getLong(send("updateDocument", "{indexName: index, term: {field: id, term: '0'}, fields: {body: 'here is another test', id: '0'}}"), "indexGen");
    JSONObject o = send("search", "{indexName: index, queryText: 'body:test', searcher: {indexGen: " + gen + "}, retrieveFields: [body]}");
    assertEquals(1, getInt(o, "totalHits"));
    assertEquals("here is another test", getString(o, "hits[0].fields.body"));
  }

  public void testBulkUpdateDocuments() throws Exception {
    deleteAllDocs();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      JSONObject o = new JSONObject();
      o.put("body", "here is the body " + i);
      o.put("id", ""+i);
      if (i > 0) {
        sb.append(',');
      }
      JSONObject o2 = new JSONObject();
      o2.put("fields", o);
      sb.append(o2.toString());
    }
    sb.append("]}");

    String s = sb.toString();

    JSONObject result = sendChunked(s, "bulkAddDocument");
    assertEquals(100, result.get("indexedDocumentCount"));
    long indexGen = ((Number) result.get("indexGen")).longValue();
    assertEquals(1, getInt(send("search", "{indexName: index, queryText: 'body:99', searcher: {indexGen: " + indexGen + "}}"), "totalHits"));

    // Now, update:
    sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      JSONObject o2 = new JSONObject();
      JSONObject o = new JSONObject();
      o2.put("fields", o);
      o.put("body", "here is the body " + i);
      o.put("id", ""+i);
      if (i > 0) {
        sb.append(',');
      }
      put(o2, "term", "{field: id, term: '" + i + "'}");
      sb.append(o2.toString());
    }
    sb.append("]}");

    s = sb.toString();

    result = sendChunked(s, "bulkUpdateDocument");
    assertEquals(100, result.get("indexedDocumentCount"));
    indexGen = ((Number) result.get("indexGen")).longValue();
    assertEquals(1, getInt(send("search", "{indexName: index, queryText: 'body:99', searcher: {indexGen: " + indexGen + "}}"), "totalHits"));

    assertEquals(100, getInt(send("search", "{indexName: index, query: MatchAllDocsQuery, searcher: {indexGen: " + indexGen + "}}"), "totalHits"));
  }

  public void testBulkAddException() throws Exception {
    deleteAllDocs();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      JSONObject o = new JSONObject();
      o.put("body", "here is the body " + i);
      o.put("id", ""+i);
      if (i > 0) {
        sb.append(',');
      }
      if (i == 57) {
        JSONArray broken = new JSONArray();
        broken.add("2013");
        broken.add("");
        broken.add("17");
        o.put("dateFacet", broken);
      }
      JSONObject o2 = new JSONObject();
      o2.put("fields", o);
      sb.append(o2.toString());
    }
    sb.append("]}");

    String s = sb.toString();

    try {
      sendChunked(s, "bulkAddDocument");
      fail("did not hit expected exception");
    } catch (IOException ioe) {
      // expected
    }
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

  public void testBulkAddDocument() throws Exception {
    deleteAllDocs();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      JSONObject o = new JSONObject();
      o.put("body", "here is the body " + i);
      o.put("author", "Mr. " + i);
      o.put("price", 15.66);
      o.put("id", ""+i);
      o.put("date", "01/01/2013");
      if (i > 0) {
        sb.append(",");
      }
      JSONObject o2 = new JSONObject();
      o2.put("fields", o);
      sb.append(o2.toString());
    }
    sb.append("]}");
    String s = sb.toString();

    JSONObject result = sendChunked(s, "bulkAddDocument");
    assertEquals(100, result.get("indexedDocumentCount"));
    long indexGen = getLong(result, "indexGen");
    JSONObject r = search("99", indexGen, null, false, true, null, null);
    assertEquals(1, ((Integer) r.get("totalHits")).intValue());
  }

  /** Make sure you get an error if you try to addDocument
   *  after index is stopped */
  public void testAddAfterStop() throws Exception {
    deleteAllDocs();
    send("stopIndex", "{indexName: index}");
    try {
      send("addDocument", "{indexName: index, fields: {}}");
      fail();
    } catch (IOException ioe) {
      // expected
    }
    send("startIndex", "{indexName: index}");
  }

  public void testBoost() throws Exception {
    _TestUtil.rmDir(new File("boost"));
    send("createIndex", "{indexName: boost, rootDir: boost}");
    send("settings", "{indexName: boost, directory: RAMDirectory, matchVersion: LUCENE_40}");
    // Just to test index.ramBufferSizeMB:
    send("liveSettings", "{indexName: boost, index.ramBufferSizeMB: 20.0}");
    send("registerFields", "{indexName: boost, fields: {id: {type: atom, store: true}, body: {type: text, analyzer: StandardAnalyzer}}}");
    send("startIndex", "{indexName: boost}");
    send("addDocument", "{indexName: boost, fields: {id: '0', body: 'here is a test'}}");
    long gen = getLong(send("addDocument", "{indexName: boost, fields: {id: '1', body: 'here is a test'}}"), "indexGen");
    JSONObject result = send("search", String.format(Locale.ROOT, "{indexName: boost, retrieveFields: [id], queryText: test, searcher: {indexGen: %d}}", gen));
    assertEquals(2, getInt(result, "hits.length"));
    // Unboosted, the hits come back in order they were added:
    assertEquals("0", getString(result, "hits[0].fields.id"));
    assertEquals("1", getString(result, "hits[1].fields.id"));

    // Do it again, this time setting higher boost for 2nd doc:
    send("deleteAllDocuments", "{indexName: boost}");
    send("addDocument", "{indexName: boost, fields: {id: '0', body: 'here is a test'}}");
    gen = getLong(send("addDocument", "{indexName: boost, fields: {id: '1', body: {boost: 2.0, value: 'here is a test'}}}"), "indexGen");
    result = send("search", String.format(Locale.ROOT, "{indexName: boost, retrieveFields: [id], queryText: test, searcher: {indexGen: %d}}", gen));
    assertEquals(2, getInt(result, "hits.length"));
    // Unboosted, the hits come back in order they were added:
    assertEquals("1", getString(result, "hits[0].fields.id"));
    assertEquals("0", getString(result, "hits[1].fields.id"));

    send("stopIndex", "{indexName: boost}");
    send("deleteIndex", "{indexName: boost}");
  }
}
