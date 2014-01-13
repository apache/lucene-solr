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

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestIndexing extends ServerBaseTestCase {

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
    put(o, "id", "{type: atom, store: true, postingsFormat: Memory}");
    put(o, "price", "{type: float, sort: true, search: true, store: true}");
    put(o, "date", "{type: atom, search: false, store: true}");
    put(o, "dateFacet", "{type: atom, search: false, store: false, facet: hierarchy}");
    put(o, "author", "{type: text, search: false, facet: flat, store: true, group: true}");
    put(o, "charCount", "{type: int, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }

  public void testUpdateDocument() throws Exception {
    send("addDocument", "{fields: {body: 'here is a test', id: '0'}}");
    long gen = getLong(send("updateDocument", "{term: {field: id, term: '0'}, fields: {body: 'here is another test', id: '0'}}"), "indexGen");
    JSONObject o = send("search", "{queryText: 'body:test', searcher: {indexGen: " + gen + "}, retrieveFields: [body]}");
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
    assertEquals(1, getInt(send("search", "{queryText: 'body:99', searcher: {indexGen: " + indexGen + "}}"), "totalHits"));

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
    assertEquals(1, getInt(send("search", "{queryText: 'body:99', searcher: {indexGen: " + indexGen + "}}"), "totalHits"));

    assertEquals(100, getInt(send("search", "{query: MatchAllDocsQuery, searcher: {indexGen: " + indexGen + "}}"), "totalHits"));
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
        o.put("foobar", 17);
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

    put(o, "facets", "[{dim: dateFacet, topN: 10}]");
    put(o, "retrieveFields", "[id, date, price, {field: body, highlight: " + (snippets ? "snippets" : "whole") + "}]");

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
    send("stopIndex");
    try {
      send("addDocument", "{fields: {}}");
      fail();
    } catch (IOException ioe) {
      // expected
    }
    send("startIndex");
  }

  public void testBoost() throws Exception {
    _TestUtil.rmDir(new File("boost"));
    curIndexName = "boost";
    send("createIndex");
    send("settings", "{directory: RAMDirectory, matchVersion: LUCENE_40}");
    // Just to test merge rate limiting:
    send("settings", "{mergeMaxMBPerSec: 10.0}");
    // Just to test index.ramBufferSizeMB:
    send("liveSettings", "{index.ramBufferSizeMB: 20.0}");
    send("registerFields", "{fields: {id: {type: atom, store: true}, body: {type: text, analyzer: StandardAnalyzer}}}");
    send("startIndex");
    send("addDocument", "{fields: {id: '0', body: 'here is a test'}}");
    long gen = getLong(send("addDocument", "{fields: {id: '1', body: 'here is a test'}}"), "indexGen");
    JSONObject result = send("search", String.format(Locale.ROOT, "{retrieveFields: [id], queryText: test, searcher: {indexGen: %d}}", gen));
    assertEquals(2, getInt(result, "hits.length"));
    // Unboosted, the hits come back in order they were added:
    assertEquals("0", getString(result, "hits[0].fields.id"));
    assertEquals("1", getString(result, "hits[1].fields.id"));

    // Do it again, this time setting higher boost for 2nd doc:
    send("deleteAllDocuments");
    send("addDocument", "{fields: {id: '0', body: 'here is a test'}}");
    gen = getLong(send("addDocument", "{fields: {id: '1', body: {boost: 2.0, value: 'here is a test'}}}"), "indexGen");
    result = send("search", String.format(Locale.ROOT, "{retrieveFields: [id], queryText: test, searcher: {indexGen: %d}}", gen));
    assertEquals(2, getInt(result, "hits.length"));
    // Unboosted, the hits come back in order they were added:
    assertEquals("1", getString(result, "hits[0].fields.id"));
    assertEquals("0", getString(result, "hits[1].fields.id"));

    send("deleteIndex");
  }

  public void testInvalidNormsFormat() throws Exception {
    try {
      send("settings", "{normsFormat: NoSuchNormsFormat}");
      fail("did not hit exception");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("unrecognized value \"NoSuchNormsFormat\""));
    }
  }

  public void testNormsFormat() throws Exception {
    for(int i=0;i<2;i++) {
      curIndexName = "normsFormat";
      if (VERBOSE) {
        System.out.println("\nTEST: createIndex");
      }
      send("createIndex");
      String norms;
      if (i == 0) {
        norms = "normsFormat: Lucene42";
      } else {
        norms = "normsFormat: {class: Lucene42, acceptableOverheadRatio: 0.0}";
      }
      send("settings", "{directory: RAMDirectory, matchVersion: LUCENE_40, " + norms + "}");
      send("registerFields",
           "{fields: {id: {type: atom, store: true}," +
           " body: {type: text, analyzer: StandardAnalyzer}}}");
      if (VERBOSE) {
        System.out.println("\nTEST: startIndex");
      }
      send("startIndex");
      send("addDocument", "{fields: {id: '0', body: 'here is a test'}}");
      long gen = getLong(send("addDocument", "{fields: {id: '1', body: 'here is a test again'}}"), "indexGen");
      JSONObject result = send("search", String.format(Locale.ROOT, "{retrieveFields: [id], queryText: test, searcher: {indexGen: %d}}", gen));
      assertEquals(2, getInt(result, "hits.length"));
      assertEquals("0", getString(result, "hits[0].fields.id"));
      assertEquals("1", getString(result, "hits[1].fields.id"));

      if (VERBOSE) {
        System.out.println("\nTEST: deleteIndex");
      }
      send("deleteIndex");
    }
  }

  public void testOnlySettings() throws Exception {
    for(int i=0;i<2;i++) {
      curIndexName = "settings";
      if (VERBOSE) {
        System.out.println("\nTEST: create");
      }
      if (i == 0) {
        send("createIndex");
      } else {
        File dir = new File(_TestUtil.getTempDir("recency"), "root");
        send("createIndex", "{rootDir: " + dir.getAbsolutePath() + "}");
      }
      String dirImpl = i == 0 ? "RAMDirectory" : "FSDirectory";

      if (VERBOSE) {
        System.out.println("\nTEST: settings1");
      }
      send("settings", "{directory: " + dirImpl + ", matchVersion: LUCENE_40}");
      send("registerFields", "{fields: {id: {type: atom, store: true}}}");
      //send("stopIndex");
      if (VERBOSE) {
        System.out.println("\nTEST: settings2");
      }
      JSONObject result = send("settings");
      assertEquals(dirImpl, getString(result, "directory"));
      if (i == 1) {
        // With FSDir, the index & settings should survive a
        // server bounce, even if the index wasn't ever started:

        if (VERBOSE) {
          System.out.println("\nTEST: bounce");
        }

        shutdownServer();
        startServer();

        if (VERBOSE) {
          System.out.println("\nTEST: settings3");
        }
        result = send("settings");
        assertEquals(dirImpl, getString(result, "directory"));
      }
      send("deleteIndex");
    }
  }

  public void testIllegalRegisterFields() throws Exception {
    // Cannot specify an analyzer with an atom field (it
    // always uses KeywordAnalyzer):
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: atom, analyzer: WhitespaceAnalyzer}}}",
                    "registerFields > fields > bad > analyzer: no analyzer allowed with atom (it's hardwired to KeywordAnalyzer internally)");

    // Must specify an analyzer with a text field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: text}}}",
                    "registerFields > fields > bad > indexAnalyzer: either analyzer or indexAnalyzer must be specified for an indexed text field");

    // Must not specify an analyzer with a non-searched text field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: text, search: false, analyzer: WhitespaceAnalyzer}}}",
                    "registerFields > fields > bad > analyzer: no analyzer allowed when search=false");

    // Must not disable store if highlight is true:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: text, store: false, highlight: true, analyzer: WhitespaceAnalyzer}}}",
                    "registerFields > fields > bad > store: store=false is not allowed when highlight=true");

    // Cannot search a facet=hierarchy field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: atom, facet: hierarchy, search: true}}}",
                    "registerFields > fields > bad > facet: facet=hierarchy fields cannot have search=true");

    // Cannot store a facet=hierarchy field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: atom, facet: hierarchy, search: false, store: true}}}",
                    "registerFields > fields > bad > facet: facet=hierarchy fields cannot have store=true");

    // Cannot highlight a facet=hierarchy field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: atom, facet: hierarchy, highlight: true}}}",
                    "registerFields > fields > bad > facet: facet=hierarchy fields cannot have highlight=true");

    // Cannot create a pointless do-nothing field:
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: atom, search: false, store: false}}}",
                    "registerFields > fields > bad: field does nothing: it's neither searched, stored, sorted, grouped, highlighted nor faceted");
  }
}
