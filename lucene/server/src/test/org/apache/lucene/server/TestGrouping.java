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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestGrouping extends ServerBaseTestCase {

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

  public void testGrouping() throws Exception {
    deleteAllDocs();
    addDocument(0, "Lisa", "this is a test.  here is a random sentence.  here is another sentence with test in it.", 10.99f, "2012/10/17");
    addDocument(0, "Tom", "this is a test.  here is another sentence with test in it.", 10.99f, "2012/10/17");
    addDocument(0, "Lisa", "this is a test.  this sentence has test twice test.", 10.99f, "2012/10/17");
    long gen = addDocument(0, "Bob", "this is a test.", 10.99f, "2012/10/17");

    JSONObject o2 = search("test", gen, null, false, false, "author", null);
    assertEquals(4, ((Number) o2.get("totalHits")).intValue());
    assertEquals(4, ((Number) o2.get("totalGroupedHits")).intValue());
    JSONArray a = (JSONArray) o2.get("groups");
    assertEquals(3, a.size());

    assertEquals("Lisa", ((JSONObject) a.get(0)).get("groupValue"));
    assertEquals(2, ((Number)((JSONObject) a.get(0)).get("totalHits")).intValue());

    assertEquals("Tom", ((JSONObject) a.get(1)).get("groupValue"));
    assertEquals(1, ((Number)((JSONObject) a.get(1)).get("totalHits")).intValue());

    assertEquals("Bob", ((JSONObject) a.get(2)).get("groupValue"));
    assertEquals(1, ((Number)((JSONObject) a.get(2)).get("totalHits")).intValue());

    // Should be this:
    /*
{
    "facets": [
        {
            "2012": 4
        }
    ],
    "groups": [
        {
            "groupSortFields": {
                "<score>": 0.7768564
            },
            "groupValue": "Bob",
            "hits": [
                {
                    "doc": 3,
                    "fields": {
                        "body": "this is a <b>test</b>.",
                        "date": "2012/10/17",
                        "id": "0",
                        "price": "10.99"
                    },
                    "score": 0.7768564
                }
            ],
            "maxScore": 0.7768564,
            "totalHits": 1
        },
        {
            "groupSortFields": {
                "<score>": 0.50458306
            },
            "groupValue": "Lisa",
            "hits": [
                {
                    "doc": 2,
                    "fields": {
                        "body": "this is a <b>test</b>.  this sentence has <b>test</b> twice <b>test</b>.",
                        "date": "2012/10/17",
                        "id": "0",
                        "price": "10.99"
                    },
                    "score": 0.50458306
                },
                {
                    "doc": 0,
                    "fields": {
                        "body": "this is a <b>test</b>.  here is a random sentence.  here is another sentence with <b>test</b> in it.",
                        "date": "2012/10/17",
                        "id": "0",
                        "price": "10.99"
                    },
                    "score": 0.3433253
                }
            ],
            "maxScore": 0.50458306,
            "totalHits": 2
        },
        {
            "groupSortFields": {
                "<score>": 0.4806554
            },
            "groupValue": "Tom",
            "hits": [
                {
                    "doc": 1,
                    "fields": {
                        "body": "this is a <b>test</b>.  here is another sentence with <b>test</b> in it.",
                        "date": "2012/10/17",
                        "id": "0",
                        "price": "10.99"
                    },
                    "score": 0.4806554
                }
            ],
            "maxScore": 0.4806554,
            "totalHits": 1
        }
    ],
    "maxScore": 0.7768564,
    "searchState": {
        "lastDocID": 1,
        "searcher": 25
    },
    "totalGroupedHits": 4,
    "totalHits": 4
}
    */
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
      if (groupField == null) {
        sort.put("doDocScores", true);
      }

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

  public void testGroupingWithGroupSort() throws Exception {
    deleteAllDocs();
    addDocument(0, "Lisa", "this is a test.  here is a random sentence.  here is another sentence with test in it.", 5.99f, "2010/10/17");
    addDocument(0, "Tom", "this is a test.  here is another sentence with test in it.", 11.99f, "2011/10/17");
    addDocument(0, "Lisa", "this is a test.  this sentence has test twice test.", 1.99f, "2012/10/17");
    long gen = addDocument(0, "Bob", "this is a test.", 7.99f, "2013/10/17");

    JSONObject o2 = search("test", gen, "price", false, false, "author", "price");
    assertEquals(4, ((Number) o2.get("totalHits")).intValue());
    assertEquals(4, ((Number) o2.get("totalGroupedHits")).intValue());
    JSONArray a = (JSONArray) o2.get("groups");
    assertEquals(3, a.size());

    assertEquals("Lisa", ((JSONObject) a.get(0)).get("groupValue"));
    assertEquals(2, ((Number)((JSONObject) a.get(0)).get("totalHits")).intValue());
    assertNull(((JSONObject) a.get(0)).get("maxScore"));

    assertEquals("Bob", ((JSONObject) a.get(1)).get("groupValue"));
    assertEquals(1, ((Number)((JSONObject) a.get(1)).get("totalHits")).intValue());
    assertNull(((JSONObject) a.get(1)).get("maxScore"));

    assertEquals("Tom", ((JSONObject) a.get(2)).get("groupValue"));
    assertEquals(1, ((Number)((JSONObject) a.get(2)).get("totalHits")).intValue());
    assertNull(((JSONObject) a.get(2)).get("maxScore"));
  }
}
