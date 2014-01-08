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
import net.minidev.json.JSONObject;

public class TestVirtualFields extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
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
    put(o, "boost", "{type: float, sort: true}");
    put(o, "text", "{type: text, analyzer: WhitespaceAnalyzer}");
    put(o, "logboost", "{type: virtual, expression: ln(boost)}");
    put(o, "scoreboost", "{type: virtual, expression: _score+ln(boost)}");
    put(o, "id", "{type: int, sort: true, store: true, index: false}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }

  // nocommit need dynamic exprs too (defined for one request)

  /** Non-reversed sort by virtual field */
  public void testSortByVirtualFieldStraight() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost}]}, retrieveFields: [id], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(0, getInt(result, "hits[0].fields.id"));
    assertEquals(1, getInt(result, "hits[1].fields.id"));

    assertEquals(0.0f, getFloat(result, "hits[0].fields.sortFields.logboost"), .0001f);
    assertEquals(.6931f, getFloat(result, "hits[1].fields.sortFields.logboost"), .0001f);
  }

  /** Reversed sort by virtual field */
  public void testSortByVirtualFieldReversed() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost, reverse: true}]}, retrieveFields: [id], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));

    assertEquals(.6931f, getFloat(result, "hits[0].fields.sortFields.logboost"), .0001f);
    assertEquals(0.0f, getFloat(result, "hits[1].fields.sortFields.logboost"), .0001f);
  }

  /** Sort by virtual field, and ask for its value */
  public void testRetrieveVirtualFieldWithSort() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost}]}, retrieveFields: [id, logboost], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(0, getInt(result, "hits[0].fields.id"));
    assertEquals(1, getInt(result, "hits[1].fields.id"));

    assertEquals(0.0f, getFloat(result, "hits[0].fields.logboost"), .0001f);
    assertEquals(.6931f, getFloat(result, "hits[1].fields.logboost"), .0001f);
  }

  /** Don't sort by virtual field, and ask for its value */
  public void testRetrieveVirtualFieldWithoutSort() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: id, reverse: true}]}, retrieveFields: [id, logboost], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));

    assertEquals(.6931f, getFloat(result, "hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat(result, "hits[1].fields.logboost"), .0001f);
  }

  public void testWithScore1() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{queryText: wind, sort: {fields: [{field: scoreboost, reverse: true}]}, retrieveFields: [id, logboost], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));

    assertEquals(.6931f, getFloat(result, "hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat(result, "hits[1].fields.logboost"), .0001f);
  }

  /** Also tries to retrieve the scoreboost */
  public void testWithScore2() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{queryText: wind, sort: {fields: [{field: scoreboost, reverse: true}]}, retrieveFields: [id, scoreboost, logboost], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));

    assertEquals(.80361f, getFloat(result, "hits[0].fields.scoreboost"), .0001f);
    assertEquals(.11046f, getFloat(result, "hits[1].fields.scoreboost"), .0001f);

    assertEquals(.6931f, getFloat(result, "hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat(result, "hits[1].fields.logboost"), .0001f);
  }

  /** Sort by not score, and try to retrieve expression
   *  using score. */
  public void testWithScore3() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    long gen = getLong(send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}"), "indexGen");
    JSONObject result = send("search", "{queryText: wind, sort: {fields: [{field: id, reverse: true}]}, retrieveFields: [id, scoreboost, logboost], searcher: {indexGen: " + gen + "}}");
    assertEquals(2, getInt(result, "totalHits"));
    assertEquals(1, getInt(result, "hits[0].fields.id"));
    assertEquals(0, getInt(result, "hits[1].fields.id"));

    assertEquals(.80361f, getFloat(result, "hits[0].fields.scoreboost"), .0001f);
    assertEquals(.11046f, getFloat(result, "hits[1].fields.scoreboost"), .0001f);

    assertEquals(.6931f, getFloat(result, "hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat(result, "hits[1].fields.logboost"), .0001f);
  }

  public void testSyntaxError() throws Exception {
    deleteAllDocs();
    try {
      send("registerFields", "{fields: {bad: {type: virtual, expression: 'ln(boost'}}}");
    } catch (IOException ioe) {
      String message = ioe.toString();
      assertTrue(message.contains("registerFields > fields > bad > expression: could not parse expression"));
      assertTrue(message.contains("unexpected token end of expression"));
    }
  }

  public void testNonExistentField() throws Exception {
    deleteAllDocs();
    try {
      send("registerFields", "{fields: {bad: {type: virtual, expression: 'ln(bad2)'}}}");
    } catch (IOException ioe) {
      String message = ioe.toString();
      assertTrue(message.contains("registerFields > fields > bad > expression: could not evaluate expression"));
      assertTrue(message.contains("Invalid reference 'bad2'"));
    }
  }

  public void testExistentButNoDocValuesField() throws Exception {
    deleteAllDocs();
    try {
      send("registerFields", "{fields: {bad2: {type: int, store: true}, bad: {type: virtual, expression: 'ln(bad2)'}}}");
    } catch (IOException ioe) {
      String message = ioe.toString();
      assertTrue(message.contains("registerFields > fields > bad > expression: could not evaluate expression"));
      assertTrue(message.contains("Field 'bad2' cannot be used in an expression: it was not registered with sort=true"));
    }
  }
}
