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
    put(o, "boost", "{type: float, sort: true}");
    put(o, "text", "{type: text, analyzer: WhitespaceAnalyzer}");
    put(o, "logboost", "{type: virtual, expression: ln(boost)}");
    put(o, "scoreboost", "{type: virtual, expression: _score+ln(boost)}");
    put(o, "id", "{type: int, sort: true, store: true, search: false}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }

  /** Non-reversed sort by virtual field */
  public void testSortByVirtualFieldStraight() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {id: 1, boost: 2.0}}");
    send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost}]}, retrieveFields: [id]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals(1, getInt("hits[1].fields.id"));

    assertEquals(0.0f, getFloat("hits[0].fields.sortFields.logboost"), .0001f);
    assertEquals(.6931f, getFloat("hits[1].fields.sortFields.logboost"), .0001f);
  }

  /** Reversed sort by virtual field */
  public void testSortByVirtualFieldReversed() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {id: 1, boost: 2.0}}");
    send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost, reverse: true}]}, retrieveFields: [id]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(.6931f, getFloat("hits[0].fields.sortFields.logboost"), .0001f);
    assertEquals(0.0f, getFloat("hits[1].fields.sortFields.logboost"), .0001f);
  }

  /** Sort by virtual field, and ask for its value */
  public void testRetrieveVirtualFieldWithSort() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {id: 1, boost: 2.0}}");
    send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: logboost}]}, retrieveFields: [id, logboost]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals(1, getInt("hits[1].fields.id"));

    assertEquals(0.0f, getFloat("hits[0].fields.logboost"), .0001f);
    assertEquals(.6931f, getFloat("hits[1].fields.logboost"), .0001f);
  }

  /** Don't sort by virtual field, and ask for its value */
  public void testRetrieveVirtualFieldWithoutSort() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {id: 1, boost: 2.0}}");
    send("search", "{query: MatchAllDocsQuery, sort: {fields: [{field: id, reverse: true}]}, retrieveFields: [id, logboost]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(.6931f, getFloat("hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat("hits[1].fields.logboost"), .0001f);
  }

  public void testFieldUsingAnother() throws Exception {
    deleteAllDocs();
    send("registerFields", "{fields: {scoreboost2: {type: virtual, expression: '2*scoreboost'}}}");

    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search", "{queryText: wind, sort: {fields: [{field: scoreboost2, reverse: true}]}, retrieveFields: [id, scoreboost2]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(1.60721f, getFloat("hits[0].fields.scoreboost2"), .0001f);
    assertEquals(0.22092f, getFloat("hits[1].fields.scoreboost2"), .0001f);

    assertEquals(1.60721f, getFloat("hits[0].fields.sortFields.scoreboost2"), .0001f);
    assertEquals(0.22092f, getFloat("hits[1].fields.sortFields.scoreboost2"), .0001f);
  }

  public void testWithScore1() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search", "{queryText: wind, sort: {fields: [{field: scoreboost, reverse: true}]}, retrieveFields: [id, logboost]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(.6931f, getFloat("hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat("hits[1].fields.logboost"), .0001f);
  }

  /** Also tries to retrieve the scoreboost */
  public void testWithScore2() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search", "{queryText: wind, sort: {fields: [{field: scoreboost, reverse: true}]}, retrieveFields: [id, scoreboost, logboost]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(.80361f, getFloat("hits[0].fields.scoreboost"), .0001f);
    assertEquals(.11046f, getFloat("hits[1].fields.scoreboost"), .0001f);

    assertEquals(.6931f, getFloat("hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat("hits[1].fields.logboost"), .0001f);
  }

  /** Sort by not score, and try to retrieve expression
   *  using score. */
  public void testWithScore3() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search", "{queryText: wind, sort: {fields: [{field: id, reverse: true}]}, retrieveFields: [id, scoreboost, logboost]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(.80361f, getFloat("hits[0].fields.scoreboost"), .0001f);
    assertEquals(.11046f, getFloat("hits[1].fields.scoreboost"), .0001f);

    assertEquals(.6931f, getFloat("hits[0].fields.logboost"), .0001f);
    assertEquals(0.0f, getFloat("hits[1].fields.logboost"), .0001f);
  }

  public void testSyntaxError() throws Exception {
    deleteAllDocs();
    try {
      send("registerFields", "{fields: {bad: {type: virtual, expression: 'ln(boost'}}}");
      fail("didn't hit exception");
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
      fail("didn't hit exception");
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
      fail("didn't hit exception");
    } catch (IOException ioe) {
      String message = ioe.toString();
      assertTrue(message.contains("registerFields > fields > bad > expression: could not evaluate expression"));
      assertTrue(message.contains("Field 'bad2' cannot be used in an expression: it was not registered with sort=true"));
    }
  }

  public void testDynamicFieldSameName() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");

    // It's an error to try to define a dynamic field name
    // that already exists:
    try {
      send("search", "{queryText: wind, virtualFields: [{name: scoreboost, expression: 2*_score}]}");
      fail("didn't hit exception");
    } catch (IOException ioe) {
      String message = ioe.toString();
      assertTrue(message.contains("search > virtualFields[0] > name: registered field or dynamic field \"scoreboost\" already exists"));
    }
  }

  public void testCycles1() throws Exception {
    assertFailsWith("registerFields", "{fields: {bad: {type: virtual, expression: ln(bad)}}}",
                    "registerFields > fields > bad > expression: could not evaluate expression: java.lang.IllegalArgumentException: Invalid reference 'bad'");
  }

  public void testCycles2() throws Exception {
    assertFailsWith("search",
                    "{queryText: wind, virtualFields: [{name: bad, expression: ln(bad)}]}",
                    "search > virtualFields[0] > expression: could not evaluate expression: java.lang.IllegalArgumentException: Invalid reference 'bad'");
  }

  public void testRetrievedDynamicField() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search",
         "{queryText: wind, virtualFields: [{name: scoreboost3, expression: 3*scoreboost}], sort: {fields: [{field: id, reverse: true}]}, retrieveFields: [id, scoreboost3]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));

    assertEquals(2.41082, getFloat("hits[0].fields.scoreboost3"), .0001f);
    assertEquals(0.33138, getFloat("hits[1].fields.scoreboost3"), .0001f);
  }

  public void testSortedDynamicField() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {text: 'the wind is howling like this swirling storm inside', id: 0, boost: 1.0}}");
    send("addDocument", "{fields: {text: 'I am one with the wind and sky', id: 1, boost: 2.0}}");
    send("search",
         "{queryText: wind, virtualFields: [{name: scoreboost3, expression: 3*scoreboost}], sort: {fields: [{field: scoreboost3}]}, retrieveFields: [id]}");
    assertEquals(2, getInt("totalHits"));
    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals(1, getInt("hits[1].fields.id"));

    assertEquals(0.33138, getFloat("hits[0].fields.sortFields.scoreboost3"), .0001f);
    assertEquals(2.41082, getFloat("hits[1].fields.sortFields.scoreboost3"), .0001f);
  }
}
