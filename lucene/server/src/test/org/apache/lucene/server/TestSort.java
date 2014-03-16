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
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONObject;

public class TestSort extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    curIndexName = "index";
    startServer();
    createAndStartIndex();
    registerFields();
    commit();
  }

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "atom", "{type: atom, sort: true}");
    put(o, "int", "{type: int, sort: true}");
    put(o, "float", "{type: float, sort: true}");
    put(o, "long", "{type: long, sort: true}");
    put(o, "double", "{type: double, sort: true}");
    put(o, "text", "{type: text, analyzer: WhitespaceAnalyzer}");
    put(o, "id", "{type: int, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    send("registerFields", o2);
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  public void testMissingLastAtom() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, atom: a}}");
    send("addDocument", "{fields: {id: 1, atom: b}}");
    // field is missing:
    send("addDocument", "{fields: {id: 2}}");
    long gen = getLong("indexGen");

    verifySort("atom");
  }

  public void testMissingLastInt() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, int: -7}}");
    send("addDocument", "{fields: {id: 1, int: 7}}");
    // field is missing:
    send("addDocument", "{fields: {id: 2}}");
    verifySort("int");
  }

  public void testMissingLastLong() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, long: -7}}");
    send("addDocument", "{fields: {id: 1, long: 7}}");
    // field is missing:
    send("addDocument", "{fields: {id: 2}}");
    verifySort("long");
  }

  public void testMissingLastFloat() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, float: -7}}");
    send("addDocument", "{fields: {id: 1, float: 7}}");
    // field is missing:
    send("addDocument", "{fields: {id: 2}}");
    verifySort("float");
  }

  public void testMissingLastDouble() throws Exception {
    deleteAllDocs();
    send("addDocument", "{fields: {id: 0, double: -7}}");
    send("addDocument", "{fields: {id: 1, double: 7}}");
    // field is missing:
    send("addDocument", "{fields: {id: 2}}");
    verifySort("double");
  }

  public void testNoSortOnText() throws Exception {
    assertFailsWith("registerFields",
                    "{fields: {bad: {type: text, sort: true, analyzer: WhitespaceAnalyzer}}}",
                    "registerFields > fields > bad > sort: cannot sort text fields; use atom instead");
  }

  private void verifySort(String field) throws Exception {

    long gen = getLong("indexGen");

    // missing is (annoyingly) first by default:
    send("search",
         "{query: MatchAllDocsQuery, topHits: 3, retrieveFields: [id], searcher: {indexGen: " + gen + "}, sort: {fields: [{field: " + field + "}]}}");
    assertEquals(3, getInt("totalHits"));
    assertEquals(2, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));
    assertEquals(1, getInt("hits[2].fields.id"));

    // reverse, missing is (annoyingly) first by default:
    send("search",
         "{query: MatchAllDocsQuery, topHits: 3, retrieveFields: [id], searcher: {indexGen: " + gen + "}, sort: {fields: [{field: " + field + ", reverse: true}]}}");
    assertEquals(3, getInt("totalHits"));
    assertEquals(1, getInt("hits[0].fields.id"));
    assertEquals(0, getInt("hits[1].fields.id"));
    assertEquals(2, getInt("hits[2].fields.id"));

    // missing last:
    send("search",
         "{query: MatchAllDocsQuery, topHits: 3, retrieveFields: [id], searcher: {indexGen: " + gen + "}, sort: {fields: [{field: " + field + ", missingLast: true}]}}");
    assertEquals(3, getInt("totalHits"));
    assertEquals(0, getInt("hits[0].fields.id"));
    assertEquals(1, getInt("hits[1].fields.id"));
    assertEquals(2, getInt("hits[2].fields.id"));

    // reverse, missing last:
    send("search",
         "{query: MatchAllDocsQuery, topHits: 3, retrieveFields: [id], searcher: {indexGen: " + gen + "}, sort: {fields: [{field: " + field + ", reverse: true, missingLast: true}]}}");
    assertEquals(3, getInt("totalHits"));
    assertEquals(2, getInt("hits[0].fields.id"));
    assertEquals(1, getInt("hits[1].fields.id"));
    assertEquals(0, getInt("hits[2].fields.id"));
  }
}
