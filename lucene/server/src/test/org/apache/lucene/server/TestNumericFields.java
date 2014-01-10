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
import net.minidev.json.JSONObject;

public class TestNumericFields extends ServerBaseTestCase {

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
    put(o, "intNoSort", "{type: int, store: true}");
    put(o, "intSort", "{type: int, sort: true, store: true}");
    put(o, "floatNoSort", "{type: float, store: true}");
    put(o, "floatSort", "{type: float, sort: true, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    o2.put("indexName", "index");
    send("registerFields", o2);
  }

  public void testRetrieve() throws Exception {
    deleteAllDocs();
    long gen = getLong(send("addDocument", "{fields: {intNoSort: 17, intSort: 22, floatNoSort: 17.0, floatSort: 22.0}}"), "indexGen");
    JSONObject result = send("search", "{retrieveFields: [intNoSort, intSort, floatNoSort, floatSort], query: MatchAllDocsQuery, searcher: {indexGen: " + gen + "}}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals(17, getInt(result, "hits[0].fields.intNoSort"));
    assertEquals(22, getInt(result, "hits[0].fields.intSort"));
    assertEquals(17.0f, getFloat(result, "hits[0].fields.floatNoSort"), 1e-7);
    assertEquals(22.0f, getFloat(result, "hits[0].fields.floatSort"), 1e-7);
  }
}
