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

public class TestLiveValues extends ServerBaseTestCase {

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
    send("registerFields", "{fields: {id: {type: atom, store: true, postingsFormat: Memory}}}");
    send("registerFields", "{fields: {value: {type: atom, search: false, store: true, liveValues: id}}}");
  }

  // nocommit testDeletions

  public void testLiveFields() throws Exception {
    JSONArray arr = new JSONArray();
    for(int i=0;i<100;i++) {
      send("addDocument", "{fields: {id: '" + i + "', value: 'value is " + i + "'}}");
      arr.add("" + i);
    }
    JSONObject request = new JSONObject();
    request.put("indexName", "index");
    request.put("ids", arr);
    request.put("field", "value");
    
    JSONObject o = send("liveValues", request);
    arr = (JSONArray) o.get("values");
    assertEquals(100, arr.size());
    for(int i=0;i<100;i++) {
      assertEquals("value is " + i, arr.get(i));
    }
  }
}