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

import java.util.Locale;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestAddDocuments extends ServerBaseTestCase {
  
  @BeforeClass
  public static void initClass() throws Exception {
    useDefaultIndex = true;
    curIndexName = "index";
    startServer();
    createAndStartIndex();
    registerFields();
    //commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  private static void registerFields() throws Exception {
    send("registerFields", "{fields: {docType: {type: atom}, name: {type: atom}, country: {type: atom}, skill: {type: atom}, year: {type: int}}}");
  }

  private JSONObject getResume(String name, String country) {
    JSONObject o = new JSONObject();
    o.put("docType", "resume");
    o.put("name", name);
    o.put("country", country);
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    return o2;
  }

  private JSONObject getJob(String skill, int year) {
    JSONObject o = new JSONObject();
    o.put("skill", skill);
    o.put("year", year);
    JSONObject o2 = new JSONObject();
    o2.put("fields", o);
    return o2;
  }

  public void testAddDocuments() throws Exception {
    deleteAllDocs();

    JSONObject o = new JSONObject();
    o.put("indexName", "index");
    o.put("parent", getResume("Lisa", "United Kingdom"));
    JSONArray arr = new JSONArray();
    o.put("children", arr);
    arr.add(getJob("java", 2007));
    arr.add(getJob("python", 2010));
    JSONObject result = send("addDocuments", o);
    long indexGen = ((Number) result.get("indexGen")).longValue();    

    // search on parent:
    result = send("search", String.format(Locale.ROOT, "{queryText: 'name:Lisa', searcher: {indexGen: %d}}", indexGen));
    assertEquals(1, result.get("totalHits"));

    // search on child:
    result = send("search", String.format(Locale.ROOT, "{queryText: 'skill:python', searcher: {indexGen: %d}}", indexGen));
    assertEquals(1, result.get("totalHits"));
  }

  public void testBulkAddDocuments() throws Exception {
    deleteAllDocs();
    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      JSONObject o = new JSONObject();
      o.put("parent", getResume("Lisa", "United Kingdom"));
      JSONArray arr = new JSONArray();
      o.put("children", arr);
      arr.add(getJob("java", 2007));
      arr.add(getJob("python", 2010));
      if (i > 0) {
        sb.append(',');
      }
      sb.append(o.toString());
    }
    sb.append("]}");

    String s = sb.toString();

    JSONObject result = sendChunked(s, "bulkAddDocuments");
    assertEquals(100, result.get("indexedDocumentBlockCount"));
    long indexGen = ((Number) result.get("indexGen")).longValue();

    // search on parent:
    result = send("search", String.format(Locale.ROOT, "{queryText: 'name:Lisa', searcher: {indexGen: %d}}", indexGen));
    assertEquals(100, result.get("totalHits"));

    // search on child:
    result = send("search", String.format(Locale.ROOT, "{queryText: 'skill:python', searcher: {indexGen: %d}}", indexGen));
    assertEquals(100, result.get("totalHits"));
  }

  // TODO: test block join/grouping once they are impl'd!
}
