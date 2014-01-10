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
import java.util.Map;

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestSnapshots extends ServerBaseTestCase {
  
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
    send("registerFields", "{fields: {body: {type: text, analyzer: {class: EnglishAnalyzer, matchVersion: LUCENE_43}}, id: {type: atom, store: true}, facet: {type: atom, index: false, facet: flat}}}");
  }

  public void testBasic() throws Exception {
    deleteAllDocs();

    // Add one doc:
    JSONObject o = send("addDocument", "{fields: {body: 'here is the body', id: '0', facet: 'facet'}}");
    long indexGen = ((Number) o.get("indexGen")).longValue();
    commit();

    o = send("search", "{queryText: 'body:body', searcher: {indexGen:" + indexGen + "}}");
    assertEquals(1, o.get("totalHits"));

    // Take snapshot before making some changes:
    JSONObject result = send("createSnapshot", "{}");
    String id = getString(result, "id");
    // System.out.println("GOT: " + prettyPrint(result));

    // Delete first doc, register new field, add another:
    send("deleteDocuments", "{field: id, values: ['0']}");
    send("registerFields", "{fields: {field: {type: 'atom'}}}");
    send("addDocument", "{fields: {body: 'here is the body', id: '1', facet: 'facet2', field: 'abc'}}");
    commit();

    File backupDir = _TestUtil.getTempDir("backup");

    // Make sure all files in the snapshot still exist, even
    // though we deleted that segment, and make a backup:
    try {
      for(Map.Entry<String,Object> ent : result.entrySet()) {
        if (ent.getKey().equals("id")) {
          continue;
        }
        File dirPath = new File(curIndexName, ent.getKey());
        File destDir = new File(backupDir, ent.getKey());
        destDir.mkdirs();
        for (Object sub : ((JSONArray) ent.getValue())) {
          String fileName = (String) sub;
          File sourceFile = new File(dirPath, fileName);
          assertTrue("file " + sourceFile + " does not exist", sourceFile.exists());
          copyFile(sourceFile, new File(destDir, fileName));
          //System.out.println("copied to " + new File(destDir, fileName));
        }
      }

      // Make sure we can search the snapshot and only get 1 hit:
      JSONObject searchResult = send("search", "{retrieveFields: [id], searcher: {snapshot: \"" + id + "\"}, query: MatchAllDocsQuery}");
      assertEquals(1, getInt(searchResult, "totalHits"));
      assertEquals("0", getString(searchResult, "hits[0].fields.id"));

      // Bounce the server:
      shutdownServer();
      startServer();
      send("startIndex", "{}");

      // Make sure files still exist (snapshot persisted):
      for(Map.Entry<String,Object> ent : result.entrySet()) {
        if (ent.getKey().equals("id")) {
          continue;
        }
        File dirPath = new File(curIndexName, ent.getKey());
        File destDir = new File(backupDir, ent.getKey());
        destDir.mkdirs();
        for (Object sub : ((JSONArray) ent.getValue())) {
          String fileName = (String) sub;
          File sourceFile = new File(dirPath, fileName);
          assertTrue(sourceFile.exists());
        }
      }

      // Make sure we can still search the snapshot:
      searchResult = send("search", "{retrieveFields: [id], searcher: {snapshot: \"" + id + "\"}, query: MatchAllDocsQuery}");
      assertEquals(1, getInt(searchResult, "totalHits"));
      assertEquals("0", getString(searchResult, "hits[0].fields.id"));

      // Now, release the snapshot:
      send("releaseSnapshot", "{id: \"" + id + "\"}");

      // Make sure some files in the snapshot are now gone:
      boolean someGone = false;
      for(Map.Entry<String,Object> ent : result.entrySet()) {
        if (ent.getKey().equals("id")) {
          continue;
        }
        String dirPath = ent.getKey();
        for (Object sub : ((JSONArray) ent.getValue())) {
          String fileName = (String) sub;
          if (!(new File(dirPath, fileName)).exists()) {
            someGone = true;
          }
        }
      }
      assertTrue(someGone);

      // nocommit test searching against old snapshot after restart

      // Restart server against the backup image:
      shutdownServer();
      startServer();
      send("startIndex", "{}");

      // Make sure seach is working, and we still see only
      // one hit:
      o = send("search", "{queryText: 'body:body', searcher: {indexGen:" + indexGen + "}}");
      assertEquals(1, o.get("totalHits"));

      shutdownServer();

    } finally {
      _TestUtil.rmDir(backupDir);
    }
  }

  // nocommit need testSearchSnapshot, and also
  // shutdown/restart server

  // TODO: threaded test, taking snapshot while threads are
  // adding/deleting/committing
}
