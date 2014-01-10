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

public class TestSettings extends ServerBaseTestCase {

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
    send("registerFields", "{fields: {body: {type: text, analyzer: StandardAnalyzer}}}");
  }

  public void testNRTCachingDirSettings() throws Exception {
    deleteAllDocs();
    commit();
    send("stopIndex", "{}");
    JSONObject o = send("settings", "{}");
    assertEquals(0, o.size());
    // Turn off NRTCachingDir:
    send("settings", "{nrtCachingDirectory.maxMergeSizeMB: 0.0, nrtCachingDirectory.maxSizeMB: 0.0}");
    o = send("settings", "{}");
    assertEquals(2, o.size());
    send("startIndex", "{}");
    long gen = getLong(send("addDocument", "{fields: {body: 'here is a test'}}"), "indexGen");
    assertEquals(1, getInt(send("search", "{queryText: test, searcher: {indexGen: " + gen + "}}"), "totalHits"));
  }
}
