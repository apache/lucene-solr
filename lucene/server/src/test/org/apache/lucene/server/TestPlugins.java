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
import java.io.PrintWriter;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestPlugins extends ServerBaseTestCase {

  @BeforeClass
  public static void init() throws Exception {
    clearDir();
    installPlugin(new File("../MockPlugin-0.1.zip"));
    startServer();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
    System.clearProperty("sun.nio.ch.bugLevel"); // hack WTF
  }


  // nocommit this test should install from zip file (call installPlugin(...))

  public void testMockPlugin() throws Exception {

    // Make sure docs reflect new mockFoobar:
    String doc = httpLoad("doc?method=addDocument");
    assertTrue(doc.indexOf("<b>mockFoobar</b>") != -1);

    // nocommit test docs: verify foobar is there
    // nocommit need a "list plugins" API: verify foobar is there
    // nocommit send addDocument & verify change "took"

    send("createIndex", "{indexName: index, rootDir: test}");
    send("startIndex", "{indexName: index}");
    send("registerFields", "{indexName: index, fields: {id: {type: int, store: true, postingsFormat: Memory}, intfield: {type: int, store: true}}}");
    long gen = getLong(send("addDocument", "{indexName: index, fields: {id: 0, mockFoobar: 7}}"), "indexGen");

    JSONObject result = send("search", "{indexName: index, searcher: {indexGen: " + gen + "}, query: MatchAllDocsQuery, retrieveFields: [id, intfield]}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals(14, getInt(result, "hits[0].fields.intfield"));
    //System.out.println("got: " + prettyPrint(result));
  }

  /** Make sure a plugin can have/serve a static file. */
  public void testStaticFile() throws Exception {
    String contents = httpLoad("plugins/Mock/hello.txt");
    assertEquals("hello world!\n", contents);
  }
}
