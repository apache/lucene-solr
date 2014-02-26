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
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.lucene.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONObject;

public class TestPlugins extends ServerBaseTestCase {

  @BeforeClass
  public static void init() throws Exception {
    useDefaultIndex = false;
    File tmpDir = TestUtil.getTempDir("TestPlugins");
    tmpDir.mkdirs();
    File zipFile = new File(tmpDir, "MockPlugin-0.1.zip");
    ZipOutputStream os = new ZipOutputStream(new FileOutputStream(zipFile));
    addToZip(os, "Mock/org/apache/lucene/server/MockPlugin.class", "MockPlugin.class");
    addToZip(os, "Mock/lucene-server-plugin.properties", "MockPlugin-lucene-server-plugin.properties");
    addToZip(os, "Mock/site/hello.txt", "MockPlugin-hello.txt");
    os.close();
    installPlugin(zipFile);
    startServer();
  }

  static void addToZip(ZipOutputStream dest, String zipName, String resourcePath) throws Exception {
    ZipEntry file = new ZipEntry(zipName);
    dest.putNextEntry(file);
    try (InputStream input = TestPlugins.class.getResourceAsStream(resourcePath)) {
      byte buf[] = new byte[1024];
      int numRead;
      while ((numRead = input.read(buf)) >= 0) {
        dest.write(buf, 0, numRead);
      }
    }
    dest.closeEntry();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
  }

  // nocommit this test should install from zip file (call installPlugin(...))

  public void testMockPlugin() throws Exception {

    // Make sure docs reflect new mockFoobar:
    String doc = httpLoad("doc?method=addDocument");
    assertTrue(doc.indexOf("<b>mockFoobar</b>") != -1);

    // nocommit test docs: verify foobar is there
    // nocommit need a "list plugins" API: verify foobar is there
    // nocommit send addDocument & verify change "took"

    TestUtil.rmDir(new File("index"));
    send("createIndex", "{rootDir: index}");
    send("startIndex");
    send("registerFields", "{fields: {id: {type: int, store: true, postingsFormat: Memory}, intfield: {type: int, store: true}}}");
    long gen = getLong(send("addDocument", "{fields: {id: 0, mockFoobar: 7}}"), "indexGen");

    JSONObject result = send("search", "{searcher: {indexGen: " + gen + "}, query: MatchAllDocsQuery, retrieveFields: [id, intfield]}");
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
