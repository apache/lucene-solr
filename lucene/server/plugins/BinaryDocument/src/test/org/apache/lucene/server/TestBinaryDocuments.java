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
import java.io.RandomAccessFile;
import java.util.Locale;

import org.apache.commons.codec.binary.Base64;
import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestBinaryDocuments extends ServerBaseTestCase {

  @BeforeClass
  public static void initClass() throws Exception {
    clearDir();

    // Install the BinaryDocumentPlugin:
    File zipFile = new File("../../../dist/BinaryDocument-0.1-SNAPSHOT.zip");
    if (!zipFile.exists()) {
      throw new RuntimeException(zipFile.getCanonicalPath() + " does not exist");
    }
    
    installPlugin(zipFile);
    // nocommit use ant / properties / something cleaner...:
    /*
    File destDir = new File("state/plugins/BinaryDocument/");
    destDir.mkdirs();
    File srcFile = new File("../../BinaryDocument-0.1.jar");
    if (!srcFile.exists()) {
      // nocommit get build.xml to do this!
      throw new RuntimeException("run ant jar first: " + srcFile.getCanonicalPath() + " does not exist");
    }
    File destFile = new File(destDir, "BinaryDocumentPlugin.jar");
    copyFile(srcFile, destFile);
    */

    startServer();
    createAndStartIndex();
    registerFields();
    commit();
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
    System.clearProperty("sun.nio.ch.bugLevel"); // hack WTF
  }

  private static void registerFields() throws Exception {
    JSONObject o = new JSONObject();
    put(o, "body", "{type: text, highlight: true, store: true, analyzer: {class: StandardAnalyzer, matchVersion: LUCENE_43}, similarity: {class: BM25Similarity, b: 0.15}}");
    put(o, "author", "{type: text, index: false, facet: flat, store: true, group: true}");
    put(o, "charCount", "{type: int, store: true}");
    JSONObject o2 = new JSONObject();
    o2.put("indexName", "index");
    o2.put("fields", o);
    send("registerFields", o2);
  }

  public void testCrackDocument() throws Exception {
    // nocommit get test-resources onto classpath so I don't have to
    // hardwire full path!
    RandomAccessFile f = new RandomAccessFile("../../../src/test-resources/test.docx", "r");
    byte[] b = new byte[(int)f.length()];
    f.read(b);

    // Make sure we have docs for crackDocument:
    assertTrue(httpLoad("doc?method=crackDocument").indexOf("<h1>Lucene Server: crackDocument</h1>") != -1);

    String enc = Base64.encodeBase64String(b);
    JSONObject result = send("crackDocument", "{indexName: index, content: '" + enc + "', fileName: 'test.docx'}");
    assertEquals("Word document with a wee bit of text in it.\n", result.get("body")); 
    assertEquals("Michael McCandless", result.get("Author"));
  }

  public void testWordDoc() throws Exception {
    send("deleteAllDocuments", "{indexName: index}");

    // nocommit get test-resources onto classpath so I don't have to
    // hardwire full path!
    RandomAccessFile f = new RandomAccessFile("../../../src/test-resources/test.docx", "r");
    byte[] b = new byte[(int)f.length()];
    f.read(b);

    // Make sure doc is updated w/ new binary param:
    assertTrue(httpLoad("doc?method=addDocument").indexOf("<b>binary</b>") != -1);
    assertTrue(httpLoad("doc?method=addDocuments").indexOf("<b>binary</b>") != -1);
    assertTrue(httpLoad("doc?method=updateDocument").indexOf("<b>binary</b>") != -1);

    String enc = Base64.encodeBase64String(b);
    send("deleteAllDocuments", "{indexName: index}");
    String params = "{indexName: index, binary: {content: '" + enc + "', fileName: 'test.docx', mappings: {body: body, Author: author, 'Character Count': charCount}}}";
    long gen = getLong(send("addDocument", params), "indexGen");

    JSONObject result = send("search", "{indexName: index, searcher: {indexGen: " + gen + "}, retrieveFields: [author, charCount], queryText: 'body:wee'}");
    assertEquals(1, getInt(result, "totalHits"));
    assertEquals("Michael McCandless", getString(result, "hits[0].fields.author"));
    assertEquals(38, getInt(result, "hits[0].fields.charCount"));
  }

  public void testBulkAddWordDoc() throws Exception {
    send("deleteAllDocuments", "{indexName: index}");

    // nocommit get test-resources onto classpath so I don't have to
    // hardwire full path!
    RandomAccessFile f = new RandomAccessFile("../../../src/test-resources/test.docx", "r");
    byte[] b = new byte[(int)f.length()];
    f.read(b);
    String enc = Base64.encodeBase64String(b);

    // Make sure doc is updated w/ new binary param:
    assertTrue(httpLoad("doc?method=bulkAddDocument").indexOf("<b>binary</b>") != -1);
    assertTrue(httpLoad("doc?method=bulkAddDocuments").indexOf("<b>binary</b>") != -1);
    assertTrue(httpLoad("doc?method=bulkUpdateDocument").indexOf("<b>binary</b>") != -1);

    StringBuilder sb = new StringBuilder();
    sb.append("{\"indexName\": \"index\", \"documents\": [");
    for(int i=0;i<100;i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append("{\"binary\": {\"content\": \"" + enc + "\", \"fileName\": \"test.docx\", \"mappings\": {\"body\": \"body\", \"Author\": \"author\", \"Character Count\": \"charCount\"}}}");
    }      
    sb.append("]}");
    String s = sb.toString();

    JSONObject result = sendChunked(s, "bulkAddDocument");
    long indexGen = getLong(result, "indexGen");
    result = send("search", "{indexName: index, queryText: wee, searcher: {indexGen: " + indexGen + "}}");
    assertEquals(100, result.get("totalHits"));
  }
}
