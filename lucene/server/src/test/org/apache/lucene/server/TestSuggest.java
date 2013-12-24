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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.lucene.util._TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

public class TestSuggest extends ServerBaseTestCase {

  static File tempFile;
  
  @BeforeClass
  public static void initClass() throws Exception {
    clearDir();
    startServer();
    createAndStartIndex();
    //registerFields();
    commit();
    File tempDir = _TestUtil.getTempDir("TestSuggest");
    tempDir.mkdirs();
    tempFile = new File(tempDir, "suggest.in");
  }

  @AfterClass
  public static void fini() throws Exception {
    shutdownServer();
    System.clearProperty("sun.nio.ch.bugLevel"); // hack WTF
    tempFile = null;
  }

  public void testAnalyzingSuggest() throws Exception {
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("5\u001flucene\u001ffoobar\n");
    out.write("10\u001flucifer\u001ffoobar\n");
    out.write("15\u001flove\u001ffoobar\n");
    out.write("5\u001ftheories take time\u001ffoobar\n");
    out.write("5\u001fthe time is now\u001ffoobar\n");
    out.close();

    JSONObject result = send("buildSuggest", "{indexName: index, localFile: '" + tempFile.getAbsolutePath() + "', class: 'AnalyzingSuggester', suggestName: 'suggest', indexAnalyzer: EnglishAnalyzer, queryAnalyzer: {tokenizer: StandardTokenizer, tokenFilters: [EnglishPossessiveFilter,LowerCaseFilter,PorterStemFilter]]}}");
    assertEquals(5, result.get("count"));
    commit();

    for(int i=0;i<2;i++) {
      result = send("suggestLookup", "{indexName: index, text: 'l', suggestName: 'suggest'}");
      assertEquals(3, get(result, "results.length"));

      assertEquals("love", get(result, "results[0].key"));
      assertEquals(15, get(result, "results[0].weight"));
      assertEquals("foobar", get(result, "results[0].payload"));

      assertEquals("lucifer", get(result, "results[1].key"));
      assertEquals(10, get(result, "results[1].weight"));
      assertEquals("foobar", get(result, "results[1].payload"));

      assertEquals("lucene", get(result, "results[2].key"));
      assertEquals(5, get(result, "results[2].weight"));
      assertEquals("foobar", get(result, "results[2].payload"));

      // ForkLastTokenFilter allows 'the' to match
      // 'theories'; without it (and StopKeywordFilter) the
      // would be dropped:
      result = send("suggestLookup", "{indexName: index, text: 'the', suggestName: 'suggest'}");
      assertEquals(1, get(result, "results.length"));

      assertEquals("theories take time", get(result, "results[0].key"));
      assertEquals(5, get(result, "results[0].weight"));
      assertEquals("foobar", get(result, "results[0].payload"));

      result = send("suggestLookup", "{indexName: index, text: 'the ', suggestName: 'suggest'}");
      assertEquals(0, get(result, "results.length"));

      // Make sure suggest survives server restart:
      shutdownServer();
      startServer();
      send("startIndex", "{indexName: index}");
    }
  }

  public void testInfixSuggest() throws Exception {
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\n");
    out.close();

    JSONObject result = send("buildSuggest", "{indexName: index, localFile: '" + tempFile.getAbsolutePath() + "', class: 'InfixSuggester', suggestName: 'suggest2', analyzer: {tokenizer: WhitespaceTokenizer, tokenFilters: [LowerCaseFilter]}}");
    assertEquals(1, result.get("count"));
    commit();

    for(int i=0;i<2;i++) {
      result = send("suggestLookup", "{indexName: index, text: 'lost', suggestName: 'suggest2'}");
      assertEquals(15, get(result, "results[0].weight"));
      assertEquals("love <font color=red>lost</font>", toString(getArray(result, "results[0].key")));
      assertEquals("foobar", get(result, "results[0].payload"));

      // Make sure suggest survives server restart:    
      shutdownServer();
      startServer();
      send("startIndex", "{indexName: index}");
    }
  }

  public String toString(JSONArray fragments) {
    StringBuilder sb = new StringBuilder();
    for(Object _o : fragments) {
      JSONObject o = (JSONObject) _o;
      if ((Boolean) o.get("isHit")) {
        sb.append("<font color=red>");
      }
      sb.append(o.get("text").toString());
      if ((Boolean) o.get("isHit")) {
        sb.append("</font>");
      }
    }
    return sb.toString();
  }

  public void testFuzzySuggest() throws Exception {
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("15\u001flove lost\u001ffoobar\n");
    out.close();

    JSONObject result = send("buildSuggest", "{indexName: index, localFile: '" + tempFile.getAbsolutePath() + "', class: 'FuzzySuggester', suggestName: 'suggest3', analyzer: {tokenizer: WhitespaceTokenizer, tokenFilters: [LowerCaseFilter]}}");
    assertEquals(1, result.get("count"));
    commit();

    for(int i=0;i<2;i++) {
      // 1 transposition and this is prefix of "love":
      result = send("suggestLookup", "{indexName: index, text: 'lvo', suggestName: 'suggest3'}");
      assertEquals(15, get(result, "results[0].weight"));
      assertEquals("love lost", get(result, "results[0].key"));
      assertEquals("foobar", get(result, "results[0].payload"));

      // Make sure suggest survives server restart:    
      shutdownServer();
      startServer();
      send("startIndex", "{indexName: index}");
    }
  }
}
