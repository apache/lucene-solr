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

package org.apache.solr.request;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.search.ReturnFields;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.PHPSerializedResponseWriter;
import org.apache.solr.response.PythonResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.RubyResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.SolrReturnFields;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test some aspects of JSON/python writer output (very incomplete)
 *
 */
public class JSONWriterTest extends SolrTestCaseJ4 {
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema.xml");
  }

  private void jsonEq(String expected, String received) {
    expected = expected.trim();
    received = received.trim();
    assertEquals(expected, received);
  }
  
  @Test
  public void testTypes() throws IOException {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    QueryResponseWriter w = new PythonResponseWriter();

    StringWriter buf = new StringWriter();
    rsp.add("data1", Float.NaN);
    rsp.add("data2", Double.NEGATIVE_INFINITY);
    rsp.add("data3", Float.POSITIVE_INFINITY);
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{'data1':float('NaN'),'data2':-float('Inf'),'data3':float('Inf')}");

    w = new RubyResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{'data1'=>(0.0/0.0),'data2'=>-(1.0/0.0),'data3'=>(1.0/0.0)}");

    w = new JSONResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    jsonEq(buf.toString(), "{\"data1\":\"NaN\",\"data2\":\"-Infinity\",\"data3\":\"Infinity\"}");
    req.close();
  }

  @Test
  public void testJSON() throws IOException {
    SolrQueryRequest req = req("wt","json","json.nl","arrarr");
    SolrQueryResponse rsp = new SolrQueryResponse();
    JSONResponseWriter w = new JSONResponseWriter();

    StringWriter buf = new StringWriter();
    NamedList nl = new NamedList();
    nl.add("data1", "he\u2028llo\u2029!");       // make sure that 2028 and 2029 are both escaped (they are illegal in javascript)
    nl.add(null, 42);
    rsp.add("nl", nl);

    rsp.add("byte", Byte.valueOf((byte)-3));
    rsp.add("short", Short.valueOf((short)-4));
    rsp.add("bytes", "abc".getBytes(StandardCharsets.UTF_8));

    w.write(buf, req, rsp);
    jsonEq("{\"nl\":[[\"data1\",\"he\\u2028llo\\u2029!\"],[null,42]],\"byte\":-3,\"short\":-4,\"bytes\":\"YWJj\"}", buf.toString());
    req.close();
  }

  @Test
  public void testJSONSolrDocument() throws IOException {
    SolrQueryRequest req = req(CommonParams.WT,"json",
                               CommonParams.FL,"id,score");
    SolrQueryResponse rsp = new SolrQueryResponse();
    JSONResponseWriter w = new JSONResponseWriter();

    ReturnFields returnFields = new SolrReturnFields(req);
    rsp.setReturnFields(returnFields);

    StringWriter buf = new StringWriter();

    SolrDocument solrDoc = new SolrDocument();
    solrDoc.addField("id", "1");
    solrDoc.addField("subject", "hello2");
    solrDoc.addField("title", "hello3");
    solrDoc.addField("score", "0.7");

    SolrDocumentList list = new SolrDocumentList();
    list.setNumFound(1);
    list.setStart(0);
    list.setMaxScore(0.7f);
    list.add(solrDoc);

    rsp.addResponse(list);

    w.write(buf, req, rsp);
    String result = buf.toString();
    assertFalse("response contains unexpected fields: " + result, 
                result.contains("hello") || 
                result.contains("\"subject\"") || 
                result.contains("\"title\""));
    assertTrue("response doesn't contain expected fields: " + result, 
               result.contains("\"id\"") &&
               result.contains("\"score\""));


    req.close();
  }
  
}
