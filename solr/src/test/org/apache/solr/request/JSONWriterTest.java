/**
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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.response.JSONResponseWriter;
import org.apache.solr.response.PHPSerializedResponseWriter;
import org.apache.solr.response.PythonResponseWriter;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.RubyResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
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
  
  @Test
  public void testNaNInf() throws IOException {
    SolrQueryRequest req = req("dummy");
    SolrQueryResponse rsp = new SolrQueryResponse();
    QueryResponseWriter w = new PythonResponseWriter();

    StringWriter buf = new StringWriter();
    rsp.add("data1", Float.NaN);
    rsp.add("data2", Double.NEGATIVE_INFINITY);
    rsp.add("data3", Float.POSITIVE_INFINITY);
    w.write(buf, req, rsp);
    assertEquals(buf.toString(), "{'data1':float('NaN'),'data2':-float('Inf'),'data3':float('Inf')}");

    w = new RubyResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals(buf.toString(), "{'data1'=>(0.0/0.0),'data2'=>-(1.0/0.0),'data3'=>(1.0/0.0)}");

    w = new JSONResponseWriter();
    buf = new StringWriter();
    w.write(buf, req, rsp);
    assertEquals(buf.toString(), "{\"data1\":\"NaN\",\"data2\":\"-Infinity\",\"data3\":\"Infinity\"}");
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

    w.write(buf, req, rsp);
    assertEquals("{\"nl\":[[\"data1\",\"he\\u2028llo\\u2029!\"],[null,42]]}", buf.toString());
    req.close();
  }
  
}
