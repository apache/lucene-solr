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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class XsltUpdateRequestHandlerTest extends SolrTestCaseJ4 {
  protected static XsltUpdateRequestHandler handler;

  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
    handler = new XsltUpdateRequestHandler();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    clearIndex();
    assertU(commit());
  }

  @Test
  public void testUpdate() throws Exception
  {
    String xml = 
      "<random>" +
      " <document>" +
      "  <node name=\"id\" enhance=\"2.2\" value=\"12345\"/>" +
      "  <node name=\"name\" value=\"kitten\"/>" +
      "  <node name=\"text\" enhance=\"3\" value=\"some other day\"/>" +
      "  <node name=\"title\" enhance=\"4\" value=\"A story\"/>" +
      "  <node name=\"timestamp\" enhance=\"5\" value=\"2011-07-01T10:31:57.140Z\"/>" +
      " </document>" +
      "</random>";

	Map<String,String> args = new HashMap<String, String>();
	args.put("tr", "xsl-update-handler-test.xsl");
    
	SolrCore core = h.getCore();
	LocalSolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
	ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
	streams.add(new ContentStreamBase.StringStream(xml));
	req.setContentStreams(streams);
	SolrQueryResponse rsp = new SolrQueryResponse();
	XsltUpdateRequestHandler handler = new XsltUpdateRequestHandler();
	handler.init(new NamedList<String>());
	handler.handleRequestBody(req, rsp);
	StringWriter sw = new StringWriter(32000);
	QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
	responseWriter.write(sw,req,rsp);
	req.close();
	String response = sw.toString();
	assertU(response);
    assertU(commit());

    assertQ("test document was correctly committed", req("q","*:*")
            , "//result[@numFound='1']"
            , "//int[@name='id'][.='12345']"
    		);  
  }
}
