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
package org.apache.solr.handler;

import org.apache.solr.SolrTestCaseJ4;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.loader.XMLLoader;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.BufferingRequestProcessor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class XsltUpdateRequestHandlerTest extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeTests() throws Exception {
    initCore("solrconfig.xml","schema.xml");
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
      "  <node name=\"id\" value=\"12345\"/>" +
      "  <node name=\"name\" value=\"kitten\"/>" +
      "  <node name=\"text\" enhance=\"3\" value=\"some other day\"/>" +
      "  <node name=\"title\" enhance=\"4\" value=\"A story\"/>" +
      "  <node name=\"timestamp\" enhance=\"5\" value=\"2011-07-01T10:31:57.140Z\"/>" +
      " </document>" +
      "</random>";

    Map<String,String> args = new HashMap<>();
    args.put(CommonParams.TR, "xsl-update-handler-test.xsl");
      
    SolrCore core = h.getCore();
    LocalSolrQueryRequest req = new LocalSolrQueryRequest( core, new MapSolrParams( args) );
    ArrayList<ContentStream> streams = new ArrayList<>();
    streams.add(new ContentStreamBase.StringStream(xml));
    req.setContentStreams(streams);
    SolrQueryResponse rsp = new SolrQueryResponse();
    try (UpdateRequestHandler handler = new UpdateRequestHandler()) {
      handler.init(new NamedList<String>());
      handler.handleRequestBody(req, rsp);
    }
    StringWriter sw = new StringWriter(32000);
    QueryResponseWriter responseWriter = core.getQueryResponseWriter(req);
    responseWriter.write(sw,req,rsp);
    req.close();
    String response = sw.toString();
    assertU(response);
    assertU(commit());

    assertQ("test document was correctly committed", req("q","*:*")
            , "//result[@numFound='1']"
            , "//str[@name='id'][.='12345']"
        );
  }
  
  @Test
  public void testEntities() throws Exception
  {
    // use a binary file, so when it's loaded fail with XML eror:
    String file = getFile("mailing_lists.pdf").toURI().toASCIIString();
    String xml = 
      "<?xml version=\"1.0\"?>" +
      "<!DOCTYPE foo [" + 
      // check that external entities are not resolved!
      "<!ENTITY bar SYSTEM \""+file+"\">"+
      // but named entities should be
      "<!ENTITY wacky \"zzz\">"+
      "]>" +
      "<random>" +
      " &bar;" +
      " <document>" +
      "  <node name=\"id\" value=\"12345\"/>" +
      "  <node name=\"foo_s\" value=\"&wacky;\"/>" +
      " </document>" +
      "</random>";
    SolrQueryRequest req = req(CommonParams.TR, "xsl-update-handler-test.xsl");
    SolrQueryResponse rsp = new SolrQueryResponse();
    BufferingRequestProcessor p = new BufferingRequestProcessor(null);
    XMLLoader loader = new XMLLoader().init(null);
    loader.load(req, rsp, new ContentStreamBase.StringStream(xml), p);

    AddUpdateCommand add = p.addCommands.get(0);
    assertEquals("12345", add.solrDoc.getField("id").getFirstValue());
    assertEquals("zzz", add.solrDoc.getField("foo_s").getFirstValue());
    req.close();
  }  
}
