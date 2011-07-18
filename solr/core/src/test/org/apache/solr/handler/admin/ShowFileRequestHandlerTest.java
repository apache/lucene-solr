package org.apache.solr.handler.admin;

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

import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Extend SolrJettyTestBase because the SOLR-2535 bug only manifested itself when
 * the {@link SolrDispatchFilter} is used, which isn't for embedded Solr use.
 */
public class ShowFileRequestHandlerTest extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
  }

  public void testDirList() throws SolrServerException {
    SolrServer server = getSolrServer();
    //assertQ(req("qt", "/admin/file")); TODO file bug that SolrJettyTestBase extends SolrTestCaseJ4
    QueryRequest request = new QueryRequest();
    request.setPath("/admin/file");
    QueryResponse resp = request.process(server);
    assertEquals(0,resp.getStatus());
    assertTrue(((NamedList) resp.getResponse().get("files")).size() > 0);//some files
  }

  public void testGetRawFile() throws SolrServerException, IOException {
    SolrServer server = getSolrServer();
    //assertQ(req("qt", "/admin/file")); TODO file bug that SolrJettyTestBase extends SolrTestCaseJ4
    QueryRequest request = new QueryRequest(params("file","schema.xml"));
    request.setPath("/admin/file");
    final AtomicBoolean readFile = new AtomicBoolean();
    request.setResponseParser(new ResponseParser() {
      @Override
      public String getWriterType() {
        return "mock";//unfortunately this gets put onto params wt=mock but it apparently has no effect
      }

      @Override
      public NamedList<Object> processResponse(InputStream body, String encoding) {
        try {
          if (body.read() >= 0)
            readFile.set(true);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        return null;
      }

      @Override
      public NamedList<Object> processResponse(Reader reader) {
        throw new UnsupportedOperationException("TODO unimplemented");//TODO
      }
    });

    server.request( request );//runs request
    //request.process(server); but we don't have a NamedList response
    assertTrue(readFile.get());
  }
}
