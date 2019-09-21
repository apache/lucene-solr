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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.BeforeClass;

/**
 * Extend SolrJettyTestBase because the SOLR-2535 bug only manifested itself when
 * the {@link org.apache.solr.servlet.SolrDispatchFilter} is used, which isn't for embedded Solr use.
 */
public class ShowFileRequestHandlerTest extends SolrJettyTestBase {

  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
  }

  public void test404ViaHttp() throws Exception {
    SolrClient client = getSolrClient();
    QueryRequest request = new QueryRequest(params("file",
                                                   "does-not-exist-404.txt"));
    request.setPath("/admin/file");
    SolrException e = expectThrows(SolrException.class, () -> request.process(client));
    assertEquals(404, e.code());
  }

  public void test404Locally() throws Exception {

    // we need to test that executing the handler directly does not 
    // throw an exception, just sets the exception on the response.
    initCore("solrconfig.xml", "schema.xml");

    // bypass TestHarness since it will throw any exception found in the
    // response.
    SolrCore core = h.getCore();
    SolrQueryResponse rsp = new SolrQueryResponse();
    core.execute(core.getRequestHandler("/admin/file"),
        req("file", "does-not-exist-404.txt"), rsp);
    assertNotNull("no exception in response", rsp.getException());
    assertTrue("wrong type of exception: " + rsp.getException().getClass(),
        rsp.getException() instanceof SolrException);
    assertEquals(404, ((SolrException)rsp.getException()).code());
  }

  public void testDirList() throws SolrServerException, IOException {
    SolrClient client = getSolrClient();
    //assertQ(req("qt", "/admin/file")); TODO file bug that SolrJettyTestBase extends SolrTestCaseJ4
    QueryRequest request = new QueryRequest();
    request.setPath("/admin/file");
    QueryResponse resp = request.process(client);
    assertEquals(0,resp.getStatus());
    assertTrue(((NamedList) resp.getResponse().get("files")).size() > 0);//some files
  }

  public void testGetRawFile() throws SolrServerException, IOException {
    SolrClient client = getSolrClient();
    //assertQ(req("qt", "/admin/file")); TODO file bug that SolrJettyTestBase extends SolrTestCaseJ4
    QueryRequest request = new QueryRequest(params("file", "managed-schema"));
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

    client.request(request);//runs request
    //request.process(client); but we don't have a NamedList response
    assertTrue(readFile.get());
  }
}
