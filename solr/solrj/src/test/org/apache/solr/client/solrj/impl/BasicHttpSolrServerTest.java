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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.Socket;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SSLTestConfig;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicHttpSolrServerTest extends SolrJettyTestBase {
  
  public static class RedirectServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      resp.sendRedirect("/solr/collection1/select?" + req.getQueryString());
    }
  }
  
  public static class SlowServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {}
    }
  }
  
  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
    }
    
    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String,String> headers = null;
    public static Map<String,String[]> parameters = null;
    
    public static void setErrorCode(Integer code) {
      errorCode = code;
    }
    

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "get";
      recordRequest(req, resp);
    }
    
    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name, req.getHeader(name));
      }
    }

    private void setParameters(HttpServletRequest req) {
      parameters = req.getParameterMap();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "post";
      recordRequest(req, resp);
    }

    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "put";
      recordRequest(req, resp);
    }
    
    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      setParameters(req);
      if (null != errorCode) {
        try { 
          resp.sendError(errorCode); 
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in DebugServlet", e);
        }
      }
    }
  }
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(ExternalPaths.EXAMPLE_HOME, null, null);
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(RedirectServlet.class, "/redirect/*");
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(SlowServlet.class, "/slow/*");
    jetty.getDispatchFilter().getServletHandler()
        .addServletWithMapping(DebugServlet.class, "/debug/*");
  }
  
  @Test
  public void testTimeout() throws Exception {
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/slow/foo");
    SolrQuery q = new SolrQuery("*:*");
    server.setSoTimeout(2000);
    try {
      QueryResponse response = server.query(q, METHOD.GET);
      fail("No exception thrown.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("Timeout"));
    }
    server.shutdown();
  }
  
  /**
   * test that SolrExceptions thrown by HttpSolrServer can
   * correctly encapsulate http status codes even when not on the list of
   * ErrorCodes solr may return.
   */
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(status + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
                 ErrorCode.UNKNOWN, ErrorCode.getErrorCode(status));

    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/debug/foo");
    try {
      DebugServlet.setErrorCode(status);
      try {
        SolrQuery q = new SolrQuery("foo");
        server.query(q, METHOD.GET);
        fail("Didn't get excepted exception from oversided request");
      } catch (SolrException e) {
        System.out.println(e);
        assertEquals("Unexpected exception status code", status, e.code());
      }
    } finally {
      server.shutdown();
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery(){
    DebugServlet.clear();
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/debug/foo");
    SolrQuery q = new SolrQuery("foo");
    q.setParam("a", "\u1234");
    try {
      server.query(q, METHOD.GET);
    } catch (Throwable t) {}
    
    //default method
    assertEquals("get", DebugServlet.lastMethod);
    //agent
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    //default wt
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    //default version
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    //agent
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    //keepalive
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    //content-type
    assertEquals(null, DebugServlet.headers.get("Content-Type"));
    //param encoding
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

    //POST
    DebugServlet.clear();
    try {
      server.query(q, METHOD.POST);
    } catch (Throwable t) {}
    assertEquals("post", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

    //PUT
    DebugServlet.clear();
    try {
      server.query(q, METHOD.PUT);
    } catch (Throwable t) {}
    assertEquals("put", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

    //XML/GET
    server.setParser(new XMLResponseParser());
    DebugServlet.clear();
    try {
      server.query(q, METHOD.GET);
    } catch (Throwable t) {}
    assertEquals("get", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

    //XML/POST
    server.setParser(new XMLResponseParser());
    DebugServlet.clear();
    try {
      server.query(q, METHOD.POST);
    } catch (Throwable t) {}
    assertEquals("post", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

    server.setParser(new XMLResponseParser());
    DebugServlet.clear();
    try {
      server.query(q, METHOD.PUT);
    } catch (Throwable t) {}
    assertEquals("put", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
    server.shutdown();
  }

  @Test
  public void testDelete(){
    DebugServlet.clear();
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/debug/foo");
    try {
      server.deleteById("id");
    } catch (Throwable t) {}
    
    //default method
    assertEquals("post", DebugServlet.lastMethod);
    //agent
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    //default wt
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    //default version
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    //agent
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    //keepalive
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

    //XML
    server.setParser(new XMLResponseParser());
    try {
      server.deleteByQuery("*:*");
    } catch (Throwable t) {}
    
    assertEquals("post", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    server.shutdown();
  }
  
  @Test
  public void testUpdate(){
    DebugServlet.clear();
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() + 
                                               "/debug/foo");
    UpdateRequest req = new UpdateRequest();
    req.add(new SolrInputDocument());
    req.setParam("a", "\u1234");
    try {
      server.request(req);
    } catch (Throwable t) {}
    
    //default method
    assertEquals("post", DebugServlet.lastMethod);
    //agent
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    //default wt
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    //default version
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    //content type
    assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
    //parameter encoding
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

    //XML response
    server.setParser(new XMLResponseParser());
    try {
      server.request(req);
    } catch (Throwable t) {}
    assertEquals("post", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    
    //javabin request
    server.setParser(new BinaryResponseParser());
    server.setRequestWriter(new BinaryRequestWriter());
    DebugServlet.clear();
    try {
      server.request(req);
    } catch (Throwable t) {}
    assertEquals("post", DebugServlet.lastMethod);
    assertEquals("Solr[" + org.apache.solr.client.solrj.impl.HttpSolrServer.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
    assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
    assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
    assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
    assertEquals(server.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
    assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
    assertEquals(1, DebugServlet.parameters.get("a").length);
    assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    server.shutdown();
  }
  
  @Test
  public void testRedirect() throws Exception {
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/redirect/foo");
    SolrQuery q = new SolrQuery("*:*");
    // default = false
    try {
      QueryResponse response = server.query(q);
      fail("Should have thrown an exception.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("redirect"));
    }
    server.setFollowRedirects(true);
    try {
      QueryResponse response = server.query(q);
    } catch (Throwable t) {
      fail("Exception was thrown:" + t);
    }
    //And back again:
    server.setFollowRedirects(false);
    try {
      QueryResponse response = server.query(q);
      fail("Should have thrown an exception.");
    } catch (SolrServerException e) {
      assertTrue(e.getMessage().contains("redirect"));
    }
    server.shutdown();
  }
  
  @Test
  public void testCompression() throws Exception {
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() +
                                               "/debug/foo");
    SolrQuery q = new SolrQuery("*:*");
    
    // verify request header gets set
    DebugServlet.clear();
    try {
      server.query(q);
    } catch (Throwable t) {}
    assertNull(DebugServlet.headers.get("Accept-Encoding"));
    server.setAllowCompression(true);
    try {
      server.query(q);
    } catch (Throwable t) {}
    assertNotNull(DebugServlet.headers.get("Accept-Encoding"));
    server.setAllowCompression(false);
    try {
      server.query(q);
    } catch (Throwable t) {}
    assertNull(DebugServlet.headers.get("Accept-Encoding"));
    
    // verify server compresses output
    HttpGet get = new HttpGet(jetty.getBaseUrl().toString() + "/collection1" +
                              "/select?q=foo&wt=xml");
    get.setHeader("Accept-Encoding", "gzip");
    HttpClient client = HttpClientUtil.createClient(null);
    HttpEntity entity = null;
    try {
      HttpResponse response = client.execute(get);
      entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      assertEquals("gzip", ceheader.getValue());
      
    } finally {
      if(entity!=null) {
        entity.getContent().close();
      }
      client.getConnectionManager().shutdown();
    }
    
    // verify compressed response can be handled
    server = new HttpSolrServer(jetty.getBaseUrl().toString() + "/collection1");
    server.setAllowCompression(true);
    q = new SolrQuery("foo");
    QueryResponse response = server.query(q);
    assertEquals(0, response.getStatus());
    server.shutdown();
  }
  
  @Test
  public void testSetParametersExternalClient(){
    HttpClient client = HttpClientUtil.createClient(null);
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString(), 
                                               client);
    try {
      server.setMaxTotalConnections(1);
      fail("Operation should not succeed.");
    } catch (UnsupportedOperationException e) {}
    try {
      server.setDefaultMaxConnectionsPerHost(1);
      fail("Operation should not succeed.");
    } catch (UnsupportedOperationException e) {}
    server.shutdown();
    client.getConnectionManager().shutdown();
  }

  @Test
  public void testGetRawStream() throws SolrServerException, IOException{
    HttpClient client = HttpClientUtil.createClient(null);
    HttpSolrServer server = new HttpSolrServer(jetty.getBaseUrl().toString() + "/collection1", 
                                               client, null);
    QueryRequest req = new QueryRequest();
    NamedList response = server.request(req);
    InputStream stream = (InputStream)response.get("stream");
    assertNotNull(stream);
    stream.close();
    client.getConnectionManager().shutdown();
  }

  /**
   * A trivial test that verifies the example keystore used for SSL testing can be 
   * found using the base class. this helps future-proof against the possibility of 
   * something moving/breaking the keystore path in a way that results in the SSL 
   * randomization logic being forced to silently never use SSL.  (We can't enforce 
   * this type of check in the base class because then it would not be usable by client 
   * code depending on the test framework
   */
  public void testExampleKeystorePath() {
    assertNotNull("Example keystore is null, meaning that something has changed in the " +
                  "structure of the example configs and/or ExternalPaths.java - " + 
                  "SSL randomization is broken",
                  SSLTestConfig.TEST_KEYSTORE);
  }


  private int findUnusedPort() {
    for (int port = 0; port < 65535; port++) {
      Socket s = new Socket();
      try {
        s.bind(null);
        int availablePort = s.getLocalPort();
        s.close();
        return availablePort;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    throw new RuntimeException("Could not find unused TCP port.");
  }
  
}
