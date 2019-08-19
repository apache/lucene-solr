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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestWrapper;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.cookie.CookieSpec;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.HttpContext;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BasicHttpSolrClientTest extends SolrJettyTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
      } catch (InterruptedException ignored) {}
    }
  }
  
  public static class DebugServlet extends HttpServlet {
    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      queryString = null;
      cookies = null;
    }
    
    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String,String> headers = null;
    public static Map<String,String[]> parameters = null;
    public static String queryString = null;
    public static javax.servlet.http.Cookie[] cookies = null;
    
    public static void setErrorCode(Integer code) {
      errorCode = code;
    }
    
    @Override
    protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "delete";
      recordRequest(req, resp);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "get";
      recordRequest(req, resp);
    }
    
    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      lastMethod = "head";
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

    @SuppressForbidden(reason = "fake servlet only")
    private void setParameters(HttpServletRequest req) {
      parameters = req.getParameterMap();
    }

    private void setQueryString(HttpServletRequest req) {
      queryString = req.getQueryString();
    }

    private void setCookies(HttpServletRequest req) {
      javax.servlet.http.Cookie[] ck = req.getCookies();
      cookies = req.getCookies();
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
      setQueryString(req);
      setCookies(req);
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
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(RedirectServlet.class), "/redirect/*")
        .withServlet(new ServletHolder(SlowServlet.class), "/slow/*")
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }
  
  @Test
  public void testTimeout() throws Exception {
    SolrQuery q = new SolrQuery("*:*");
    try(HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 2000)) {
      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q, METHOD.GET));
      assertTrue(e.getMessage().contains("Timeout"));
    }

  }
  
  /**
   * test that SolrExceptions thrown by HttpSolrClient can
   * correctly encapsulate http status codes even when not on the list of
   * ErrorCodes solr may return.
   */
  public void testSolrExceptionCodeNotFromSolr() throws IOException, SolrServerException {
    final int status = 527;
    assertEquals(status + " didn't generate an UNKNOWN error code, someone modified the list of valid ErrorCode's w/o changing this test to work a different way",
        ErrorCode.UNKNOWN, ErrorCode.getErrorCode(status));

    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      DebugServlet.setErrorCode(status);
      SolrQuery q = new SolrQuery("foo");
      SolrException e = expectThrows(SolrException.class, () -> client.query(q, METHOD.GET));
      assertEquals("Unexpected exception status code", status, e.code());
    } finally {
      DebugServlet.clear();
    }
  }

  @Test
  public void testQuery() throws Exception {
    DebugServlet.clear();
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");
      expectThrows(ParseException.class, () -> client.query(q, METHOD.GET));

      //default method
      assertEquals("get", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      //content-type
      assertEquals(null, DebugServlet.headers.get("Content-Type"));
      //param encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //POST
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q, METHOD.POST));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      //PUT
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q, METHOD.PUT));

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      //XML/GET
      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q, METHOD.GET));

      assertEquals("get", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      //XML/POST
      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q, METHOD.POST));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));

      client.setParser(new XMLResponseParser());
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q, METHOD.PUT));

      assertEquals("put", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
      assertEquals("application/x-www-form-urlencoded; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
    }

  }

  @Test
  public void testDelete() throws Exception {
    DebugServlet.clear();
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      expectThrows(ParseException.class, () -> client.deleteById("id"));

      //default method
      assertEquals("post", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //keepalive
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));

      //XML
      client.setParser(new XMLResponseParser());
      expectThrows(ParseException.class, () -> client.deleteByQuery("*:*"));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals("keep-alive", DebugServlet.headers.get("Connection"));
    }

  }

  @Test
  public void testGetById() throws Exception {
    DebugServlet.clear();
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      Collection<String> ids = Collections.singletonList("a");
      expectThrows(ParseException.class, () -> client.getById("a"));
      expectThrows(ParseException.class, () -> client.getById(ids, null));
      expectThrows(ParseException.class, () -> client.getById("foo", "a"));
      expectThrows(ParseException.class, () -> client.getById("foo", ids, null));
    }
  }

  @Test
  public void testUpdate() throws Exception {
    DebugServlet.clear();
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/debug/foo")) {
      UpdateRequest req = new UpdateRequest();
      req.add(new SolrInputDocument());
      req.setParam("a", "\u1234");
      expectThrows(ParseException.class, () -> client.request(req));

      //default method
      assertEquals("post", DebugServlet.lastMethod);
      //agent
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      //default wt
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      //default version
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      //content type
      assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
      //parameter encoding
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //XML response and writer
      client.setParser(new XMLResponseParser());
      client.setRequestWriter(new RequestWriter());
      expectThrows(ParseException.class, () -> client.request(req));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("xml", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/xml; charset=UTF-8", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);

      //javabin request
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.request(req));

      assertEquals("post", DebugServlet.lastMethod);
      assertEquals("Solr[" + HttpSolrClient.class.getName() + "] 1.0", DebugServlet.headers.get("User-Agent"));
      assertEquals(1, DebugServlet.parameters.get(CommonParams.WT).length);
      assertEquals("javabin", DebugServlet.parameters.get(CommonParams.WT)[0]);
      assertEquals(1, DebugServlet.parameters.get(CommonParams.VERSION).length);
      assertEquals(client.getParser().getVersion(), DebugServlet.parameters.get(CommonParams.VERSION)[0]);
      assertEquals("application/javabin", DebugServlet.headers.get("Content-Type"));
      assertEquals(1, DebugServlet.parameters.get("a").length);
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
    }

  }
  
  @Test
  public void testRedirect() throws Exception {
    final String clientUrl = jetty.getBaseUrl().toString() + "/redirect/foo";
    try (HttpSolrClient client = getHttpSolrClient(clientUrl)) {
      SolrQuery q = new SolrQuery("*:*");
      // default = false
      SolrServerException e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));

      client.setFollowRedirects(true);
      client.query(q);

      //And back again:
      client.setFollowRedirects(false);
      e = expectThrows(SolrServerException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

  }
  
  @Test
  public void testCompression() throws Exception {
    final SolrQuery q = new SolrQuery("*:*");
    
    final String clientUrl = jetty.getBaseUrl().toString() + "/debug/foo";
    try (HttpSolrClient client = getHttpSolrClient(clientUrl)) {
      // verify request header gets set
      DebugServlet.clear();
      expectThrows(ParseException.class, () -> client.query(q));
      assertNull(DebugServlet.headers.toString(), DebugServlet.headers.get("Accept-Encoding"));
    }
    
    try (HttpSolrClient client = getHttpSolrClient(clientUrl, null, null, true)) {
      try {
        client.query(q);
      } catch (ParseException ignored) {}
      assertNotNull(DebugServlet.headers.get("Accept-Encoding"));
    }
    
    try (HttpSolrClient client = getHttpSolrClient(clientUrl, null, null, false)) {
      try {
        client.query(q);
      } catch (ParseException ignored) {}
    }
    assertNull(DebugServlet.headers.get("Accept-Encoding"));
    
    // verify server compresses output
    HttpGet get = new HttpGet(jetty.getBaseUrl().toString() + "/collection1" +
                              "/select?q=foo&wt=xml");
    get.setHeader("Accept-Encoding", "gzip");
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_ALLOW_COMPRESSION, true);
    
    RequestConfig config = RequestConfig.custom().setDecompressionEnabled(false).build();   
    get.setConfig(config);
    
    CloseableHttpClient httpclient = HttpClientUtil.createClient(params);
    HttpEntity entity = null;
    try {
      HttpResponse response = httpclient.execute(get, HttpClientUtil.createNewHttpClientRequestContext());
      entity = response.getEntity();
      Header ceheader = entity.getContentEncoding();
      assertNotNull(Arrays.asList(response.getAllHeaders()).toString(), ceheader);
      assertEquals("gzip", ceheader.getValue());
    } finally {
      if (entity != null) {
        entity.getContent().close();
      }
      HttpClientUtil.close(httpclient);
    }
    
    // verify compressed response can be handled
    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/collection1")) {
      QueryResponse response = client.query(new SolrQuery("foo"));
      assertEquals(0, response.getStatus());
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    try (HttpSolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString())) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      client.add("collection1", doc);
      client.commit("collection1");

      assertEquals(1, client.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());
    }

    final String collection1Url = jetty.getBaseUrl().toString() + "/collection1";
    try (HttpSolrClient client = getHttpSolrClient(collection1Url)) {
      assertEquals(1, client.query(new SolrQuery("id:collection")).getResults().getNumFound());
    }

  }

  @Test
  public void testGetRawStream() throws SolrServerException, IOException{
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    try {
      HttpSolrClient solrClient = getHttpSolrClient(jetty.getBaseUrl().toString() + "/collection1",
          client, null);
      QueryRequest req = new QueryRequest();
      NamedList response = solrClient.request(req);
      InputStream stream = (InputStream) response.get("stream");
      assertNotNull(stream);
      stream.close();
    } finally {
      HttpClientUtil.close(client);;
    }
  }

  /**
   * An interceptor changing the request
   */
  HttpRequestInterceptor changeRequestInterceptor = new HttpRequestInterceptor() {

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException,
    IOException {
      log.info("Intercepted params: "+context);

      HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
      URIBuilder uribuilder = new URIBuilder(wrapper.getURI());
      uribuilder.addParameter("b", "\u4321");
      try {
        wrapper.setURI(uribuilder.build());
      } catch (URISyntaxException ex) {
        throw new HttpException("Invalid request URI", ex);
      }
    }
  };

  public static final String cookieName = "cookieName";
  public static final String cookieValue = "cookieValue";

  /**
   * An interceptor setting a cookie
   */
  HttpRequestInterceptor cookieSettingRequestInterceptor = new HttpRequestInterceptor() {    
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException,
    IOException {
      BasicClientCookie cookie = new BasicClientCookie(cookieName, cookieValue);
      cookie.setVersion(0);
      cookie.setPath("/");
      cookie.setDomain(jetty.getBaseUrl().getHost());

      CookieStore cookieStore = new BasicCookieStore();
      CookieSpec cookieSpec = new SolrPortAwareCookieSpecFactory().create(context);
     // CookieSpec cookieSpec = registry.lookup(policy).create(context);
      // Add the cookies to the request
      List<Header> headers = cookieSpec.formatCookies(Collections.singletonList(cookie));
      for (Header header : headers) {
        request.addHeader(header);
      }
      context.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);
    }
  };


  /**
   * Set cookies via interceptor
   * Change the request via an interceptor
   * Ensure cookies are actually set and that request is actually changed
   */
  @Test
  public void testInterceptors() {
    DebugServlet.clear();
    HttpClientUtil.addRequestInterceptor(changeRequestInterceptor);
    HttpClientUtil.addRequestInterceptor(cookieSettingRequestInterceptor);    

    final String clientUrl = jetty.getBaseUrl().toString() + "/debug/foo";
    try(HttpSolrClient server = getHttpSolrClient(clientUrl)) {

      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");
      expectThrows(Exception.class, () -> server.query(q, random().nextBoolean()?METHOD.POST:METHOD.GET));

      // Assert cookies from UseContextCallback 
      assertNotNull(DebugServlet.cookies);
      boolean foundCookie = false;
      for (javax.servlet.http.Cookie cookie : DebugServlet.cookies) {
        if (cookieName.equals(cookie.getName())
            && cookieValue.equals(cookie.getValue())) {
          foundCookie = true;
          break;
        }
      }
      assertTrue(foundCookie);

      // Assert request changes by ChangeRequestCallback
      assertEquals("\u1234", DebugServlet.parameters.get("a")[0]);
      assertEquals("\u4321", DebugServlet.parameters.get("b")[0]);

    } catch (IOException ex) {
      throw new RuntimeException(ex);
    } finally {
      HttpClientUtil.removeRequestInterceptor(changeRequestInterceptor);
      HttpClientUtil.removeRequestInterceptor(cookieSettingRequestInterceptor);    
    }
  }

  private Set<String> setOf(String... keys) {
    Set<String> set = new TreeSet<>();
    if (keys != null) {
      Collections.addAll(set, keys);
    }
    return set;
  }

  private void setReqParamsOf(UpdateRequest req, String... keys) {
    if (keys != null) {
      for (String k : keys) {
        req.setParam(k, k+"Value");
      }
    }
  }

  private void verifyServletState(HttpSolrClient client, SolrRequest request) {
    // check query String
    Iterator<String> paramNames = request.getParams().getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String name = paramNames.next();
      String [] values = request.getParams().getParams(name);
      if (values != null) {
        for (String value : values) {
          boolean shouldBeInQueryString = client.getQueryParams().contains(name)
            || (request.getQueryParams() != null && request.getQueryParams().contains(name));
          assertEquals(shouldBeInQueryString, DebugServlet.queryString.contains(name + "=" + value));
          // in either case, it should be in the parameters
          assertNotNull(DebugServlet.parameters.get(name));
          assertEquals(1, DebugServlet.parameters.get(name).length);
          assertEquals(value, DebugServlet.parameters.get(name)[0]);
        }
      }
    }
  }

  @Test
  public void testQueryString() throws Exception {

    final String clientUrl = jetty.getBaseUrl().toString() + "/debug/foo";
    try(HttpSolrClient client = getHttpSolrClient(clientUrl)) {
      // test without request query params
      DebugServlet.clear();
      client.setQueryParams(setOf("serverOnly"));
      UpdateRequest req = new UpdateRequest();
      setReqParamsOf(req, "serverOnly", "notServer");
      expectThrows(ParseException.class, () -> client.request(req));
      verifyServletState(client, req);
  
      // test without server query params
      DebugServlet.clear();
      client.setQueryParams(setOf());
      UpdateRequest req2 = new UpdateRequest();
      req2.setQueryParams(setOf("requestOnly"));
      setReqParamsOf(req2, "requestOnly", "notRequest");
      expectThrows(ParseException.class, () -> client.request(req2));
      verifyServletState(client, req2);
  
      // test with both request and server query params
      DebugServlet.clear();
      UpdateRequest req3 = new UpdateRequest();
      client.setQueryParams(setOf("serverOnly", "both"));
      req3.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req3, "serverOnly", "requestOnly", "both", "neither");
      expectThrows(ParseException.class, () -> client.request(req3));
      verifyServletState(client, req3);
  
      // test with both request and server query params with single stream
      DebugServlet.clear();
      UpdateRequest req4 = new UpdateRequest();
      req4.add(new SolrInputDocument());
      client.setQueryParams(setOf("serverOnly", "both"));
      req4.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req4, "serverOnly", "requestOnly", "both", "neither");
      expectThrows(ParseException.class, () -> client.request(req4));
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request
      // so it passes the verification step.
      req4.setQueryParams(setOf("requestOnly", "both", "neither"));
      verifyServletState(client, req4);
    }
  }

  @Test
  public void testInvariantParams() throws IOException {
    try(HttpSolrClient createdClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl(jetty.getBaseUrl().toString())
        .withInvariantParams(SolrTestCaseJ4.params("param", "value"))
        .build()) {
      assertEquals("value", createdClient.getInvariantParams().get("param"));
    }

    try(HttpSolrClient createdClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl(jetty.getBaseUrl().toString())
        .withInvariantParams(SolrTestCaseJ4.params("fq", "fq1", "fq", "fq2"))
        .build()) {
      assertEquals(2, createdClient.getInvariantParams().getParams("fq").length);
    }

    try(HttpSolrClient createdClient = new HttpSolrClient.Builder()
        .withBaseSolrUrl(jetty.getBaseUrl().toString())
        .withKerberosDelegationToken("mydt")
        .withInvariantParams(SolrTestCaseJ4.params(DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM, "mydt"))
        .build()) {
      fail();
    } catch(Exception ex) {
      if (!ex.getMessage().equals("parameter "+ DelegationTokenHttpSolrClient.DELEGATION_TOKEN_PARAM +" is redefined.")) {
        throw ex;
      }
    }
  }
}
