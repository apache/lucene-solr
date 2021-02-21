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
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.METHOD;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// A number of tests from this class have been moved over to Http2SolrClientTest
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
        Thread.sleep(TEST_NIGHTLY ? 5000 : 0);
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
  
  @Before
  public void setUp() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(RedirectServlet.class), "/redirect/*")
        .withServlet(new ServletHolder(SlowServlet.class), "/slow/*")
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    jetty = createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
    super.setUp();
  }
  
  @Test
  @Ignore // MRM TODO: changed for http2
  public void testCompression() throws Exception {
    final SolrQuery q = new SolrQuery("*:*");
    
    final String clientUrl = jetty.getBaseUrl().toString() + "/debug/foo";
    try (Http2SolrClient client = getHttpSolrClient(clientUrl)) {
      // verify request header gets set
      DebugServlet.clear();
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q));
      assertNull(DebugServlet.headers.toString(), DebugServlet.headers.get("Accept-Encoding"));
    }
    
    try (Http2SolrClient client = getHttpSolrClient(clientUrl, null, null, true)) {
      try {
        client.query(q);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {}
      assertNotNull(DebugServlet.headers.get("Accept-Encoding"));
    }
    
    try (Http2SolrClient client = getHttpSolrClient(clientUrl, null, null, false)) {
      try {
        client.query(q);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {}
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
    try (Http2SolrClient client = getHttpSolrClient(jetty.getBaseUrl().toString() + "/collection1")) {
      QueryResponse response = client.query(new SolrQuery("foo"));
      assertEquals(0, response.getStatus());
    }
  }

  @Test
  @Ignore // MRM TODO: flakey
  public void testGetRawStream() throws SolrServerException, IOException{
    CloseableHttpClient client = HttpClientUtil.createClient(null);
    try {
      Http2SolrClient solrClient = getHttpSolrClient(jetty.getBaseUrl().toString() + "/collection1");
      QueryRequest req = new QueryRequest();
      NamedList response = solrClient.request(req);
      InputStream stream = (InputStream) response.get("stream");
      assertNotNull(stream);
      while (stream.read() != -1) {}
    } finally {
      HttpClientUtil.close(client);;
    }
  }

  /**
   * An interceptor changing the request
   */
  HttpRequestInterceptor changeRequestInterceptor = new MyHttpRequestInterceptor2();

  public static final String cookieName = "cookieName";
  public static final String cookieValue = "cookieValue";

  /**
   * An interceptor setting a cookie
   */
  HttpRequestInterceptor cookieSettingRequestInterceptor = new MyHttpRequestInterceptor();


  /**
   * Set cookies via interceptor
   * Change the request via an interceptor
   * Ensure cookies are actually set and that request is actually changed
   */
  @Test
  @Ignore // MRM TODO: changed for http2
  public void testInterceptors() {
    DebugServlet.clear();
    HttpClientUtil.addRequestInterceptor(changeRequestInterceptor);
    HttpClientUtil.addRequestInterceptor(cookieSettingRequestInterceptor);    

    final String clientUrl = jetty.getBaseUrl().toString() + "/debug/foo";
    try(Http2SolrClient server = getHttpSolrClient(clientUrl)) {

      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");
      LuceneTestCase.expectThrows(Exception.class, () -> server.query(q, random().nextBoolean()?METHOD.POST:METHOD.GET));

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

    } catch (Exception ex) {
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

  private void verifyServletState(Http2SolrClient client, SolrRequest request) {
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

  private class MyHttpRequestInterceptor implements HttpRequestInterceptor {
    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      BasicClientCookie cookie = new BasicClientCookie(cookieName, cookieValue);
      cookie.setVersion(0);
      cookie.setPath("/");
      cookie.setDomain(jetty.getHost());

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
  }

  private static class MyHttpRequestInterceptor2 implements HttpRequestInterceptor {

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      log.info("Intercepted params: {}", context);

      HttpRequestWrapper wrapper = (HttpRequestWrapper) request;
      URIBuilder uribuilder = new URIBuilder(wrapper.getURI());
      uribuilder.addParameter("b", "\u4321");
      try {
        wrapper.setURI(uribuilder.build());
      } catch (URISyntaxException ex) {
        throw new HttpException("Invalid request URI", ex);
      }
    }
  }
}
