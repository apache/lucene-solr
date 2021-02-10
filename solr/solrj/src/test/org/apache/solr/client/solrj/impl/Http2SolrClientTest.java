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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.github.tomakehurst.wiremock.http.HttpHeaders;
import com.github.tomakehurst.wiremock.http.QueryParameter;
import com.github.tomakehurst.wiremock.http.RequestMethod;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getAllServeEvents;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.http.RequestMethod.GET;
import static com.github.tomakehurst.wiremock.http.RequestMethod.POST;
import static com.github.tomakehurst.wiremock.http.RequestMethod.PUT;
import static org.apache.solr.SolrTestCaseJ4.getHttpSolrClient;

public class Http2SolrClientTest extends BaseSolrClientWireMockTest {

  private static final String EXPECTED_USER_AGENT = "Solr[" + Http2SolrClient.class.getName() + "] 2.0";

  private Http2SolrClient getHttp2SolrClient(String url, int connectionTimeOut, int socketTimeout) {
    return new Http2SolrClient.Builder(url)
        .connectionTimeout(connectionTimeOut)
        .idleTimeout(socketTimeout)
        .build();
  }

  private Http2SolrClient getHttp2SolrClient(String url) {
    return new Http2SolrClient.Builder(url).build();
  }

  @Test
  @LuceneTestCase.Nightly // works but is slow due to timeout
  public void testTimeout() throws Exception {
    stubFor(get(urlPathEqualTo("/slow/foo/select"))
        .willReturn(aResponse().withFixedDelay(1050).withStatus(200)));

    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client = getHttp2SolrClient(mockSolr.baseUrl() + "/slow/foo", DEFAULT_CONNECTION_TIMEOUT, 1000)) {
      SolrServerException e = LuceneTestCase.expectThrows(SolrServerException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertTrue(e.getMessage().contains("Timeout"));
    }
  }

  @Test
  public void test0IdleTimeout() throws Exception {
    stubFor(get(urlPathEqualTo("/debug/foo/select"))
        .willReturn(aResponse().withFixedDelay(50).withStatus(200)));

    SolrQuery q = new SolrQuery("*:*");
    try (Http2SolrClient client = getHttp2SolrClient(mockSolr.baseUrl() + "/debug/foo", DEFAULT_CONNECTION_TIMEOUT, 0)) {
      try {
        client.query(q, SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
    }
  }

  /**
   * test that SolrExceptions thrown by HttpSolrClient can
   * correctly encapsulate http status codes even when not on the list of
   * ErrorCodes solr may return.
   */
  @Test
  public void testSolrExceptionCodeNotFromSolr() {
    final int status = 527;
    assertEquals(status + " didn't generate an UNKNOWN error code, someone modified the list of valid " +
            "ErrorCode's w/o changing this test to work a different way",
        SolrException.ErrorCode.UNKNOWN, SolrException.ErrorCode.getErrorCode(status));

    stubFor(get(urlPathEqualTo("/debug/foo/select")).willReturn(aResponse().withStatus(status)));

    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("foo");
      SolrException e = LuceneTestCase.expectThrows(SolrException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertEquals("Unexpected exception status code", status, e.code());
    }
  }

  @Test
  public void testQuery() throws Exception {
    stubFor(get(urlPathEqualTo("/debug/foo/select")).willReturn(aResponse().withStatus(500)));
    stubFor(post(urlPathEqualTo("/debug/foo/select")).willReturn(aResponse().withStatus(500)));
    stubFor(put(urlPathEqualTo("/debug/foo/select")).willReturn(aResponse().withStatus(500)));

    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl() + "/debug/foo")) {
      SolrQuery q = new SolrQuery("foo");
      q.setParam("a", "\u1234");

      // GET
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertQueryRequest(1, GET, null, client.getParser().getVersion(), "javabin");

      //POST
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.POST));
      assertQueryRequest(2, POST, "application/x-www-form-urlencoded; charset=UTF-8", client.getParser().getVersion(), "javabin");

      //PUT
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.PUT));
      assertQueryRequest(3, PUT, "application/x-www-form-urlencoded; charset=UTF-8", client.getParser().getVersion(), "javabin");

      //XML/GET
      client.setParser(new XMLResponseParser());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.GET));
      assertQueryRequest(4, GET, null, client.getParser().getVersion(), "xml");

      //XML/POST
      client.setParser(new XMLResponseParser());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.POST));
      assertQueryRequest(5, POST, "application/x-www-form-urlencoded; charset=UTF-8", client.getParser().getVersion(), "xml");

      client.setParser(new XMLResponseParser());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.query(q, SolrRequest.METHOD.PUT));
      assertQueryRequest(6, PUT, "application/x-www-form-urlencoded; charset=UTF-8", client.getParser().getVersion(), "xml");
    }
  }

  @Test
  public void testDelete() throws Exception {
    stubFor(post(urlPathEqualTo("/debug/foo/update")).willReturn(aResponse().withStatus(500)));

    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl() + "/debug/foo")) {
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.deleteById("id"));
      LoggedRequest req = assertRequest(1, POST, "application/javabin");
      assertRequestParam(req, CommonParams.WT, "javabin");
      assertRequestParam(req, CommonParams.VERSION, client.getParser().getVersion());

      //XML
      client.setParser(new XMLResponseParser());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.deleteByQuery("*:*"));
      req = assertRequest(2, POST, "application/javabin");
      assertRequestParam(req, CommonParams.WT, "xml");
      assertRequestParam(req, CommonParams.VERSION, client.getParser().getVersion());
    }
  }

  @Test
  public void testGetById() {
    // TODO: not really sure what is being tested here?
    stubFor(post(urlPathEqualTo("/debug/foo/select")).willReturn(aResponse().withStatus(500)));
    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl() + "/debug/foo")) {
      Collection<String> ids = Collections.singletonList("a");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("a"));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById(ids, null));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", "a"));
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.getById("foo", ids, null));
    }
  }

  @Test
  public void testUpdate() throws Exception {
    stubFor(post(urlPathEqualTo("/debug/foo/update")).willReturn(aResponse().withStatus(500)));

    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl() + "/debug/foo")) {
      UpdateRequest ureq = new UpdateRequest();
      ureq.add(new SolrInputDocument());
      ureq.setParam("a", "\u1234");
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));

      LoggedRequest req = assertRequest(1, POST, "application/javabin");
      assertRequestParam(req, CommonParams.WT, "javabin");
      assertRequestParam(req, CommonParams.VERSION, client.getParser().getVersion());
      assertRequestParam(req, "a", "\u1234");

      //XML response and writer
      client.setParser(new XMLResponseParser());
      client.setRequestWriter(new RequestWriter());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));
      req = assertRequest(2, POST, "application/xml; charset=UTF-8");
      assertRequestParam(req, CommonParams.WT, "xml");
      assertRequestParam(req, CommonParams.VERSION, client.getParser().getVersion());
      assertRequestParam(req, "a", "\u1234");

      //javabin request
      client.setParser(new BinaryResponseParser());
      client.setRequestWriter(new BinaryRequestWriter());
      LuceneTestCase.expectThrows(BaseHttpSolrClient.RemoteSolrException.class, () -> client.request(ureq));
      req = assertRequest(3, POST, "application/javabin");
      assertRequestParam(req, CommonParams.WT, "javabin");
      assertRequestParam(req, CommonParams.VERSION, client.getParser().getVersion());
      assertRequestParam(req, "a", "\u1234");
    }
  }

  @Test
  public void testRedirect() throws Exception {
    stubFor(get(urlPathEqualTo("/proxied/redirect/foo/select"))
        .willReturn(ok()
            .withHeader("Content-Type", RESPONSE_CONTENT_TYPE)
            .withBody(queryResponseOk())));

    stubFor(get(urlPathEqualTo("/redirect/foo/select"))
        .willReturn(aResponse().withStatus(302).withHeader("Location", mockSolr.baseUrl() + "/proxied/redirect/foo/select")));

    final String clientUrl = mockSolr.baseUrl() + "/redirect/foo";
    try (Http2SolrClient client = getHttpSolrClient(clientUrl)) {
      SolrQuery q = new SolrQuery("*:*");
      // default = false
      SolrException e = LuceneTestCase.expectThrows(SolrException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));

      client.setFollowRedirects(true);
      client.query(q);

      //And back again:
      client.setFollowRedirects(false);
      e = LuceneTestCase.expectThrows(SolrException.class, () -> client.query(q));
      assertTrue(e.getMessage().contains("redirect"));
    }

  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {
    stubFor(post(urlPathEqualTo("/wireMock/update"))
        .willReturn(ok().withBody(updateRequestOk()).withHeader("Content-Type", RESPONSE_CONTENT_TYPE)));

    stubFor(get(urlPathEqualTo("/wireMock/select"))
        .willReturn(ok().withHeader("Content-Type", RESPONSE_CONTENT_TYPE).withBody(queryResponseOk())));

    try (Http2SolrClient client = getHttpSolrClient(mockSolr.baseUrl())) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      client.add(BUILT_IN_MOCK_COLLECTION, doc);
      client.commit(BUILT_IN_MOCK_COLLECTION);

      assertEquals(1, client.query(BUILT_IN_MOCK_COLLECTION, new SolrQuery("id:collection")).getResults().getNumFound());
    }

    final String collection1Url = mockSolr.baseUrl() + "/wireMock";
    try (Http2SolrClient client = getHttpSolrClient(collection1Url)) {
      assertEquals(1, client.query(new SolrQuery("id:collection")).getResults().getNumFound());
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
        req.setParam(k, k + "Value");
      }
    }
  }

  private void verifyServletState(Http2SolrClient client, SolrRequest request) {

    List<ServeEvent> allServeEvents = getAllServeEvents();
    assertTrue(allServeEvents != null);
    ServeEvent e = allServeEvents.get(0); // works like a stack
    assertNotNull(e);
    LoggedRequest req = e.getRequest();
    assertNotNull(req);

    // check query String
    Iterator<String> paramNames = request.getParams().getParameterNamesIterator();
    while (paramNames.hasNext()) {
      String name = paramNames.next();
      String[] values = request.getParams().getParams(name);
      if (values != null) {
        for (String value : values) {
          boolean shouldBeInQueryString = client.getQueryParams().contains(name)
              || (request.getQueryParams() != null && request.getQueryParams().contains(name));

          // in either case, it should be in the parameters
          if (shouldBeInQueryString) {
            QueryParameter param = req.queryParameter(name);
            assertTrue(param.isPresent());
            assertEquals(1, param.values().size());
            assertEquals(value, param.firstValue());
          } else {
            assertTrue(!req.queryParameter(name).isPresent());
          }
        }
      }
    }
  }

  @Test
  public void testQueryString() throws Exception {
    stubFor(post(urlPathEqualTo("/debug/foo/update"))
        .willReturn(ok().withBody(updateRequestOk()).withHeader("Content-Type", RESPONSE_CONTENT_TYPE)));

    final String clientUrl = mockSolr.baseUrl() + "/debug/foo";
    try (Http2SolrClient client = getHttp2SolrClient(clientUrl)) {
      // test without request query params
      client.setQueryParams(setOf("serverOnly"));
      UpdateRequest req = new UpdateRequest();
      setReqParamsOf(req, "serverOnly", "notServer");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test without server query params
      client.setQueryParams(setOf());
      req = new UpdateRequest();
      req.setQueryParams(setOf("requestOnly"));
      setReqParamsOf(req, "requestOnly", "notRequest");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test with both request and server query params
      req = new UpdateRequest();
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      verifyServletState(client, req);

      // test with both request and server query params with single stream
      req = new UpdateRequest();
      req.add(new SolrInputDocument());
      client.setQueryParams(setOf("serverOnly", "both"));
      req.setQueryParams(setOf("requestOnly", "both"));
      setReqParamsOf(req, "serverOnly", "requestOnly", "both", "neither");
      try {
        client.request(req);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
      }
      // NOTE: single stream requests send all the params
      // as part of the query string.  So add "neither" to the request
      // so it passes the verification step.
      req.setQueryParams(setOf("requestOnly", "both", "neither"));
      verifyServletState(client, req);
    }
  }

  @Test
  public void testGetDefaultSslContextFactory() {
    assertNull(Http2SolrClient.getDefaultSslContextFactory().getEndpointIdentificationAlgorithm());

    System.setProperty("solr.jetty.ssl.verifyClientHostName", "HTTPS");
    SslContextFactory.Client sslContextFactory = Http2SolrClient.getDefaultSslContextFactory();
    assertEquals("HTTPS", sslContextFactory.getEndpointIdentificationAlgorithm());
    System.clearProperty("solr.jetty.ssl.verifyClientHostName");
  }

  private void assertRequestParam(LoggedRequest req, String paramName, String paramValue) {
    QueryParameter param = req.queryParameter(paramName);
    if (!param.isPresent() && !GET.equals(req.getMethod())) {
      // if not a GET, then try parsing the param from the body
      param = parseRequestParameterFromBody(req, paramName);
    }
    assertTrue(param != null && param.isPresent());
    assertTrue(param.values().size() == 1);
    assertEquals(paramValue, param.firstValue());
  }

  // parse a parameter value list from the request body
  private QueryParameter parseRequestParameterFromBody(LoggedRequest req, String paramName) {
    String requestBody = req.getBodyAsString();
    if (requestBody != null && (requestBody.contains("&") || requestBody.contains("="))) {
      List<String> valueList = new LinkedList<>();
      for (String pair : requestBody.split("&")) {
        final String[] values = pair.split("=");
        if (values.length == 2 && paramName.equals(values[0])) {
          try {
            valueList.add(URLDecoder.decode(values[1], StandardCharsets.UTF_8.name()));
          } catch (UnsupportedEncodingException e) {
            // ignore
          }
        }
      }
      return valueList.isEmpty() ? QueryParameter.absent(paramName) : new QueryParameter(paramName, valueList);
    }
    return QueryParameter.absent(paramName);
  }

  private LoggedRequest assertRequest(int reqCount, RequestMethod expRequestMethod, String expContentType) {
    List<ServeEvent> allServeEvents = getAllServeEvents();
    assertTrue(allServeEvents != null && allServeEvents.size() == reqCount);
    ServeEvent e = allServeEvents.get(0); // works like a stack
    assertNotNull(e);
    LoggedRequest req = e.getRequest();
    assertNotNull(req);
    assertEquals(expRequestMethod, req.getMethod());
    HttpHeaders headers = req.getHeaders();
    assertEquals(EXPECTED_USER_AGENT, headers.getHeader("user-agent").firstValue());

    if (expContentType != null) {
      assertTrue(expContentType.contains(headers.getHeader("Content-Type").firstValue()));
    } else {
      assertTrue(!headers.getHeader("Content-Type").isPresent());
    }
    return req;
  }

  private void assertQueryRequest(int reqCount, RequestMethod expRequestMethod, String expContentType, String expVersion, String expWriterType) {
    LoggedRequest req = assertRequest(reqCount, expRequestMethod, expContentType);
    assertRequestParam(req, CommonParams.WT, expWriterType);
    assertRequestParam(req, CommonParams.VERSION, expVersion);
    assertRequestParam(req, "a", "\u1234");
  }

  /**
   * Missed tests : see BasicHttpSolrClientTest
   * - set cookies via interceptor
   * - invariant params
   * - compression
   * - get raw stream
   */
}
