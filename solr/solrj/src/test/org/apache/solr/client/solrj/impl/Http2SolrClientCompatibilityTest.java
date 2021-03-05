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

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.util.LogLevel;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.http2.client.http.HttpClientTransportOverHTTP2;
import org.eclipse.jetty.servlet.ServletHolder;

@LogLevel("org.eclipse.jetty.client=DEBUG;org.eclipse.jetty.util=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class Http2SolrClientCompatibilityTest extends SolrJettyTestBase {

  public void testSystemPropertyFlag() {
    System.setProperty("solr.http1", "true");
    try (Http2SolrClient client = new Http2SolrClient.Builder()
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
    }
    System.clearProperty("solr.http1");
    try (Http2SolrClient client = new Http2SolrClient.Builder()
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP2);
    }
  }

  public void testConnectToOldNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(Http2SolrClientTest.DebugServlet.class), "/debug/*")
        .useOnlyHttp1(true)
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo")
        .useHttp1_1(true)
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {}
    } finally {
      afterSolrJettyTestBase();
    }
  }

  public void testConnectToNewNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(Http2SolrClientTest.DebugServlet.class), "/debug/*")
        .useOnlyHttp1(false)
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo")
        .useHttp1_1(true)
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {}
    } finally {
      afterSolrJettyTestBase();
    }
  }

  public void testConnectToOldNodesUsingHttp2() throws Exception {
    // if this test some how failure, this mean that Jetty client now be able to switch between HTTP/1
    // and HTTP/2.2 protocol dynamically therefore rolling updates will be easier we should then notify this to users
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(Http2SolrClientTest.DebugServlet.class), "/debug/*")
        .useOnlyHttp1(true)
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    System.clearProperty("solr.http1");
    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo")
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP2);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
        fail("Jetty client with HTTP2 transport should not be able to connect to HTTP1 only nodes");
      } catch (BaseHttpSolrClient.RemoteSolrException ignored) {
        fail("Jetty client with HTTP2 transport should not be able to connect to HTTP1 only nodes");
      } catch (SolrServerException e) {
        // expected
      }
    } finally {
      afterSolrJettyTestBase();
    }
  }
}
