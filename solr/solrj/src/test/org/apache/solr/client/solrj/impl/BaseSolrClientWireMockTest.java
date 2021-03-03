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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.JettySettings;
import com.github.tomakehurst.wiremock.core.Options;
import com.github.tomakehurst.wiremock.http.AdminRequestHandler;
import com.github.tomakehurst.wiremock.http.HttpServer;
import com.github.tomakehurst.wiremock.http.HttpServerFactory;
import com.github.tomakehurst.wiremock.http.StubRequestHandler;
import com.github.tomakehurst.wiremock.jetty94.Jetty94HttpServer;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.MockZkStateReader;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SolrQueuedThreadPool;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.io.NetworkTrafficListener;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.configureFor;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

@ThreadLeakFilters(filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class,
    BaseSolrClientWireMockTest.WireMockThreadFilter.class
})
public abstract class BaseSolrClientWireMockTest extends SolrTestCase {

  // since Solr's test framework randomizes the Locale, need to override WireMockServer baseUrl() method
  // to use the Locale.US for formatting the URL
  static class USLocaleWireMockServer extends WireMockServer {
    USLocaleWireMockServer(Options options) {
      super(options);
    }

    @Override
    public String baseUrl() {
      boolean https = options.httpsSettings().enabled();
      String protocol = https ? "https" : "http";
      int port = https ? httpsPort() : port();

      return String.format(Locale.US, "%s://localhost:%d", protocol, port);
    }
  }

  static class JettyHttp2ServerFactory implements HttpServerFactory {
    @Override
    public HttpServer buildHttpServer(Options options, AdminRequestHandler adminRequestHandler, StubRequestHandler stubRequestHandler) {
      return new JettyHttp2Server(options, adminRequestHandler, stubRequestHandler);
    }
  }

  static class JettyHttp2Server extends Jetty94HttpServer {
    public JettyHttp2Server(Options options, AdminRequestHandler adminRequestHandler, StubRequestHandler stubRequestHandler) {
      super(options, adminRequestHandler, stubRequestHandler);
    }

    @Override
    protected ServerConnector createHttpConnector(String bindAddress, int port, JettySettings jettySettings, NetworkTrafficListener listener) {
      HttpConfiguration httpConfig = createHttpConfig(jettySettings);
      // h2c is the protocol for un-encrypted HTTP/2
      HTTP2CServerConnectionFactory h2c = new HTTP2CServerConnectionFactory(httpConfig);
      HttpConnectionFactory http = new HttpConnectionFactory(httpConfig);
      ServerConnector connector = createServerConnector(bindAddress, jettySettings, port, listener, http, h2c);
      connector.setReuseAddress(true);
      return connector;
    }

    protected Server createServer(Options options) {
      final Server server = new Server(new SolrQueuedThreadPool("JettyMockWireServer"));

      return server;
    }
  }

  // WireMock is not shutting down the Jetty threads properly, causing problems during test shutdown
  public static final class WireMockThreadFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
      final ThreadGroup tg = t.getThreadGroup();
      return tg != null && tg.getName().contains("TGRP-") && tg.getName().endsWith("Test");
    }
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final String RESPONSE_CONTENT_TYPE = "application/octet-stream";

  protected static volatile SolrQueuedThreadPool qtp;
  protected static WireMockServer mockSolr;
  protected static DocCollection mockDocCollection;

  @BeforeClass
  public static void beforeCloudHttp2SolrClientWireMockTest() throws Exception {
    qtp = getQtp();
    qtp.start();

    mockSolr = new USLocaleWireMockServer(options().jettyAcceptors(1).bindAddress("127.0.0.1").dynamicPort()
        .httpServerFactory(new JettyHttp2ServerFactory()));
    mockSolr.start();

    log.info("Mock Solr is running at url: {}", mockSolr.baseUrl());
    configureFor("http", "localhost", mockSolr.port());

    mockDocCollection = buildMockDocCollection();
  }

  @AfterClass
  public static void afterCloudHttp2SolrClientWireMockTest() {
    if (qtp != null) {
      qtp.close();
      qtp = null;
    }

    if (mockSolr != null) {
      mockSolr.stop();
      mockSolr = null;
    }
    mockDocCollection = null;
  }

  protected static final String BUILT_IN_MOCK_COLLECTION = "wireMock";
  protected static final String SHARD1_PATH = "/solr/wireMock_s1_r_n1";
  protected static final String SHARD2_PATH = "/solr/wireMock_s2_r_n2";
  protected static final String MOCK_COLLECTION_JSON = "{\n" +
      "    \"s2\":{\n" +
      "      \"range\":\"0-7fffffff\",\n" +
      "      \"state\":\"active\",\n" +
      "      \"replicas\":{\"wireMock_s2_r_n2\":{\n" +
      "        \"core\":\"wireMock_s2_r_n2\",\n" +
      "        \"base_url\":\"http://127.0.0.1:65261/solr\",\n" +
      "        \"node_name\":\"MOCK_SOLR1\",\n" +
      "        \"state\":\"active\",\n" +
      "        \"type\":\"NRT\",\n" +
      "        \"force_set_state\":\"false\",\n" +
      "        \"id\":\"1\",\n" +
      "        \"leader\":\"true\"}}},\n" +
      "    \"s1\":{\n" +
      "      \"range\":\"80000000-ffffffff\",\n" +
      "      \"state\":\"active\",\n" +
      "      \"replicas\":{\"wireMock_s1_r_n1\":{\n" +
      "        \"core\":\"wireMock_s1_r_n1\",\n" +
      "        \"base_url\":\"http://127.0.0.1:65260/solr\",\n" +
      "        \"node_name\":\"MOCK_SOLR1\",\n" +
      "        \"state\":\"active\",\n" +
      "        \"type\":\"NRT\",\n" +
      "        \"force_set_state\":\"false\",\n" +
      "        \"id\":\"2\",\n" +
      "        \"leader\":\"true\"}}}}";

  protected static DocCollection buildMockDocCollection() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(MOCK_COLLECTION_JSON);
    updateReplicaBaseUrl(json, "s1", "wireMock_s1_r_n1", mockSolr.baseUrl());
    updateReplicaBaseUrl(json, "s2", "wireMock_s2_r_n2", mockSolr.baseUrl());
    Map<String, Object> slices = mapper.convertValue(json, new TypeReference<Map<String, Object>>() {});

    Map<String, Object> props = new HashMap<>();
    props.put("replicationFactor", "1");
    props.put("maxShardsPerNode", "1");
    props.put("autoAddReplicas", "false");
    props.put("nrtReplicas", "1");
    props.put("id", 1l);

    return new DocCollection(BUILT_IN_MOCK_COLLECTION, Slice.loadAllFromMap(nodeName -> mockSolr.baseUrl() + "/solr", BUILT_IN_MOCK_COLLECTION, -1l, slices), props, DocRouter.DEFAULT);
  }

  protected static void updateReplicaBaseUrl(JsonNode json, String shard, String replica, String baseUrl) {
    ObjectNode replicaNode = (ObjectNode) json.get(shard).get("replicas").get(replica);
    replicaNode.put("node_name", baseUrl);
  }

  protected CloudHttp2SolrClient testClient = null;
  protected ClusterStateProvider stateProvider = null;

  @Before
  public void createTestClient() throws TimeoutException, InterruptedException, IOException {
    final String baseUrl = mockSolr.baseUrl();
    Map<String, ClusterState.CollectionRef> collectionStates =
        Collections.singletonMap(BUILT_IN_MOCK_COLLECTION, new ClusterState.CollectionRef(mockDocCollection));
    ClusterState clusterState = new ClusterState(collectionStates, 1);
    MockZkStateReader zkStateReader = new MockZkStateReader(clusterState, Collections.singleton(baseUrl), Collections.singleton(BUILT_IN_MOCK_COLLECTION));
    stateProvider = new ZkClientClusterStateProvider(zkStateReader, true);

    /*
    stateProvider = mock(ClusterStateProvider.class);
    when(stateProvider.getLiveNodes()).thenReturn(Collections.singleton(baseUrl));
    when(stateProvider.resolveAlias(anyString())).thenReturn(Collections.singletonList(BUILT_IN_MOCK_COLLECTION));

    when(stateProvider.getCollection(BUILT_IN_MOCK_COLLECTION)).thenReturn(mockDocCollection);
    when(stateProvider.getState(BUILT_IN_MOCK_COLLECTION)).thenReturn(new ClusterState.CollectionRef(mockDocCollection));
     */

    List<String> solrUrls = Collections.singletonList(baseUrl);
    testClient = new CloudHttp2SolrClient.Builder(solrUrls).sendDirectUpdatesToShardLeadersOnly().withClusterStateProvider(stateProvider).build();
    testClient.connect(500, TimeUnit.MILLISECONDS);
  }

  @After
  public void closeTestClient() throws IOException {
    if (testClient != null) {
      testClient.close();
    }
    if (stateProvider != null) {
      stateProvider.close();
    }
    mockSolr.resetAll();
  }

  protected byte[] queryResponseOk() throws IOException {
    return queryResponseOk("javabin");
  }

  protected byte[] queryResponseOk(final String wt) throws IOException {
    NamedList<Object> rh = new NamedList<>();
    rh.add("status", 0);
    rh.add("QTime", 10);

    SolrDocumentList rs = new SolrDocumentList();
    rs.setNumFound(1);
    rs.setMaxScore(1f);
    rs.setNumFoundExact(true);
    rs.add(new SolrDocument(Collections.singletonMap("id", "test")));

    NamedList<Object> nl = new NamedList<>();
    nl.add("responseHeader", rh);
    nl.add("response", rs);
    return "javabin".equals(wt) ? toJavabin(nl) : toXml(nl);
  }

  protected byte[] toXml(NamedList<Object> nl) {
    return "<response></response>".getBytes(); // TODO;
  }

  protected byte[] updateRequestOk() throws IOException {
    NamedList<Object> rh = new NamedList<>();
    rh.add("rf", 1);
    rh.add("errors", Collections.emptyList());
    rh.add("maxErrors", -1);
    rh.add("status", 0);
    rh.add("QTime", 10);
    NamedList<Object> nl = new NamedList<>();
    nl.add("responseHeader", rh);
    return toJavabin(nl);
  }

  protected byte[] toJavabin(NamedList<Object> nl) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (JavaBinCodec codec = new JavaBinCodec()) {
      codec.marshal(nl, baos);
    }
    return baos.toByteArray();
  }

  protected UpdateRequest buildUpdateRequest(final int numDocs) {
    String threadName = Thread.currentThread().getName();
    UpdateRequest req = new UpdateRequest();
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", threadName+(1000+i));
      req.add(doc);
    }
    return req;
  }
}
