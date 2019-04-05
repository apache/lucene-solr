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
package org.apache.solr.security;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.security.AuditEvent.EventType;
import org.apache.solr.security.AuditEvent.RequestType;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.getClusterStatus;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.getOverseerStatus;
import static org.apache.solr.security.AuditEvent.EventType.COMPLETED;
import static org.apache.solr.security.AuditEvent.EventType.ERROR;
import static org.apache.solr.security.AuditEvent.EventType.REJECTED;
import static org.apache.solr.security.AuditEvent.EventType.UNAUTHORIZED;
import static org.apache.solr.security.AuditEvent.RequestType.ADMIN;
import static org.apache.solr.security.AuditEvent.RequestType.SEARCH;

/**
 * Validate that audit logging works in a live cluster
 */
@SolrTestCaseJ4.SuppressSSL
public class AuditLoggerIntegrationTest extends SolrCloudAuthTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  // Use a harness per thread to be able to beast this test
  private ThreadLocal<AuditTestHarness> testHarness = new ThreadLocal<>();

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    testHarness.set(new AuditTestHarness());
  }

  @Override
  @After
  public void tearDown() throws Exception {
    testHarness.get().close();
    super.tearDown();
  }
  
  @Test
  public void testSynchronous() throws Exception {
    setupCluster(false, 0, false, null);
    runAdminCommands();
    waitForAuditEventCallbacks(3);
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    assertThreeAdminEvents();
  }
  
  @Test
  public void testAsync() throws Exception {
    setupCluster(true, 0, false, null);
    runAdminCommands();
    waitForAuditEventCallbacks(3);
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    assertThreeAdminEvents();
  }

  @Test
  public void testQueuedTimeMetric() throws Exception {
    setupCluster(true, 100, false, null);
    runAdminCommands();
    waitForAuditEventCallbacks(3);
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    ArrayList<MetricRegistry> registries = getMetricsReigstries(testHarness.get().cluster);
    Timer timer = ((Timer) registries.get(0).getMetrics().get("SECURITY./auditlogging.CallbackAuditLoggerPlugin.queuedTime"));
    double meanTimeOnQueue = timer.getSnapshot().getMean() / 1000000; // Convert to ms
    assertTrue("Expecting mean time on queue >10ms, got " + meanTimeOnQueue, meanTimeOnQueue > 10);
  }

  @Test
  public void testAsyncQueueDrain() throws Exception {
    setupCluster(true, 100, false, null);
    runAdminCommands();
    assertTrue("Expecting <2 callbacks in buffer, was " + testHarness.get().receiver.getBuffer().size(),
        testHarness.get().receiver.getBuffer().size() < 2); // Events still on queue
    // We shutdown cluster while events are still in queue
    testHarness.get().shutdownCluster();
    assertThreeAdminEvents();
  }
  
  @Test
  public void testMuteAdminListCollections() throws Exception {
    setupCluster(false, 0, false, "[ \"type:UNKNOWN\", [ \"path:/admin\", \"param:action=LIST\" ] ]");
    runAdminCommands();
    testHarness.get().shutdownCluster();
    waitForAuditEventCallbacks(2);
    assertEquals(2, testHarness.get().receiver.getBuffer().size());
  }

  @Test
  public void searchWithException() throws Exception {
    setupCluster(false, 0, false, null);
    try {
      testHarness.get().cluster.getSolrClient().request(CollectionAdminRequest.createCollection("test", 1, 1));
      testHarness.get().cluster.getSolrClient().query("test", new MapSolrParams(Collections.singletonMap("q", "a(bc")));
      fail("Query should fail");
    } catch (SolrException ex) {
      waitForAuditEventCallbacks(3);
      CallbackReceiver receiver = testHarness.get().receiver;
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/cores");
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/collections");
      assertAuditEvent(receiver.popEvent(), ERROR,"/select", SEARCH, null, 400);
    }
  }

  @Test
  public void auth() throws Exception {
    setupCluster(false, 0, true, null);
    CloudSolrClient client = testHarness.get().cluster.getSolrClient();
    try {
      CollectionAdminRequest.List request = new CollectionAdminRequest.List();
      client.request(request);
      request.setBasicAuthCredentials("solr", "SolrRocks");
      client.request(request);
      CollectionAdminRequest.Create createRequest = CollectionAdminRequest.createCollection("test", 1, 1);
      client.request(createRequest);
      fail("Call should fail with 401");
    } catch (SolrException ex) {
      waitForAuditEventCallbacks(3);
      CallbackReceiver receiver = testHarness.get().receiver;
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/collections", ADMIN, null, 200, "action", "LIST");
      AuditEvent e = receiver.popEvent();
      System.out.println(new AuditLoggerPlugin.JSONAuditEventFormatter().formatEvent(e));
      assertAuditEvent(e, COMPLETED, "/admin/collections", ADMIN, "solr", 200, "action", "LIST");
      assertAuditEvent(receiver.popEvent(), REJECTED, "/admin/collections", ADMIN, null,401);
    }
    try {
      CollectionAdminRequest.Create createRequest = CollectionAdminRequest.createCollection("test", 1, 1);
      createRequest.setBasicAuthCredentials("solr", "wrongPW");
      client.request(createRequest);       
      fail("Call should fail with 403");
    } catch (SolrException ex) {
      waitForAuditEventCallbacks(1);
      CallbackReceiver receiver = testHarness.get().receiver;
      assertAuditEvent(receiver.popEvent(), UNAUTHORIZED, "/admin/collections", ADMIN, null,403);
    }
  }

  private void assertAuditEvent(AuditEvent e, EventType type, String path, String... params) {
    assertAuditEvent(e, type, path, null, null,null, params);
  }

  private void assertAuditEvent(AuditEvent e, EventType type, String path, RequestType requestType, String username, Integer status, String... params) {
    assertEquals(type, e.getEventType());
    assertEquals(path, e.getResource());
    if (requestType != null) {
      assertEquals(requestType, e.getRequestType());
    }
    if (username != null) {
      assertEquals(username, e.getUsername());
    }
    if (status != null) {
      assertEquals(status.intValue(), e.getStatus());
    }
    if (params != null && params.length > 0) {
      List<String> p = new LinkedList<>(Arrays.asList(params));
      while (p.size() >= 2) {
        String val = e.getSolrParamAsString(p.get(0));
        assertEquals(p.get(1), val);
        p.remove(0);
        p.remove(0);
      }
    }
  }

  private void waitForAuditEventCallbacks(int number) throws InterruptedException {
    waitForAuditEventCallbacks(number, 5);
  }

  private void waitForAuditEventCallbacks(int number, int timeoutSeconds) throws InterruptedException {
    CallbackReceiver receiver = testHarness.get().receiver;
    int count = 0;
    while(receiver.buffer.size() < number) { 
      Thread.sleep(100);
      if (++count >= timeoutSeconds*10) fail("Failed waiting for " + number + " callbacks after " + timeoutSeconds + " seconds");
    }
  }

  private ArrayList<MetricRegistry> getMetricsReigstries(MiniSolrCloudCluster cluster) {
    ArrayList<MetricRegistry> registries = new ArrayList<>();
    cluster.getJettySolrRunners().forEach(r -> {
      MetricRegistry registry = r.getCoreContainer().getMetricManager().registry("solr.node");
      assertNotNull(registry);
      registries.add(registry);
    });
    return registries;
  }
  
  private void runAdminCommands() throws IOException, SolrServerException {
    SolrClient client = testHarness.get().cluster.getSolrClient();
    CollectionAdminRequest.listCollections(client);
    client.request(getClusterStatus());
    client.request(getOverseerStatus());
  }

  private void assertThreeAdminEvents() throws Exception {
    CallbackReceiver receiver = testHarness.get().receiver;
    waitForAuditEventCallbacks(3);
    assertEquals(3, receiver.getTotalCount());
    assertEquals(3, receiver.getCountForPath("/admin/collections"));
    
    AuditEvent e = receiver.getBuffer().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("action=LIST&wt=javabin&version=2", e.getHttpQueryString());
    assertEquals("LIST", e.getSolrParamAsString("action"));
    assertEquals("javabin", e.getSolrParamAsString("wt"));

    e = receiver.getBuffer().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("CLUSTERSTATUS", e.getSolrParamAsString("action"));

    e = receiver.getBuffer().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("OVERSEERSTATUS", e.getSolrParamAsString("action"));
  }

  private static String AUTH_SECTION = ",\n" +
      "  \"authentication\":{\n" +
      "    \"blockUnknown\":\"false\",\n" +
      "    \"class\":\"solr.BasicAuthPlugin\",\n" +
      "    \"credentials\":{\"solr\":\"orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw=\"}},\n" +
      "  \"authorization\":{\n" +
      "    \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n" +
      "    \"user-role\":{\"solr\":\"admin\"},\n" +
      "    \"permissions\":[{\"name\":\"collection-admin-edit\",\"role\":\"admin\"}]\n" +
      "  }\n";
  
  private void setupCluster(boolean async, int delay, boolean enableAuth, String muteRulesJson) throws Exception {
    String securityJson = FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("auditlog_plugin_security.json").toFile(), StandardCharsets.UTF_8);
    securityJson = securityJson.replace("_PORT_", Integer.toString(testHarness.get().callbackPort));
    securityJson = securityJson.replace("_ASYNC_", Boolean.toString(async));
    securityJson = securityJson.replace("_DELAY_", Integer.toString(delay));
    securityJson = securityJson.replace("_AUTH_", enableAuth ? AUTH_SECTION : "");
    securityJson = securityJson.replace("_MUTERULES_", muteRulesJson != null ? muteRulesJson : "[]");
    MiniSolrCloudCluster myCluster = new Builder(NUM_SERVERS, createTempDir())
        .withSecurityJson(securityJson)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .build();
    
    myCluster.waitForAllNodes(10);
    testHarness.get().setCluster(myCluster);
  }


  /**
   * Listening for socket callbacks in background thread from the custom CallbackAuditLoggerPlugin
   */
  private class CallbackReceiver implements Runnable, AutoCloseable {
    private final ServerSocket serverSocket;
    private AtomicInteger count = new AtomicInteger();
    private Map<String,AtomicInteger> resourceCounts = new HashMap<>();
    private LinkedList<AuditEvent> buffer = new LinkedList<>();

    CallbackReceiver() throws IOException {
      serverSocket = new ServerSocket(0);
    }

    int getTotalCount() {
      return count.get();
    }

    int getCountForPath(String path) {
      return resourceCounts.getOrDefault(path, new AtomicInteger()).get();
    }
    
    public int getPort() {
      return serverSocket.getLocalPort();
    }

    @Override
    public void run() {
      try {
        log.info("Listening for audit callbacks on on port {}", serverSocket.getLocalPort());
        Socket socket = serverSocket.accept();
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
        while (!Thread.currentThread().isInterrupted()) {
          if (!reader.ready()) continue;
          ObjectMapper om = new ObjectMapper();
          om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
          AuditEvent event = om.readValue(reader.readLine(), AuditEvent.class);
          buffer.add(event);
          String r = event.getResource();
          log.info("Received audit event for path " + r);
          count.incrementAndGet();
          AtomicInteger resourceCounter = resourceCounts.get(r);
          if (resourceCounter == null) {
            resourceCounter = new AtomicInteger(1);
            resourceCounts.put(r, resourceCounter);
          } else {
            resourceCounter.incrementAndGet();
          }
        }
      } catch (IOException e) { 
        log.info("Socket closed", e);
      }
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
    }

    protected LinkedList<AuditEvent> getBuffer() {
      return buffer;
    }

    protected AuditEvent popEvent() {
      return buffer.pop();
    }
  }

  private class AuditTestHarness implements AutoCloseable {
    CallbackReceiver receiver;
    int callbackPort;
    Thread receiverThread;
    private MiniSolrCloudCluster cluster;

    AuditTestHarness() throws IOException {
      receiver = new CallbackReceiver();
      callbackPort = receiver.getPort();
      receiverThread = new DefaultSolrThreadFactory("auditTestCallback").newThread(receiver);
      receiverThread.start();
    }

    @Override
    public void close() throws Exception {
      shutdownCluster();
      receiverThread.interrupt();
      receiver.close();
      receiverThread = null;
    }

    public void shutdownCluster() throws Exception {
      if (cluster != null) cluster.shutdown();
    }

    public void setCluster(MiniSolrCloudCluster cluster) {
      this.cluster = cluster;
    }
  }
}
