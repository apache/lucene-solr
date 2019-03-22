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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
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


/**
 * Validate that audit logging works in a live cluster
 */
@SolrTestCaseJ4.SuppressSSL
public class AuditLoggerIntegrationTest extends SolrCloudAuthTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private CallbackReceiver receiver;
  private int callbackPort;
  private Thread receiverThread;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    receiver = new CallbackReceiver();
    callbackPort = receiver.getPort();
    receiverThread = new DefaultSolrThreadFactory("auditTestCallback").newThread(receiver);
    receiverThread.start();
  }

  @Test
  public void testSynchronous() throws Exception {
    setupCluster(false, 0);
    runAdminCommands();
    assertAuditMetricsMinimums(CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    shutdownCluster();
    assertThreeAdminEvents(receiver);
  }
  
  @Test
  public void testAsync() throws Exception {
    setupCluster(true, 0);
    runAdminCommands();
    assertAuditMetricsMinimums(CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    shutdownCluster();
    assertThreeAdminEvents(receiver);
  }

  @Test
  public void testAsyncWithQueue() throws Exception {
    setupCluster(true, 100);
    runAdminCommands();
    assertAuditMetricsMinimums(CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    shutdownCluster();
    assertThreeAdminEvents(receiver);
  }

  @Test
  public void searchWithException() throws Exception {
    setupCluster(false, 0);
    try {
      cluster.getSolrClient().request(CollectionAdminRequest.createCollection("test", 1, 1));
      cluster.getSolrClient().query("test", new MapSolrParams(Collections.singletonMap("q", "a(bc")));
      fail("Query should fail");
    } catch (SolrException ex) {
      waitForAuditEventCallbacks(3);
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/cores");
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/collections");
      assertAuditEvent(receiver.popEvent(), ERROR,"/select", "READ", null, 500);
    }
  }

  @Test
  public void auth() throws Exception {
    setupCluster(false, 0, true);
    CloudSolrClient client = cluster.getSolrClient();
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
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/collections", "action", "LIST");
      assertAuditEvent(receiver.popEvent(), COMPLETED, "/admin/collections", "ADMIN", "solr", 0, "action", "LIST");
      assertAuditEvent(receiver.popEvent(), REJECTED, "/admin/collections", "ADMIN", null,401);
    }
    try {
      CollectionAdminRequest.Create createRequest = CollectionAdminRequest.createCollection("test", 1, 1);
      createRequest.setBasicAuthCredentials("solr", "wrongPW");
      client.request(createRequest);       
      fail("Call should fail with 403");
    } catch (SolrException ex) {
      waitForAuditEventCallbacks(1);
      assertAuditEvent(receiver.popEvent(), UNAUTHORIZED, "/admin/collections", "ADMIN", null,403);
    }
  }

  private void assertAuditEvent(AuditEvent e, AuditEvent.EventType type, String path, String... params) {
    assertAuditEvent(e, type, path, null, null,null, params);
  }

  private void assertAuditEvent(AuditEvent e, AuditEvent.EventType type, String path, String requestType, String username, Integer status, String... params) {
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
    while(receiver.count.get() < number) { 
      Thread.sleep(100); 
    }
  }

  private void runAdminCommands() throws IOException, SolrServerException {
    SolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.listCollections(client);
    client.request(getClusterStatus());
    client.request(getOverseerStatus());
  }

  private void assertThreeAdminEvents(CallbackReceiver receiver) {
    assertEquals(3, receiver.getTotalCount());
    assertEquals(3, receiver.getCountForPath("/admin/collections"));
    
    AuditEvent e = receiver.getHistory().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("action=LIST&wt=javabin&version=2", e.getHttpQueryString());
    assertEquals("LIST", e.getSolrParamAsString("action"));
    assertEquals("javabin", e.getSolrParamAsString("wt"));

    e = receiver.getHistory().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("CLUSTERSTATUS", e.getSolrParamAsString("action"));

    e = receiver.getHistory().pop();
    assertEquals(COMPLETED, e.getEventType());
    assertEquals("GET", e.getHttpMethod());
    assertEquals("OVERSEERSTATUS", e.getSolrParamAsString("action"));
  }

  private void setupCluster(boolean async, int delay) throws Exception {
    setupCluster(async, delay, false);
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
  
  void setupCluster(boolean async, int delay, boolean enableAuth) throws Exception {
    String securityJson = FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("auditlog_plugin_security.json").toFile(), StandardCharsets.UTF_8);
    securityJson = securityJson.replace("_PORT_", Integer.toString(callbackPort));
    securityJson = securityJson.replace("_ASYNC_", Boolean.toString(async));
    securityJson = securityJson.replace("_DELAY_", Integer.toString(delay));
    securityJson = securityJson.replace("_AUTH_", enableAuth ? AUTH_SECTION : "");
    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(securityJson)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    
    cluster.waitForAllNodes(10);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    shutdownCluster();
    receiverThread.interrupt();
    receiver.close();
  }

  /**
   * Listening for socket callbacks in background thread from the custom CallbackAuditLoggerPlugin
   */
  private class CallbackReceiver implements Runnable, AutoCloseable {
    private final ServerSocket serverSocket;
    private AtomicInteger count = new AtomicInteger();
    private Map<String,AtomicInteger> resourceCounts = new HashMap<>();
    private LinkedList<AuditEvent> history = new LinkedList<>();

    public CallbackReceiver() throws IOException {
      serverSocket = new ServerSocket(0);
    }

    public int getTotalCount() {
      return count.get();
    }

    public int getCountForPath(String path) {
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
          history.add(event);
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

    public LinkedList<AuditEvent> getHistory() {
      return history;
    }

    public AuditEvent popEvent() {
      return history.pop();
    }
  }
}
