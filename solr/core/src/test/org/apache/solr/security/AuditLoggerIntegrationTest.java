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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.util.TestUtil;
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
import org.apache.solr.security.AuditLoggerPlugin.JSONAuditEventFormatter;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.request.CollectionAdminRequest.Create;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.getClusterStatus;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.getOverseerStatus;
import static org.apache.solr.security.AuditEvent.EventType.COMPLETED;
import static org.apache.solr.security.AuditEvent.EventType.ERROR;
import static org.apache.solr.security.AuditEvent.EventType.REJECTED;
import static org.apache.solr.security.AuditEvent.EventType.UNAUTHORIZED;
import static org.apache.solr.security.AuditEvent.RequestType.ADMIN;
import static org.apache.solr.security.AuditEvent.RequestType.SEARCH;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

/**
 * Validate that audit logging works in a live cluster
 */
@SolrTestCaseJ4.SuppressSSL
public class AuditLoggerIntegrationTest extends SolrCloudAuthTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final JSONAuditEventFormatter formatter = new JSONAuditEventFormatter();

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
    if (null != testHarness.get()) {
      testHarness.get().close();
    }
    super.tearDown();
    CallbackAuditLoggerPlugin.BLOCKING_SEMAPHORES.clear();
  }
  
  @Test
  public void testSynchronous() throws Exception {
    setupCluster(false, null, false);
    runThreeTestAdminCommands();
    assertThreeTestAdminEvents();
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
  }
  
  @Test
  public void testAsync() throws Exception {
    setupCluster(true, null, false);
    runThreeTestAdminCommands();
    assertThreeTestAdminEvents();
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
  }

  @Test
  public void testQueuedTimeMetric() throws Exception {
    final Semaphore gate = new Semaphore(0);
    CallbackAuditLoggerPlugin.BLOCKING_SEMAPHORES.put("testQueuedTimeMetric_semaphore", gate);
    setupCluster(true, "testQueuedTimeMetric_semaphore", false);

    // NOTE: gate is empty, we don't allow any of the events to be logged yet
    runThreeTestAdminCommands();

    // Don't assume anything about the system clock,
    // Thread.sleep is not a garunteed minimum for a predictible elapsed time...
    final long start = System.nanoTime();
    Thread.sleep(100);
    final long end = System.nanoTime();
    gate.release(3);

    assertThreeTestAdminEvents();
    assertAuditMetricsMinimums(testHarness.get().cluster, CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);
    ArrayList<MetricRegistry> registries = getMetricsReigstries(testHarness.get().cluster);
    Timer timer = ((Timer) registries.get(0).getMetrics().get("SECURITY./auditlogging.CallbackAuditLoggerPlugin.queuedTime"));
    double meanTimeOnQueue = timer.getSnapshot().getMean();
    double meanTimeExpected = (start - end) / 3.0D;
    assertTrue("Expecting mean time on queue > "+meanTimeExpected+", got " + meanTimeOnQueue,
               meanTimeOnQueue > meanTimeExpected);
  }

  @Test
  public void testAsyncQueueDrain() throws Exception {
    final AuditTestHarness harness = testHarness.get();
    final Semaphore gate = new Semaphore(0);
    CallbackAuditLoggerPlugin.BLOCKING_SEMAPHORES.put("testAsyncQueueDrain_semaphore", gate);
    setupCluster(true, "testAsyncQueueDrain_semaphore", false);

    final int preShutdownEventsAllowed = TestUtil.nextInt(random(), 0, 2);
    final int postShutdownEventsAllowed = 3 - preShutdownEventsAllowed;

    // Starting by only allowing 2/3 of the (expected) events to be logged right away...
    log.info("Test will allow {} events to happen prior to shutdown", preShutdownEventsAllowed);
    gate.release(preShutdownEventsAllowed);
    runThreeTestAdminCommands();

    final List<AuditEvent> events = new ArrayList
      (harness.receiver.waitForAuditEvents(preShutdownEventsAllowed));
    assertEquals(preShutdownEventsAllowed, events.size());

    // Now shutdown cluster while 1 event still in process
    // Do this in a background thread because it blocks...
    final Thread shutdownThread = new DefaultSolrThreadFactory("shutdown")
      .newThread(() -> { try {
            log.info("START Shutting down Cluster.");
            harness.shutdownCluster();
            log.info("END   Shutting down Cluster.");
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    try {
      shutdownThread.start();
      // release the ticket so the event can be processed
      log.info("releasing final {} semaphore tickets...", postShutdownEventsAllowed);
      gate.release(postShutdownEventsAllowed);

      events.addAll(harness.receiver.waitForAuditEvents(postShutdownEventsAllowed));

      assertThreeTestAdminEvents(events);
    } finally {
      shutdownThread.join();
    }
  }
  
  @Test
  public void testMuteAdminListCollections() throws Exception {
    setupCluster(false, null, false, "\"type:UNKNOWN\"", "[ \"path:/admin\", \"param:action=LIST\" ]");
    runThreeTestAdminCommands();
    testHarness.get().shutdownCluster();
    final List<AuditEvent> events = testHarness.get().receiver.waitForAuditEvents(2);
    assertEquals(2, events.size()); // sanity check

    assertAuditEvent(events.get(0), COMPLETED, "/admin/collections", ADMIN, null, 200,
                     "action", "CLUSTERSTATUS");

    assertAuditEvent(events.get(1), COMPLETED, "/admin/collections", ADMIN, null, 200,
                     "action", "OVERSEERSTATUS");
  }

  @Test
  public void searchWithException() throws Exception {
    setupCluster(false, null, false);
    testHarness.get().cluster.getSolrClient().request(CollectionAdminRequest.createCollection("test", 1, 1));
    expectThrows(SolrException.class, () -> {
      testHarness.get().cluster.getSolrClient().query("test", new MapSolrParams(Collections.singletonMap("q", "a(bc")));
      });
    final List<AuditEvent> events = testHarness.get().receiver.waitForAuditEvents(3);
    assertAuditEvent(events.get(0), COMPLETED, "/admin/cores");
    assertAuditEvent(events.get(1), COMPLETED, "/admin/collections");
    assertAuditEvent(events.get(2), ERROR,"/select", SEARCH, null, 400);
  }

  @Test
  public void illegalAdminPathError() throws Exception {
    setupCluster(false, null, false);
    String baseUrl = testHarness.get().cluster.getJettySolrRunner(0).getBaseUrl().toString();
    expectThrows(FileNotFoundException.class, () -> {
      IOUtils.toString(new URL(baseUrl.replace("/solr", "") + "/api/node/foo"), StandardCharsets.UTF_8);
    });
    final List<AuditEvent> events = testHarness.get().receiver.waitForAuditEvents(1);
    assertAuditEvent(events.get(0), ERROR, "/api/node/foo", ADMIN, null, 404);
  }

  @Test
  public void authValid() throws Exception {
    setupCluster(false, null, true);
    final CloudSolrClient client = testHarness.get().cluster.getSolrClient();
    final CallbackReceiver receiver = testHarness.get().receiver;

    { // valid READ requests: #1 with, and #2 without, (valid) Authentication
      final CollectionAdminRequest.List req = new CollectionAdminRequest.List();

      // we don't block unknown users for READ, so this should succeed
      client.request(req);

      // Authenticated user (w/valid password) should also succeed
      req.setBasicAuthCredentials("solr", SOLR_PASS);
      client.request(req);

      final List<AuditEvent> events = receiver.waitForAuditEvents(2);
      assertAuditEvent(events.get(0), COMPLETED, "/admin/collections", ADMIN, null, 200, "action", "LIST");
      assertAuditEvent(events.get(1), COMPLETED, "/admin/collections", ADMIN, "solr", 200, "action", "LIST");
    }

    { // valid CREATE request: Authenticated admin user should be allowed to CREATE collection
      final Create req = CollectionAdminRequest.createCollection("test_create", 1, 1);
      req.setBasicAuthCredentials("solr", SOLR_PASS);
      client.request(req);

      // collection createion leads to AuditEvent's for the core as well...
      final List<AuditEvent> events = receiver.waitForAuditEvents(2);
      assertAuditEvent(events.get(0), COMPLETED, "/admin/cores", ADMIN, null, 200, "action", "CREATE");
      assertAuditEvent(events.get(1), COMPLETED, "/admin/collections", ADMIN, null, 200, "action", "CREATE");
    }
  }

  @Test
  public void authFailures() throws Exception {
    setupCluster(false, null, true);
    final CloudSolrClient client = testHarness.get().cluster.getSolrClient();
    final CallbackReceiver receiver = testHarness.get().receiver;

    { // invalid request: Authenticated user not allowed to CREATE w/o Authorization
      final SolrException e = expectThrows(SolrException.class, () -> {
          final Create createRequest = CollectionAdminRequest.createCollection("test_jimbo", 1, 1);
          createRequest.setBasicAuthCredentials("jimbo", JIMBO_PASS);
          client.request(createRequest);
        });
      assertEquals(403, e.code());

      final List<AuditEvent> events = receiver.waitForAuditEvents(1);
      assertAuditEvent(events.get(0), UNAUTHORIZED, "/admin/collections", ADMIN, "jimbo", 403, "name", "test_jimbo");
    }

    { // invalid request: Anon user not allowed to CREATE w/o authentication + authorization
      final SolrException e = expectThrows(SolrException.class, () -> {
          Create createRequest = CollectionAdminRequest.createCollection("test_anon", 1, 1);
          client.request(createRequest);
        });
      assertEquals(401, e.code());

      final List<AuditEvent> events = receiver.waitForAuditEvents(1);
      assertAuditEvent(events.get(0), REJECTED,     "/admin/collections", ADMIN, null, 401, "name", "test_anon");
    }

    { // invalid request: Admin user not Authenticated due to incorrect password
      final SolrException e = expectThrows(SolrException.class, () -> {
          Create createRequest = CollectionAdminRequest.createCollection("test_wrongpass", 1, 1);
          createRequest.setBasicAuthCredentials("solr", "wrong_" + SOLR_PASS);
          client.request(createRequest);
        });
      assertEquals(401, e.code());

      final List<AuditEvent> events = receiver.waitForAuditEvents(1);
      // Event generated from HttpServletRequest. Has no user since auth failed
      assertAuditEvent(events.get(0), REJECTED, "/admin/collections", RequestType.ADMIN, null, 401);
    }
  }

  private static void assertAuditEvent(AuditEvent e, EventType type, String path, String... params) {
    assertAuditEvent(e, type, path, null, null,null, params);
  }

  private static void assertAuditEvent(AuditEvent e, EventType type, String path, RequestType requestType, String username, Integer status, String... params) {
    try {
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
    } catch (AssertionError ae) {
      throw new AssertionError(formatter.formatEvent(e) + " => " + ae.getMessage(), ae);
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

  /** @see #assertThreeTestAdminEvents */
  private void runThreeTestAdminCommands() throws IOException, SolrServerException {
    SolrClient client = testHarness.get().cluster.getSolrClient();
    CollectionAdminRequest.listCollections(client);
    client.request(getClusterStatus());
    client.request(getOverseerStatus());
  }

  /** @see #runThreeTestAdminCommands */
  private void assertThreeTestAdminEvents() throws Exception {
    final CallbackReceiver receiver = testHarness.get().receiver;
    final List<AuditEvent> events = receiver.waitForAuditEvents(3);
    assertThreeTestAdminEvents(events);
  }

  /** @see #runThreeTestAdminCommands */
  private static void assertThreeTestAdminEvents(final List<AuditEvent> events) throws Exception {
    assertEquals(3, events.size()); // sanity check

    assertAuditEvent(events.get(0), COMPLETED, "/admin/collections", ADMIN, null, 200,
                     "action", "LIST", "wt", "javabin");
    
    assertAuditEvent(events.get(1), COMPLETED, "/admin/collections", ADMIN, null, 200,
                     "action", "CLUSTERSTATUS");

    assertAuditEvent(events.get(2), COMPLETED, "/admin/collections", ADMIN, null, 200,
                     "action", "OVERSEERSTATUS");

  }

  private static String SOLR_PASS = "SolrRocks";
  private static String JIMBO_PASS = "JimIsCool";
  private static String AUTH_SECTION = ",\n" +
      "  \"authentication\":{\n" +
      "    \"blockUnknown\":\"false\",\n" +
      "    \"class\":\"solr.BasicAuthPlugin\",\n" +
      "    \"credentials\":{\"solr\":\"" + getSaltedHashedValue(SOLR_PASS) + "\"," +
      "                     \"jimbo\":\"" + getSaltedHashedValue(JIMBO_PASS)  + "\"}},\n" +
      "  \"authorization\":{\n" +
      "    \"class\":\"solr.RuleBasedAuthorizationPlugin\",\n" +
      "    \"user-role\":{\"solr\":\"admin\"},\n" +
      "    \"permissions\":[{\"name\":\"collection-admin-edit\",\"role\":\"admin\"}]\n" +
      "  }\n";

  /**
   * Starts the cluster with a security.json built from template, using CallbackAuditLoggerPlugin. The params
   * to this method will fill the template.
   * @param async enable async audit logging
   * @param semaphoreName name of semaphore for controlling how to delay logging
   * @param enableAuth should authentication be enabled in this cluster?
   * @param muteRulesJson mute rules to trim down what events we care about in our tests
   * @throws Exception if anything goes wrong
   */
  private void setupCluster(boolean async, String semaphoreName, boolean enableAuth, String... muteRulesJson) throws Exception {
    String securityJson = FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("auditlog_plugin_security.json").toFile(), StandardCharsets.UTF_8);
    securityJson = securityJson.replace("_PORT_", Integer.toString(testHarness.get().callbackPort));
    securityJson = securityJson.replace("_ASYNC_", Boolean.toString(async));
    securityJson = securityJson.replace("_SEMAPHORE_",
                                        null == semaphoreName ? "null" : "\""+semaphoreName+"\"");
    securityJson = securityJson.replace("_AUTH_", enableAuth ? AUTH_SECTION : "");

    // start with any test specific mute rules...
    final List<String> muteRules = new ArrayList<>(Arrays.asList(muteRulesJson));

    // for test purposes, ignore any intranode /metrics requests...
    muteRules.add("\"path:/admin/metrics\"");

    // With auth enabled we're also getting /admin/info/key requests
    // So for test purposes, we're automatically MUTEing those when auth is enabled...
    if (enableAuth) {
      muteRules.add("\"path:/admin/info/key\"");
    }

    securityJson = securityJson.replace("_MUTERULES_", "[" + StringUtils.join(muteRules, ",") + "]");

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
    private BlockingQueue<AuditEvent> queue = new LinkedBlockingDeque<>();

    CallbackReceiver() throws IOException {
      serverSocket = new ServerSocket(0);
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
          final String msg = reader.readLine();
          final AuditEvent event = om.readValue(msg, AuditEvent.class);
          log.info("Received {}: {}", event, msg);
          queue.add(event);
        }
      } catch (IOException e) { 
        log.info("Socket closed", e);
      }
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
      assertEquals("Unexpected AuditEvents still in the queue",
                   Collections.emptyList(), new LinkedList<>(queue));
    }

    public List<AuditEvent> waitForAuditEvents(final int expected) throws InterruptedException {
      final LinkedList<AuditEvent> results = new LinkedList<>();
      for (int i = 1; i <= expected; i++) { // NOTE: counting from 1 for error message readabiity...
        final AuditEvent e = queue.poll(120, TimeUnit.SECONDS);
        if (null == e) {
          fail("did not recieved expected event #" + i + "/" + expected
               + " even after waiting an excessive amount of time");
        }
        log.info("Waited for and recieved event: {}", e);
        results.add(e);
      }
      return results;
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
