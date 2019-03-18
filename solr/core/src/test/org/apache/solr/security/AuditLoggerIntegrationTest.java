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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate that audit logging works in a live cluster
 */
@SolrTestCaseJ4.SuppressSSL
public class AuditLoggerIntegrationTest extends SolrCloudAuthTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private final String COLLECTION = "auditCollection";

  @Test
  public void testSynchronous() throws Exception {
    doTest(false, 0);     
  }

  @Test
  public void testAsync() throws Exception {
    doTest(true, 0);     
  }

  @Test
  public void testAsyncWithQueue() throws Exception {
    doTest(true, 100);     
  }
  
  void doTest(boolean async, int delay) throws Exception {
    CallbackReceiver receiver = new CallbackReceiver();
    int callbackPort = receiver.getPort();

    // Kicking off background thread for listening to the audit logger callbacks
    Thread receiverThread = new DefaultSolrThreadFactory("auditTestCallback").newThread(receiver);
    receiverThread.start();

    String securityJson = FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("auditlog_plugin_security.json").toFile(), StandardCharsets.UTF_8);
    securityJson = securityJson.replace("_PORT_", Integer.toString(callbackPort));
    securityJson = securityJson.replace("_ASYNC_", Boolean.toString(async));
    securityJson = securityJson.replace("_DELAY_", Integer.toString(delay));
    configureCluster(NUM_SERVERS)// nodes
        .withSecurityJson(securityJson)
        .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
    
    cluster.waitForAllNodes(10);

    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.listCollections(client);
    client.request(CollectionAdminRequest.getClusterStatus());
    client.request(CollectionAdminRequest.getOverseerStatus());
    
    assertAuditMetricsMinimums(CallbackAuditLoggerPlugin.class.getSimpleName(), 3, 0);

    shutdownCluster();       
    assertEquals(3, receiver.getTotalCount());
    assertEquals(3, receiver.getCountForPath("/admin/collections"));

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
          
          String r = reader.readLine();
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
  }
}
