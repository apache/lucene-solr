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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate that audit logging works in a live cluster
 */
@LuceneTestCase.Slow
@SolrTestCaseJ4.SuppressSSL
public class AuditLoggerIntegrationTest extends SolrCloudAuthTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected static final int NUM_SERVERS = 1;
  protected static final int NUM_SHARDS = 1;
  protected static final int REPLICATION_FACTOR = 1;
  private final String COLLECTION = "auditCollection";
  private String baseUrl;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void test() throws Exception {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try (CallbackReceiver receiver = new CallbackReceiver()) {
      int callbackPort = receiver.getPort();

      executorService.submit(receiver);

      log.info("Starting cluster with callbackPort {}", callbackPort);
      String securityJson = FileUtils.readFileToString(TEST_PATH().resolve("security").resolve("auditlog_plugin_security.json").toFile(), StandardCharsets.UTF_8);
      securityJson = securityJson.replace("_PORT_", Integer.toString(callbackPort));
      configureCluster(NUM_SERVERS)// nodes
          .withSecurityJson(securityJson)
          .addConfig("conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
          .withDefaultClusterProperty("useLegacyReplicaAssignment", "false")
          .configure();
      baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();

      cluster.waitForAllNodes(10);

      String baseUrl = cluster.getRandomJetty(random()).getBaseUrl().toString();

      createCollection(COLLECTION);

      get(baseUrl + "/" + COLLECTION + "/query?q=*:*");
      assertEquals(1, receiver.getCount());
    } finally {
      shutdownCluster();       
      executorService.shutdown();
    }
  }
  
  private Pair<String, Integer> get(String url) throws IOException {
    URL createUrl = new URL(url);
    HttpURLConnection createConn = (HttpURLConnection) createUrl.openConnection();
    BufferedReader br2 = new BufferedReader(new InputStreamReader((InputStream) createConn.getContent(), StandardCharsets.UTF_8));
    String result = br2.lines().collect(Collectors.joining("\n"));
    int code = createConn.getResponseCode();
    createConn.disconnect();
    return new Pair<>(result, code);
  }

  private void createCollection(String collectionName) throws IOException {
    assertEquals(200, get(baseUrl + "/admin/collections?action=CREATE&name=" + collectionName + "&numShards=" + NUM_SHARDS).second().intValue());
    cluster.waitForActiveCollection(collectionName, NUM_SHARDS, REPLICATION_FACTOR);
  }

  private class CallbackReceiver implements Runnable, AutoCloseable {
    private final ServerSocket serverSocket;
    private AtomicInteger count = new AtomicInteger();
    private AtomicInteger errors = new AtomicInteger();
    
    public CallbackReceiver() throws IOException {
      serverSocket = new ServerSocket(0);
    }

    public int getCount() {
      return count.get();
    }

    public int getErrors() {
      return errors.get();
    }
    
    public int getPort() {
      return serverSocket.getLocalPort();
    }

    @Override
    public void run() {
      try {
        Socket socket = serverSocket.accept();
        InputStream is = socket.getInputStream();
        DataInputStream dis = new DataInputStream(is);
        int b;
        while (true) {
          b = dis.readByte();
          log.info("Callback=" + b);
          count.incrementAndGet();
        }
      } catch (IOException e) { 
        log.info("Socket closed");
      }
    }

    @Override
    public void close() throws Exception {
      serverSocket.close();
    }
  }
}
