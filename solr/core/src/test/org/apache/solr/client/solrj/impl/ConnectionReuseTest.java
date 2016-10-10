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
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionRequest;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ManagedClientConnection;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressSSL
public class ConnectionReuseTest extends SolrCloudTestCase {
  
  private AtomicInteger id = new AtomicInteger();

  private static final String COLLECTION = "collection1";

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    TestInjection.failUpdateRequests = "true:100";
    configureCluster(1)
        .addConfig("config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "config", 1, 1)
        .processAndWait(cluster.getSolrClient(), DEFAULT_TIMEOUT);

    cluster.getSolrClient().waitForState(COLLECTION, DEFAULT_TIMEOUT, TimeUnit.SECONDS,
        (n, c) -> DocCollection.isFullyActive(n, c, 1, 1));
  }

  private SolrClient buildClient(HttpClient httpClient, URL url) {
    switch (random().nextInt(3)) {
      case 0:
        // currently only testing with 1 thread
        return new ConcurrentUpdateSolrClient(url.toString() + "/" + COLLECTION, httpClient, 6, 1) {
          @Override
          public void handleError(Throwable ex) {
            // we're expecting random errors here, don't spam the logs
          }
        };
      case 1:
        return getHttpSolrClient(url.toString() + "/" + COLLECTION, httpClient);
      case 2:
        CloudSolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress(), random().nextBoolean(), httpClient);
        client.setParallelUpdates(random().nextBoolean());
        client.setDefaultCollection(COLLECTION);
        client.getLbClient().setConnectionTimeout(30000);
        client.getLbClient().setSoTimeout(60000);
        return client;
    }
    throw new RuntimeException("impossible");
  }
  
  @Test
  public void testConnectionReuse() throws Exception {

    URL url = cluster.getJettySolrRunners().get(0).getBaseUrl();

    HttpClient httpClient = HttpClientUtil.createClient(null);
    PoolingClientConnectionManager cm = (PoolingClientConnectionManager) httpClient.getConnectionManager();

    try (SolrClient client = buildClient(httpClient, url)) {

      log.info("Using client of type {}", client.getClass());

      HttpHost target = new HttpHost(url.getHost(), url.getPort(), isSSLMode() ? "https" : "http");
      HttpRoute route = new HttpRoute(target);

      ClientConnectionRequest mConn = getClientConnectionRequest(httpClient, route);

      ManagedClientConnection conn1 = getConn(mConn);
      headerRequest(target, route, conn1);
      conn1.releaseConnection();
      cm.releaseConnection(conn1, -1, TimeUnit.MILLISECONDS);

      int queueBreaks = 0;
      int cnt1 = atLeast(3);
      int cnt2 = atLeast(30);
      for (int j = 0; j < cnt1; j++) {
        boolean done = false;
        for (int i = 0; i < cnt2; i++) {
          AddUpdateCommand c = new AddUpdateCommand(null);
          c.solrDoc = sdoc("id", id.incrementAndGet());
          try {
            client.add(c.solrDoc);
            log.info("Added document");
          } catch (Exception e) {
            log.info("Error adding document: {}", e.getMessage());
          }
          if (!done && i > 0 && i < cnt2 - 1 && client instanceof ConcurrentUpdateSolrClient
              && random().nextInt(10) > 8) {
            log.info("Pausing - should start a new request");
            queueBreaks++;
            done = true;
            Thread.sleep(350); // wait past streaming client poll time of 250ms
          }
        }
      }
      if (client instanceof ConcurrentUpdateSolrClient) {
        ((ConcurrentUpdateSolrClient) client).blockUntilFinished();
      }


      route = new HttpRoute(new HttpHost(url.getHost(), url.getPort(), isSSLMode() ? "https" : "http"));

      mConn = cm.requestConnection(route, HttpSolrClient.cacheKey 
          );

      ManagedClientConnection conn2 = getConn(mConn);

      headerRequest(target, route, conn2);
      HttpConnectionMetrics metrics = conn2.getMetrics();
      conn2.releaseConnection();
      cm.releaseConnection(conn2, -1, TimeUnit.MILLISECONDS);


      assertNotNull("No connection metrics found - is the connection getting aborted? server closing the connection? " +
         client.getClass().getSimpleName(), metrics);

      // we try and make sure the connection we get has handled all of the requests in this test
      final long requestCount = metrics.getRequestCount();
     // System.out.println(cm.getTotalStats() + " " + cm );
      if (client instanceof ConcurrentUpdateSolrClient) {
        // we can't fully control queue polling breaking up requests - allow a bit of leeway
        log.info("Outer loop count: {}", cnt1);
        log.info("Queue breaks: {}", queueBreaks);
        int exp = queueBreaks + 3;
        log.info("Expected: {}", exp);
        log.info("Requests: {}", requestCount);
        assertTrue(
            "We expected all communication via streaming client to use one connection! expected=" + exp + " got="
                + requestCount + " "+ client.getClass().getSimpleName(), 
            Math.max(exp, requestCount) - Math.min(exp, requestCount) < 3);
      } else {
        log.info("Outer loop count: {}", cnt1);
        log.info("Inner loop count: {}", cnt2);
        assertTrue("We expected all communication to use one connection! " + client.getClass().getSimpleName()
            +" "+cnt1+" "+ cnt2+ " "+ requestCount,
            cnt1 * cnt2 + 2 <= requestCount);
      }
    }
    finally {
      HttpClientUtil.close(httpClient);
    }

  }

  public ManagedClientConnection getConn(ClientConnectionRequest mConn)
      throws InterruptedException, ConnectionPoolTimeoutException {
    ManagedClientConnection conn = mConn.getConnection(30, TimeUnit.SECONDS);
    conn.setIdleDuration(-1, TimeUnit.MILLISECONDS);
    conn.markReusable();
    return conn;
  }

  public void headerRequest(HttpHost target, HttpRoute route, ManagedClientConnection conn)
      throws IOException, HttpException {
    HttpRequest req = new BasicHttpRequest("OPTIONS", "*", HttpVersion.HTTP_1_1);

    req.addHeader("Host", target.getHostName());
    BasicHttpParams p = new BasicHttpParams();
    HttpProtocolParams.setVersion(p, HttpVersion.HTTP_1_1);
    if (!conn.isOpen()) {
      conn.open(route, HttpClientUtil.createNewHttpClientRequestContext(), p);
    }
    conn.sendRequestHeader(req);
    conn.flush();
    conn.receiveResponseHeader();
  }

  public ClientConnectionRequest getClientConnectionRequest(HttpClient httpClient, HttpRoute route) { // passing second argument to mimic what happens in HttpSolrClient.execute()
    ClientConnectionRequest mConn = ((PoolingClientConnectionManager) httpClient.getConnectionManager()).requestConnection(route, HttpSolrClient.cacheKey);
    return mConn;
  }

}

