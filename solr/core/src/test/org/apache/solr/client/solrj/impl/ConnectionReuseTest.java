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
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.HttpClientConnection;
import org.apache.http.HttpConnectionMetrics;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpVersion;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHttpRequest;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL
public class ConnectionReuseTest extends SolrCloudTestCase {
  
  private AtomicInteger id = new AtomicInteger();
  private HttpClientContext context = HttpClientContext.create();

  private static final String COLLECTION = "collection1";

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

  private SolrClient buildClient(CloseableHttpClient httpClient, URL url) {
    switch (random().nextInt(3)) {
      case 0:
        // currently only testing with 1 thread
        return getConcurrentUpdateSolrClient(url.toString() + "/" + COLLECTION, httpClient, 6, 1);
      case 1:
        return getHttpSolrClient(url.toString() + "/" + COLLECTION, httpClient);
      case 2:
        CloudSolrClient client = getCloudSolrClient(cluster.getZkServer().getZkAddress(), random().nextBoolean(), httpClient, 30000, 60000);
        client.setDefaultCollection(COLLECTION);
        return client;
    }
    throw new RuntimeException("impossible");
  }
  
  @Test
  public void testConnectionReuse() throws Exception {

    URL url = cluster.getJettySolrRunners().get(0).getBaseUrl();
    PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

    CloseableHttpClient httpClient = HttpClientUtil.createClient(null, cm);
    try (SolrClient client = buildClient(httpClient, url)) {

      HttpHost target = new HttpHost(url.getHost(), url.getPort(), isSSLMode() ? "https" : "http");
      HttpRoute route = new HttpRoute(target);

      ConnectionRequest mConn = getClientConnectionRequest(httpClient, route, cm);

      HttpClientConnection conn1 = getConn(mConn);
      headerRequest(target, route, conn1, cm);

      cm.releaseConnection(conn1, null, -1, TimeUnit.MILLISECONDS);

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
          } catch (Exception e) {
            e.printStackTrace();
          }
          if (!done && i > 0 && i < cnt2 - 1 && client instanceof ConcurrentUpdateSolrClient
              && random().nextInt(10) > 8) {
            queueBreaks++;
            done = true;
            Thread.sleep(350); // wait past streaming client poll time of 250ms
          }
        }
        if (client instanceof ConcurrentUpdateSolrClient) {
          ((ConcurrentUpdateSolrClient) client).blockUntilFinished();
        }
      }

      route = new HttpRoute(new HttpHost(url.getHost(), url.getPort(), isSSLMode() ? "https" : "http"));

      mConn = cm.requestConnection(route, HttpSolrClient.cacheKey);

      HttpClientConnection conn2 = getConn(mConn);

      HttpConnectionMetrics metrics = conn2.getMetrics();
      headerRequest(target, route, conn2, cm);

      cm.releaseConnection(conn2, null, -1, TimeUnit.MILLISECONDS);

      assertNotNull("No connection metrics found - is the connection getting aborted? server closing the connection? "
          + client.getClass().getSimpleName(), metrics);

      // we try and make sure the connection we get has handled all of the requests in this test
      if (client instanceof ConcurrentUpdateSolrClient) {
        // we can't fully control queue polling breaking up requests - allow a bit of leeway
        int exp = cnt1 + queueBreaks + 2;
        assertTrue(
            "We expected all communication via streaming client to use one connection! expected=" + exp + " got="
                + metrics.getRequestCount(),
            Math.max(exp, metrics.getRequestCount()) - Math.min(exp, metrics.getRequestCount()) < 3);
      } else {
        assertTrue("We expected all communication to use one connection! " + client.getClass().getSimpleName() + " "
            + metrics.getRequestCount(),
            cnt1 * cnt2 + 2 <= metrics.getRequestCount());
      }

    }
    finally {
      HttpClientUtil.close(httpClient);
    }
  }

  public HttpClientConnection getConn(ConnectionRequest mConn)
      throws InterruptedException, ConnectionPoolTimeoutException, ExecutionException {
    HttpClientConnection conn = mConn.get(30, TimeUnit.SECONDS);

    return conn;
  }

  public void headerRequest(HttpHost target, HttpRoute route, HttpClientConnection conn, PoolingHttpClientConnectionManager cm)
      throws IOException, HttpException {
    HttpRequest req = new BasicHttpRequest("OPTIONS", "*", HttpVersion.HTTP_1_1);

    req.addHeader("Host", target.getHostName());
    if (!conn.isOpen()) {
      // establish connection based on its route info
      cm.connect(conn, route, 1000, context);
      // and mark it as route complete
      cm.routeComplete(conn, route, context);
    }
    conn.sendRequestHeader(req);
    conn.flush();
    conn.receiveResponseHeader();
  }

  public ConnectionRequest getClientConnectionRequest(HttpClient httpClient, HttpRoute route, PoolingHttpClientConnectionManager cm) {
    ConnectionRequest mConn = cm.requestConnection(route, HttpSolrClient.cacheKey);
    return mConn;
  }

}

