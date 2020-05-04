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
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConcurrentUpdateHttp2SolrClientTest extends SolrJettyTestBase {


  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(ConcurrentUpdateSolrClientTest.TestServlet.class), "/cuss/*")
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }

  @Test
  public void testConcurrentUpdate() throws Exception {
    ConcurrentUpdateSolrClientTest.TestServlet.clear();

    String serverUrl = jetty.getBaseUrl().toString() + "/cuss/foo";

    int cussThreadCount = 2;
    int cussQueueSize = 100;

    // for tracking callbacks from CUSS
    final AtomicInteger successCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);
    final StringBuilder errors = new StringBuilder();

    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         ConcurrentUpdateHttp2SolrClient concurrentClient = new OutcomeCountingConcurrentUpdateSolrClient.Builder(serverUrl, http2Client, successCounter, errorCounter, errors)
             .withQueueSize(cussQueueSize)
             .withThreadCount(cussThreadCount)
             .build()) {
      concurrentClient.setPollQueueTime(0);

      // ensure it doesn't block where there's nothing to do yet
      concurrentClient.blockUntilFinished();

      int poolSize = 5;
      ExecutorService threadPool = ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("testCUSS"));

      int numDocs = 100;
      int numRunnables = 5;
      for (int r=0; r < numRunnables; r++)
        threadPool.execute(new ConcurrentUpdateSolrClientTest.SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient));

      // ensure all docs are sent
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
      threadPool.shutdown();

      // wait until all requests are processed by CUSS
      concurrentClient.blockUntilFinished();
      concurrentClient.shutdownNow();

      assertEquals("post", ConcurrentUpdateSolrClientTest.TestServlet.lastMethod);

      // expect all requests to be successful
      int expectedSuccesses = ConcurrentUpdateSolrClientTest.TestServlet.numReqsRcvd.get();
      assertTrue(expectedSuccesses > 0); // at least one request must have been sent

      assertTrue("Expected no errors but got "+errorCounter.get()+
          ", due to: "+errors.toString(), errorCounter.get() == 0);
      assertTrue("Expected "+expectedSuccesses+" successes, but got "+successCounter.get(),
          successCounter.get() == expectedSuccesses);

      int expectedDocs = numDocs * numRunnables;
      assertTrue("Expected CUSS to send "+expectedDocs+" but got "+ ConcurrentUpdateSolrClientTest.TestServlet.numDocsRcvd.get(),
          ConcurrentUpdateSolrClientTest.TestServlet.numDocsRcvd.get() == expectedDocs);
    }



  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    int cussThreadCount = 2;
    int cussQueueSize = 10;

    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
        ConcurrentUpdateHttp2SolrClient concurrentClient
             = (new ConcurrentUpdateHttp2SolrClient.Builder(jetty.getBaseUrl().toString(), http2Client))
        .withQueueSize(cussQueueSize)
        .withThreadCount(cussThreadCount).build()) {

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      concurrentClient.add("collection1", doc);
      concurrentClient.commit("collection1");

      assertEquals(1, concurrentClient.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());
    }

    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         ConcurrentUpdateHttp2SolrClient concurrentClient
             = new ConcurrentUpdateHttp2SolrClient.Builder(jetty.getBaseUrl().toString() + "/collection1", http2Client)
             .withQueueSize(cussQueueSize)
             .withThreadCount(cussThreadCount).build()) {

      assertEquals(1, concurrentClient.query(new SolrQuery("id:collection")).getResults().getNumFound());
    }

  }

  @Test
  public void testConcurrentCollectionUpdate() throws Exception {

    int cussThreadCount = 2;
    int cussQueueSize = 100;
    int numDocs = 100;
    int numRunnables = 5;
    int expected = numDocs * numRunnables;

    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         ConcurrentUpdateHttp2SolrClient concurrentClient
             = new ConcurrentUpdateHttp2SolrClient.Builder(jetty.getBaseUrl().toString(), http2Client)
             .withQueueSize(cussQueueSize)
             .withThreadCount(cussThreadCount).build()) {
      concurrentClient.setPollQueueTime(0);

      // ensure it doesn't block where there's nothing to do yet
      concurrentClient.blockUntilFinished();

      // Delete all existing documents.
      concurrentClient.deleteByQuery("collection1", "*:*");

      int poolSize = 5;
      ExecutorService threadPool = ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("testCUSS"));

      for (int r=0; r < numRunnables; r++)
        threadPool.execute(new ConcurrentUpdateSolrClientTest.SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient, "collection1"));

      // ensure all docs are sent
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
      threadPool.shutdown();

      concurrentClient.commit("collection1");

      assertEquals(expected, concurrentClient.query("collection1", new SolrQuery("*:*")).getResults().getNumFound());

      // wait until all requests are processed by CUSS
      concurrentClient.blockUntilFinished();
      concurrentClient.shutdownNow();
    }

    try (Http2SolrClient http2Client = new Http2SolrClient.Builder().build();
         ConcurrentUpdateHttp2SolrClient concurrentClient
             = new ConcurrentUpdateHttp2SolrClient.Builder(jetty.getBaseUrl().toString() + "/collection1", http2Client)
             .withQueueSize(cussQueueSize)
             .withThreadCount(cussThreadCount).build()) {

      assertEquals(expected, concurrentClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

  }

  static class OutcomeCountingConcurrentUpdateSolrClient extends ConcurrentUpdateHttp2SolrClient {
    private final AtomicInteger successCounter;
    private final AtomicInteger failureCounter;
    private final StringBuilder errors;

    public OutcomeCountingConcurrentUpdateSolrClient(OutcomeCountingConcurrentUpdateSolrClient.Builder builder) {
      super(builder);
      this.successCounter = builder.successCounter;
      this.failureCounter = builder.failureCounter;
      this.errors = builder.errors;
    }

    @Override
    public void handleError(Throwable ex) {
      failureCounter.incrementAndGet();
      errors.append(" "+ex);
    }

    @Override
    public void onSuccess(Response resp, InputStream respBody) {
      successCounter.incrementAndGet();
    }

    static class Builder extends ConcurrentUpdateHttp2SolrClient.Builder {
      protected final AtomicInteger successCounter;
      protected final AtomicInteger failureCounter;
      protected final StringBuilder errors;

      public Builder(String baseSolrUrl, Http2SolrClient http2Client, AtomicInteger successCounter, AtomicInteger failureCounter, StringBuilder errors) {
        super(baseSolrUrl, http2Client);
        this.successCounter = successCounter;
        this.failureCounter = failureCounter;
        this.errors = errors;
      }

      public OutcomeCountingConcurrentUpdateSolrClient build() {
        return new OutcomeCountingConcurrentUpdateSolrClient(this);
      }
    }
  }
}
