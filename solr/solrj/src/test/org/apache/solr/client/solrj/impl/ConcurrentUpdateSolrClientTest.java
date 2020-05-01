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

import org.apache.http.HttpResponse;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ConcurrentUpdateSolrClientTest extends SolrJettyTestBase {

  /**
   * Mock endpoint where the CUSS being tested in this class sends requests.
   */
  public static class TestServlet extends HttpServlet 
    implements JavaBinUpdateRequestCodec.StreamingUpdateHandler
  {   
    private static final long serialVersionUID = 1L;

    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      numReqsRcvd.set(0);
      numDocsRcvd.set(0);
    }
    
    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String,String> headers = null;
    public static Map<String,String[]> parameters = null;
    public static AtomicInteger numReqsRcvd = new AtomicInteger(0);
    public static AtomicInteger numDocsRcvd = new AtomicInteger(0);
    
    public static void setErrorCode(Integer code) {
      errorCode = code;
    }
        
    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name, req.getHeader(name));
      }
    }

    private void setParameters(HttpServletRequest req) {
      //parameters = req.getParameterMap();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      
      numReqsRcvd.incrementAndGet();
      lastMethod = "post";
      recordRequest(req, resp);
            
      InputStream reqIn = req.getInputStream();
      JavaBinUpdateRequestCodec javabin = new JavaBinUpdateRequestCodec();
      for (;;) {
        try {
          javabin.unmarshal(reqIn, this);
        } catch (EOFException e) {
          break; // this is expected
        }
      }      
    }
    
    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      setParameters(req);
      if (null != errorCode) {
        try { 
          resp.sendError(errorCode); 
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in TestServlet", e);
        }
      }
    }

    @Override
    public void update(SolrInputDocument document, UpdateRequest req, Integer commitWithin, Boolean override) {
      numDocsRcvd.incrementAndGet();
    }
  } // end TestServlet
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(TestServlet.class), "/cuss/*")
        .withSSLConfig(sslConfig.buildServerSSLConfig())
        .build();
    createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);
  }
  
  @Test
  public void testConcurrentUpdate() throws Exception {
    TestServlet.clear();
    
    String serverUrl = jetty.getBaseUrl().toString() + "/cuss/foo";
        
    int cussThreadCount = 2;
    int cussQueueSize = 100;
    
    // for tracking callbacks from CUSS
    final AtomicInteger successCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);    
    final StringBuilder errors = new StringBuilder();     
    
    @SuppressWarnings("serial")
    ConcurrentUpdateSolrClient concurrentClient = new OutcomeCountingConcurrentUpdateSolrClient.Builder(serverUrl, successCounter, errorCounter, errors)
      .withQueueSize(cussQueueSize)
      .withThreadCount(cussThreadCount)
      .build();
    
    concurrentClient.setPollQueueTime(0);
    
    // ensure it doesn't block where there's nothing to do yet
    concurrentClient.blockUntilFinished();
    
    int poolSize = 5;
    ExecutorService threadPool = ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("testCUSS"));

    int numDocs = 100;
    int numRunnables = 5;
    for (int r=0; r < numRunnables; r++)
      threadPool.execute(new SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient));
    
    // ensure all docs are sent
    threadPool.awaitTermination(5, TimeUnit.SECONDS);
    threadPool.shutdown();
    
    // wait until all requests are processed by CUSS 
    concurrentClient.blockUntilFinished();
    concurrentClient.shutdownNow();
    
    assertEquals("post", TestServlet.lastMethod);
        
    // expect all requests to be successful
    int expectedSuccesses = TestServlet.numReqsRcvd.get();
    assertTrue(expectedSuccesses > 0); // at least one request must have been sent
    
    assertTrue("Expected no errors but got "+errorCounter.get()+
        ", due to: "+errors.toString(), errorCounter.get() == 0);
    assertTrue("Expected "+expectedSuccesses+" successes, but got "+successCounter.get(), 
        successCounter.get() == expectedSuccesses);
    
    int expectedDocs = numDocs * numRunnables;
    assertTrue("Expected CUSS to send "+expectedDocs+" but got "+TestServlet.numDocsRcvd.get(), 
        TestServlet.numDocsRcvd.get() == expectedDocs);
  }
  
  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    int cussThreadCount = 2;
    int cussQueueSize = 10;

    try (ConcurrentUpdateSolrClient concurrentClient
         = (new ConcurrentUpdateSolrClient.Builder(jetty.getBaseUrl().toString()))
         .withQueueSize(cussQueueSize)
         .withThreadCount(cussThreadCount).build()) {
      
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      concurrentClient.add("collection1", doc);
      concurrentClient.commit("collection1");

      assertEquals(1, concurrentClient.query("collection1", new SolrQuery("id:collection")).getResults().getNumFound());
    }

    try (ConcurrentUpdateSolrClient concurrentClient
         = (new ConcurrentUpdateSolrClient.Builder(jetty.getBaseUrl().toString() + "/collection1"))
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

    try (ConcurrentUpdateSolrClient concurrentClient
         = (new ConcurrentUpdateSolrClient.Builder(jetty.getBaseUrl().toString()))
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
        threadPool.execute(new SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient, "collection1"));

      // ensure all docs are sent
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
      threadPool.shutdown();

      concurrentClient.commit("collection1");

      assertEquals(expected, concurrentClient.query("collection1", new SolrQuery("*:*")).getResults().getNumFound());

      // wait until all requests are processed by CUSS 
      concurrentClient.blockUntilFinished();
      concurrentClient.shutdownNow();
    }

    try (ConcurrentUpdateSolrClient concurrentClient
         = (new ConcurrentUpdateSolrClient.Builder(jetty.getBaseUrl().toString() + "/collection1"))
         .withQueueSize(cussQueueSize)
         .withThreadCount(cussThreadCount).build()) {

      assertEquals(expected, concurrentClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }

  }

  static class SendDocsRunnable implements Runnable {
    
    private String id;
    private int numDocs;
    private SolrClient cuss;
    private String collection;
    
    SendDocsRunnable(String id, int numDocs, SolrClient cuss) {
      this(id, numDocs, cuss, null);
    }
    
    SendDocsRunnable(String id, int numDocs, SolrClient cuss, String collection) {
      this.id = id;
      this.numDocs = numDocs;
      this.cuss = cuss;
      this.collection = collection;
    }

    @Override
    public void run() {
      for (int d=0; d < numDocs; d++) {
        SolrInputDocument doc = new SolrInputDocument();
        String docId = id+"_"+d;
        doc.setField("id", docId);    
        UpdateRequest req = new UpdateRequest();
        req.add(doc);        
        try {
          if (this.collection == null)
            cuss.request(req);
          else
            cuss.request(req, this.collection);
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }      
    }    
  }

  static class OutcomeCountingConcurrentUpdateSolrClient extends ConcurrentUpdateSolrClient {
    private final AtomicInteger successCounter;
    private final AtomicInteger failureCounter;
    private final StringBuilder errors;
    
    public OutcomeCountingConcurrentUpdateSolrClient(Builder builder) {
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
    public void onSuccess(HttpResponse resp) {
      successCounter.incrementAndGet();
    }
    
    static class Builder extends ConcurrentUpdateSolrClient.Builder {
      protected final AtomicInteger successCounter;
      protected final AtomicInteger failureCounter;
      protected final StringBuilder errors;

      public Builder(String baseSolrUrl, AtomicInteger successCounter, AtomicInteger failureCounter, StringBuilder errors) {
        super(baseSolrUrl);
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
