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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class HttpSolrClientConPoolTest extends SolrJettyTestBase {

  protected static JettySolrRunner yetty;
  private static String jettyUrl;
  private static String yettyUrl;
  protected static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeTest() throws Exception {
    yetty = createAndStartJetty(legacyExampleCollection1SolrHome());
    yettyUrl = yetty.getBaseUrl() + "/" + "collection1";
    
    jetty = createAndStartJetty(legacyExampleCollection1SolrHome());
    jettyUrl = jetty.getBaseUrl() + "/" + "collection1";
  }
  
  @AfterClass
  public static void stopYetty() throws Exception {
    if (null != yetty) {
      yetty.stop();
      yetty = null;
    }
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
  }
  
  public void testPoolSize() throws SolrServerException, IOException {

    Http2SolrClient client1 = null;
    try {
      client1 = getHttpSolrClient(jettyUrl);
      client1.setBaseUrl(jettyUrl);
      client1.deleteByQuery("*:*");
      client1.setBaseUrl(yettyUrl);
      client1.deleteByQuery("*:*");

      List<String> urls = new ArrayList<>();
      for (int i = 0; i < 17; i++) {
        urls.add(jettyUrl);
      }
      for (int i = 0; i < 31; i++) {
        urls.add(yettyUrl);
      }

      Collections.shuffle(urls, random());


      int i = 0;
      for (String url : urls) {
        if (!client1.getBaseURL().equals(url)) {
          client1.setBaseUrl(url);
        }
        client1.add(new SolrInputDocument("id", "" + (i++)));
      }
      client1.setBaseUrl(jettyUrl);
      client1.commit();
      assertEquals(17, client1.query(new SolrQuery("*:*")).getResults().getNumFound());

      client1.setBaseUrl(yettyUrl);
      client1.commit();
      assertEquals(31, client1.query(new SolrQuery("*:*")).getResults().getNumFound());

      // PoolStats stats = pool.getTotalStats();
      //assertEquals("oh "+stats, 2, stats.getAvailable());
    } finally {
      if (client1 != null) {
        client1.close();
      }
    }
  }
  

  public void testLBClient() throws IOException, SolrServerException {
    
    PoolingHttpClientConnectionManager pool = HttpClientUtil.createPoolingConnectionManager();
    final HttpSolrClient client1 ;
    int threadCount = atLeast(2);
    final ExecutorService threads = ExecutorUtil.newMDCAwareFixedThreadPool(threadCount,
        new SolrNamedThreadFactory(getClass().getSimpleName()+"TestScheduler"));
    CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams(), pool);
    LBHttpSolrClient roundRobin = null;
    try{
      roundRobin = new LBHttpSolrClient.Builder().
                withBaseSolrUrl(jettyUrl).
                withBaseSolrUrl(yettyUrl).
                withHttpClient(httpClient)
                .build();
      
      List<ConcurrentUpdateSolrClient> concurrentClients = Arrays.asList(
          new ConcurrentUpdateSolrClient.Builder(jettyUrl)
          .withHttpClient(httpClient).withThreadCount(threadCount)
          .withQueueSize(10)
         .withExecutorService(threads).build(),
           new ConcurrentUpdateSolrClient.Builder(yettyUrl)
          .withHttpClient(httpClient).withThreadCount(threadCount)
          .withQueueSize(10)
         .withExecutorService(threads).build()); 
      
      for (int i=0; i<2; i++) {
        roundRobin.deleteByQuery("*:*");
      }
      
      for (int i=0; i<57; i++) {
        final SolrInputDocument doc = new SolrInputDocument("id", ""+i);
        if (random().nextBoolean()) {
          final ConcurrentUpdateSolrClient concurrentClient = concurrentClients.get(random().nextInt(concurrentClients.size()));
          concurrentClient.add(doc); // here we are testing that CUSC and plain clients reuse pool 
          concurrentClient.blockUntilFinished();
        } else {
          if (random().nextBoolean()) {
            roundRobin.add(doc);
          } else {
            final UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.add(doc); // here we mimic CloudSolrClient impl
            final List<String> urls = Arrays.asList(jettyUrl, yettyUrl);
            Collections.shuffle(urls, random());
            LBHttpSolrClient.Req req = new LBHttpSolrClient.Req(updateRequest, 
                    urls);
             roundRobin.request(req);
          }
        }
      }
      
      for (int i=0; i<2; i++) {
        roundRobin.commit();
      }
      int total=0;
      for (int i=0; i<2; i++) {
        total += roundRobin.query(new SolrQuery("*:*")).getResults().getNumFound();
      }
      assertEquals(57, total);
      PoolStats stats = pool.getTotalStats();
      //System.out.println("\n"+stats);
      assertEquals("expected number of connections shouldn't exceed number of endpoints" + stats, 
          2, stats.getAvailable());
    }finally {
      threads.shutdown();
      HttpClientUtil.close(httpClient);
      if (roundRobin != null) {
        roundRobin.close();
      }
    }
  }
}
