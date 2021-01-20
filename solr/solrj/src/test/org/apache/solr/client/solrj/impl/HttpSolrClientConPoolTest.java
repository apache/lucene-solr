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
  private static String fooUrl;
  private static String barUrl;
  
  @BeforeClass
  public static void beforeTest() throws Exception {
    createAndStartJetty(legacyExampleCollection1SolrHome());
    // stealing the first made jetty
    yetty = jetty;
    barUrl = yetty.getBaseUrl().toString() + "/" + "collection1";
    
    createAndStartJetty(legacyExampleCollection1SolrHome());
    
    fooUrl = jetty.getBaseUrl().toString() + "/" + "collection1";
  }
  
  @AfterClass
  public static void stopYetty() throws Exception {
    if (null != yetty) {
      yetty.stop();
      yetty = null;
    }
  }
  
  public void testPoolSize() throws SolrServerException, IOException {
    PoolingHttpClientConnectionManager pool = HttpClientUtil.createPoolingConnectionManager();
    final HttpSolrClient client1 ;
    final String fooUrl;
    {
      fooUrl = jetty.getBaseUrl().toString() + "/" + "collection1";
      CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams(), pool,
            false /* let client shutdown it*/);
      client1 = getHttpSolrClient(fooUrl, httpClient, DEFAULT_CONNECTION_TIMEOUT);
    }
    final String barUrl = yetty.getBaseUrl().toString() + "/" + "collection1";
    
    {
      client1.setBaseURL(fooUrl);
      client1.deleteByQuery("*:*");
      client1.setBaseURL(barUrl);
      client1.deleteByQuery("*:*");
    }
    
    List<String> urls = new ArrayList<>();
    for(int i=0; i<17; i++) {
      urls.add(fooUrl);
    }
    for(int i=0; i<31; i++) {
      urls.add(barUrl);
    }
    
    Collections.shuffle(urls, random());
    
    try {
      int i=0;
      for (String url : urls) {
        if (!client1.getBaseURL().equals(url)) {
          client1.setBaseURL(url);
        }
        client1.add(new SolrInputDocument("id", ""+(i++)));
      }
      client1.setBaseURL(fooUrl);
      client1.commit();
      assertEquals(17, client1.query(new SolrQuery("*:*")).getResults().getNumFound());
      
      client1.setBaseURL(barUrl);
      client1.commit();
      assertEquals(31, client1.query(new SolrQuery("*:*")).getResults().getNumFound());
      
      PoolStats stats = pool.getTotalStats();
      assertEquals("oh "+stats, 2, stats.getAvailable());
    } finally {
      for (HttpSolrClient c : new HttpSolrClient []{ client1}) {
        HttpClientUtil.close(c.getHttpClient());
        c.close();
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
    try{
      final LBHttpSolrClient roundRobin = new LBHttpSolrClient.Builder().
                withBaseSolrUrl(fooUrl).
                withBaseSolrUrl(barUrl).
                withHttpClient(httpClient)
                .build();
      
      List<ConcurrentUpdateSolrClient> concurrentClients = Arrays.asList(
          new ConcurrentUpdateSolrClient.Builder(fooUrl)
          .withHttpClient(httpClient).withThreadCount(threadCount)
          .withQueueSize(10)
         .withExecutorService(threads).build(),
           new ConcurrentUpdateSolrClient.Builder(barUrl)
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
            final List<String> urls = Arrays.asList(fooUrl, barUrl);
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
    }
  }
}
