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
import java.util.Collections;
import java.util.List;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.pool.PoolStats;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.RandomizeSSL;
import org.junit.AfterClass;
import org.junit.BeforeClass;

@RandomizeSSL(1.0)
public class HttpSolrClientSSLAuthConPoolTest extends SolrJettyTestBase {

  private static JettySolrRunner yetty;

  @BeforeClass
  public static void beforeTest() throws Exception {
    createJetty(legacyExampleCollection1SolrHome());
    // stealing the first made jetty
    yetty = jetty;
    createJetty(legacyExampleCollection1SolrHome());
  }
  
  @AfterClass
  public static void stopYetty() throws Exception {
    yetty.stop();
    yetty = null;
  }
  
  public void testPoolSize() throws SolrServerException, IOException {
    PoolingHttpClientConnectionManager pool = HttpClientUtil.createPoolingConnectionManager();
    final HttpSolrClient client1 ;
    final String fooUrl;
    {
      fooUrl = jetty.getBaseUrl().toString() + "/" + "collection1";
      CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams(), pool,
            false /* let client shutdown it*/);
      client1 = getHttpSolrClient(fooUrl, httpClient);
      client1.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
    }
    final String barUrl = yetty.getBaseUrl().toString() + "/" + "collection1";
    
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
}
