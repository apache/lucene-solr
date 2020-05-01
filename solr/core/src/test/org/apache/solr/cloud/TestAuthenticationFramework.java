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
package org.apache.solr.cloud;

import javax.servlet.FilterChain;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.http.HttpRequestInterceptor;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the MiniSolrCloudCluster functionality with authentication enabled.
 */
@LuceneTestCase.Slow
public class TestAuthenticationFramework extends SolrCloudTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final int numShards = 2;
  private static final int numReplicas = 2;
  private static final int maxShardsPerNode = 2;
  private static final int nodeCount = (numShards*numReplicas + (maxShardsPerNode-1))/maxShardsPerNode;
  private static final String configName = "solrCloudCollectionConfig";
  private static final String collectionName = "testcollection";

  static String requestUsername = MockAuthenticationPlugin.expectedUsername;
  static String requestPassword = MockAuthenticationPlugin.expectedPassword;

  @Override
  public void setUp() throws Exception {
    setupAuthenticationPlugin();
    configureCluster(nodeCount).addConfig(configName, configset("cloud-minimal")).configure();
    super.setUp();
  }
  
  private void setupAuthenticationPlugin() throws Exception {
    System.setProperty("authenticationPlugin", "org.apache.solr.cloud.TestAuthenticationFramework$MockAuthenticationPlugin");
    MockAuthenticationPlugin.expectedUsername = null;
    MockAuthenticationPlugin.expectedPassword = null;
  }
  
  @Test
  public void testBasics() throws Exception {
    collectionCreateSearchDeleteTwice();

    MockAuthenticationPlugin.expectedUsername = "solr";
    MockAuthenticationPlugin.expectedPassword = "s0lrRocks";

    // Should fail with 401
    try {
      HttpSolrClient.RemoteSolrException e = expectThrows(HttpSolrClient.RemoteSolrException.class,
          this::collectionCreateSearchDeleteTwice);
      assertTrue("Should've returned a 401 error", e.getMessage().contains("Error 401"));
    } finally {
      MockAuthenticationPlugin.expectedUsername = null;
      MockAuthenticationPlugin.expectedPassword = null;
    }
  }

  @Override
  public void tearDown() throws Exception {
    System.clearProperty("authenticationPlugin");
    shutdownCluster();
    super.tearDown();
  }

  private void createCollection(String collectionName)
      throws Exception {
    if (random().nextBoolean()) {  // process asynchronously
      CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
          .setMaxShardsPerNode(maxShardsPerNode)
          .processAndWait(cluster.getSolrClient(), 90);
      cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    }
    else {
      CollectionAdminRequest.createCollection(collectionName, configName, numShards, numReplicas)
          .setMaxShardsPerNode(maxShardsPerNode)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collectionName, numShards, numShards * numReplicas);
    }

  }

  public void collectionCreateSearchDeleteTwice() throws Exception {
    final CloudSolrClient client = cluster.getSolrClient();

    for (int i = 0 ; i < 2 ; ++i) {
      // create collection
      createCollection(collectionName);

      // check that there's no left-over state
      assertEquals(0, client.query(collectionName, new SolrQuery("*:*")).getResults().getNumFound());

      // modify/query collection
      Thread.sleep(100); // not everyone is up to date just because we waited to make sure one was - pause a moment
      new UpdateRequest().add("id", "1").commit(client, collectionName);
      QueryResponse rsp = client.query(collectionName, new SolrQuery("*:*"));
      assertEquals(1, rsp.getResults().getNumFound());

      // delete the collection
     cluster.deleteAllCollections();
    }
  }

  public static class MockAuthenticationPlugin extends AuthenticationPlugin implements HttpClientBuilderPlugin {
    public static String expectedUsername;
    public static String expectedPassword;
    private HttpRequestInterceptor interceptor;
    @Override
    public void init(Map<String,Object> pluginConfig) {}

    @Override
    public boolean doAuthenticate(ServletRequest request, ServletResponse response, FilterChain filterChain)
        throws Exception {
      if (expectedUsername == null) {
        filterChain.doFilter(request, response);
        return true;
      }
      HttpServletRequest httpRequest = (HttpServletRequest)request;
      String username = httpRequest.getHeader("username");
      String password = httpRequest.getHeader("password");
      
      log.info("Username: {}, password: {}", username, password);
      if(MockAuthenticationPlugin.expectedUsername.equals(username) && MockAuthenticationPlugin.expectedPassword.equals(password)) {
        filterChain.doFilter(request, response);
        return true;
      } else {
        ((HttpServletResponse)response).sendError(401, "Unauthorized request");
        return false;
      }
    }

    @Override
    public SolrHttpClientBuilder getHttpClientBuilder(SolrHttpClientBuilder httpClientBuilder) {
      interceptor = (req, rsp) -> {
        req.addHeader("username", requestUsername);
        req.addHeader("password", requestPassword);
      };

      HttpClientUtil.addRequestInterceptor(interceptor);
      return httpClientBuilder;
    }

    @Override
    public void close() {
      HttpClientUtil.removeRequestInterceptor(interceptor);
    }
    
  }
}
