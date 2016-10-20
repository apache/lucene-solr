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
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressSysoutChecks;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.util.RevertDefaultThreadHandlerRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test of the MiniSolrCloudCluster functionality with authentication enabled.
 */
@LuceneTestCase.Slow
@SuppressSysoutChecks(bugUrl = "Solr logs to JUL")
public class TestAuthenticationFramework extends LuceneTestCase {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private int NUM_SERVERS = 5;
  private int NUM_SHARDS = 2;
  private int REPLICATION_FACTOR = 2;
  
  static String requestUsername = MockAuthenticationPlugin.expectedUsername;
  static String requestPassword = MockAuthenticationPlugin.expectedPassword;

  @Rule
  public TestRule solrTestRules = RuleChain
      .outerRule(new SystemPropertiesRestoreRule());

  @ClassRule
  public static TestRule solrClassRules = RuleChain.outerRule(
      new SystemPropertiesRestoreRule()).around(
      new RevertDefaultThreadHandlerRule());

  @Before
  public void setUp() throws Exception {
    setupAuthenticationPlugin();
    super.setUp();
  }
  
  private void setupAuthenticationPlugin() throws Exception {
    System.setProperty("authenticationPlugin", "org.apache.solr.cloud.TestAuthenticationFramework$MockAuthenticationPlugin");
    MockAuthenticationPlugin.expectedUsername = null;
    MockAuthenticationPlugin.expectedPassword = null;
  }
  
  @Test
  public void testBasics() throws Exception {

    MiniSolrCloudCluster miniCluster = createMiniSolrCloudCluster();
    try {
      // Should pass
      collectionCreateSearchDelete(miniCluster);

      MockAuthenticationPlugin.expectedUsername = "solr";
      MockAuthenticationPlugin.expectedPassword = "s0lrRocks";

      // Should fail with 401
      try {
        collectionCreateSearchDelete(miniCluster);
        fail("Should've returned a 401 error");
      } catch (Exception ex) {
        if (!ex.getMessage().contains("Error 401")) {
          fail("Should've returned a 401 error");
        }
      } finally {
        MockAuthenticationPlugin.expectedUsername = null;
        MockAuthenticationPlugin.expectedPassword = null;
      }
    } finally {
      miniCluster.shutdown();
    }
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty("authenticationPlugin");
    super.tearDown();
  }

  private MiniSolrCloudCluster createMiniSolrCloudCluster() throws Exception {
    JettyConfig.Builder jettyConfig = JettyConfig.builder();
    jettyConfig.waitForLoadingCoresToFinish(null);
    return new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), jettyConfig.build());
  }

  private void createCollection(MiniSolrCloudCluster miniCluster, String collectionName, String asyncId)
      throws Exception {
    String configName = "solrCloudCollectionConfig";
    miniCluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1/conf"), configName);

    final boolean persistIndex = random().nextBoolean();
    Map<String, String>  collectionProperties = new HashMap<>();

    collectionProperties.putIfAbsent(CoreDescriptor.CORE_CONFIG, "solrconfig-tlog.xml");
    collectionProperties.putIfAbsent("solr.tests.maxBufferedDocs", "100000");
    collectionProperties.putIfAbsent("solr.tests.ramBufferSizeMB", "100");
    // use non-test classes so RandomizedRunner isn't necessary
    if (random().nextBoolean()) {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICY, TieredMergePolicy.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "false");
    } else {
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_MERGEPOLICYFACTORY, TieredMergePolicyFactory.class.getName());
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICYFACTORY, "true");
      collectionProperties.putIfAbsent(SolrTestCaseJ4.SYSTEM_PROPERTY_SOLR_TESTS_USEMERGEPOLICY, "false");
    }
    collectionProperties.putIfAbsent("solr.tests.mergeScheduler", "org.apache.lucene.index.ConcurrentMergeScheduler");
    collectionProperties.putIfAbsent("solr.directoryFactory", (persistIndex ? "solr.StandardDirectoryFactory" : "solr.RAMDirectoryFactory"));

    if (asyncId == null) {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setProperties(collectionProperties)
          .process(miniCluster.getSolrClient());
    }
    else {
      CollectionAdminRequest.createCollection(collectionName, configName, NUM_SHARDS, REPLICATION_FACTOR)
          .setProperties(collectionProperties)
          .processAndWait(miniCluster.getSolrClient(), 30);
    }

  }

  public void collectionCreateSearchDelete(MiniSolrCloudCluster miniCluster) throws Exception {

    final String collectionName = "testcollection";

    final CloudSolrClient cloudSolrClient = miniCluster.getSolrClient();

    assertNotNull(miniCluster.getZkServer());
    List<JettySolrRunner> jettys = miniCluster.getJettySolrRunners();
    assertEquals(NUM_SERVERS, jettys.size());
    for (JettySolrRunner jetty : jettys) {
      assertTrue(jetty.isRunning());
    }

    // create collection
    log.info("#### Creating a collection");
    final String asyncId = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
    createCollection(miniCluster, collectionName, asyncId);

    ZkStateReader zkStateReader = miniCluster.getSolrClient().getZkStateReader();
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

    // modify/query collection
    log.info("#### updating a querying collection");
    cloudSolrClient.setDefaultCollection(collectionName);
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "1");
    cloudSolrClient.add(doc);
    cloudSolrClient.commit();
    SolrQuery query = new SolrQuery();
    query.setQuery("*:*");
    QueryResponse rsp = cloudSolrClient.query(query);
    assertEquals(1, rsp.getResults().getNumFound());

    // delete the collection we created earlier
    CollectionAdminRequest.deleteCollection(collectionName).process(miniCluster.getSolrClient());

    // create it again
    String asyncId2 = (random().nextBoolean() ? null : "asyncId("+collectionName+".create)="+random().nextInt());
    createCollection(miniCluster, collectionName, asyncId2);
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(collectionName, zkStateReader, true, true, 330);

    // check that there's no left-over state
    assertEquals(0, cloudSolrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    cloudSolrClient.add(doc);
    cloudSolrClient.commit();
    assertEquals(1, cloudSolrClient.query(new SolrQuery("*:*")).getResults().getNumFound());
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
      
      log.info("Username: "+username+", password: "+password);
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
      interceptor = new HttpRequestInterceptor() {
        @Override
        public void process(HttpRequest req, HttpContext rsp) throws HttpException, IOException {
          req.addHeader("username", requestUsername);
          req.addHeader("password", requestPassword);
        }
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
