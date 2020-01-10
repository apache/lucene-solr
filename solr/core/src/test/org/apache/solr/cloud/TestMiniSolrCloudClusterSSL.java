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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.TestRuleRestoreSystemProperties;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.util.SSLTestConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests various permutations of SSL options with {@link MiniSolrCloudCluster}.
 * <b>NOTE: This Test ignores the randomized SSL &amp; clientAuth settings selected by base class</b>,
 * instead each method initializes a {@link SSLTestConfig} will specific combinations of settings to test.
 *
 * @see TestSSLRandomization
 */
public class TestMiniSolrCloudClusterSSL extends SolrTestCaseJ4 {

  private static final SSLContext DEFAULT_SSL_CONTEXT;
  static {
    try {
      DEFAULT_SSL_CONTEXT = SSLContext.getDefault();
      assert null != DEFAULT_SSL_CONTEXT;
    } catch (Exception e) {
      throw new RuntimeException("Unable to initialize 'Default' SSLContext Algorithm, JVM is borked", e);
    }
  }
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final int NUM_SERVERS = 3;
  public static final String CONF_NAME = MethodHandles.lookup().lookupClass().getName();
  
  @Rule
  public TestRule syspropRestore = new TestRuleRestoreSystemProperties
    (HttpClientUtil.SYS_PROP_CHECK_PEER_NAME);
  
  @Before
  public void before() {
    // undo the randomization of our super class
    log.info("NOTE: This Test ignores the randomized SSL & clientAuth settings selected by base class");
    HttpClientUtil.resetHttpClientBuilder(); // also resets SchemaRegistryProvider
    Http2SolrClient.resetSslContextFactory();
    System.clearProperty(ZkStateReader.URL_SCHEME);
  }
  @After
  public void after() {
    HttpClientUtil.resetHttpClientBuilder(); // also resets SchemaRegistryProvider
    Http2SolrClient.resetSslContextFactory();
    System.clearProperty(ZkStateReader.URL_SCHEME);
    SSLContext.setDefault(DEFAULT_SSL_CONTEXT);
  }
  
  public void testNoSsl() throws Exception {
    final SSLTestConfig sslConfig = new SSLTestConfig(false, false);
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "http");
    checkClusterWithNodeReplacement(sslConfig);
  }
  
  public void testNoSslButSillyClientAuth() throws Exception {
    // this combination doesn't really make sense, since ssl==false the clientauth option will be ignored
    // but we test it anyway for completeness of sanity checking the behavior of code that looks at those
    // options.
    final SSLTestConfig sslConfig = new SSLTestConfig(false, true);
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "http");
    checkClusterWithNodeReplacement(sslConfig);
  }
  
  public void testSslAndNoClientAuth() throws Exception {
    final SSLTestConfig sslConfig = new SSLTestConfig(true, false);
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "https");
    checkClusterWithNodeReplacement(sslConfig);
  }

  public void testSslAndClientAuth() throws Exception {
    assumeFalse("SOLR-9039: SSL w/clientAuth does not work on MAC_OS_X", Constants.MAC_OS_X);
    
    final SSLTestConfig sslConfig = new SSLTestConfig(true, true);

    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "https");
    checkClusterWithNodeReplacement(sslConfig);
  }

  // commented out on: 17-Feb-2019   @LuceneTestCase.BadApple(bugUrl="https://issues.apache.org/jira/browse/SOLR-12028") // 2-Aug-2018
  public void testSslWithCheckPeerName() throws Exception {
    final SSLTestConfig sslConfig = new SSLTestConfig(true, false, true);
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "https");
    checkClusterWithNodeReplacement(sslConfig);
  }
  
  /**
   * Constructs a cluster with the specified sslConfigs, runs {@link #checkClusterWithCollectionCreations}, 
   * then verifies that if we modify the default SSLContext (mimicing <code>javax.net.ssl.*</code> 
   * sysprops set on JVM startup) and reset to the default HttpClientBuilder, new HttpSolrClient instances 
   * will still be able to talk to our servers.
   *
   * @see SSLContext#setDefault
   * @see HttpClientUtil#resetHttpClientBuilder
   * @see #checkClusterWithCollectionCreations
   */
  private void checkClusterWithNodeReplacement(SSLTestConfig sslConfig) throws Exception {
    
    final JettyConfig config = JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build();
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), config);
    try {
      checkClusterWithCollectionCreations(cluster, sslConfig);

      
      // Change the defaul SSLContext to match our test config, or to match our original system default if
      // our test config doesn't use SSL, and reset HttpClientUtil to it's defaults so it picks up our
      // SSLContext that way.
      SSLContext.setDefault( sslConfig.isSSLMode() ? sslConfig.buildClientSSLContext() : DEFAULT_SSL_CONTEXT);
      System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME,
                         Boolean.toString(sslConfig.getCheckPeerName()));
      HttpClientUtil.resetHttpClientBuilder();
      Http2SolrClient.resetSslContextFactory();
      
      // recheck that we can communicate with all the jetty instances in our cluster
      checkClusterJettys(cluster, sslConfig);
    } finally {
      cluster.shutdown();
    }
  }

  /** Sanity check that our test scaffolding for validating SSL peer names fails when it should */
  public void testSslWithInvalidPeerName() throws Exception {
    // NOTE: first initialize the cluster w/o peer name checks, which means our server will use
    // certs with a bogus hostname/ip and clients shouldn't care...
    final SSLTestConfig sslConfig = new SSLTestConfig(true, false, false);
    HttpClientUtil.setSchemaRegistryProvider(sslConfig.buildClientSchemaRegistryProvider());
    Http2SolrClient.setDefaultSSLConfig(sslConfig.buildClientSSLConfig());
    System.setProperty(ZkStateReader.URL_SCHEME, "https");
    final JettyConfig config = JettyConfig.builder().withSSLConfig(sslConfig.buildServerSSLConfig()).build();
    final MiniSolrCloudCluster cluster = new MiniSolrCloudCluster(NUM_SERVERS, createTempDir(), config);
    try {
      checkClusterWithCollectionCreations(cluster, sslConfig);
      
      // now initialize a client that still uses the existing SSLContext/Provider, so it will accept
      // our existing certificate, but *does* care about validating the peer name
      System.setProperty(HttpClientUtil.SYS_PROP_CHECK_PEER_NAME, "true");
      HttpClientUtil.resetHttpClientBuilder();
      Http2SolrClient.resetSslContextFactory();

      // and validate we get failures when trying to talk to our cluster...
      final List<JettySolrRunner> jettys = cluster.getJettySolrRunners();
      for (JettySolrRunner jetty : jettys) {
        final String baseURL = jetty.getBaseUrl().toString();
        // verify new solr clients validate peer name and can't talk to this server
        Exception ex = expectThrows(SolrServerException.class, () -> {
            try (HttpSolrClient client = getRandomizedHttpSolrClient(baseURL)) {
              CoreAdminRequest req = new CoreAdminRequest();
              req.setAction( CoreAdminAction.STATUS );
              client.request(req);
            }
          });
        assertTrue("Expected an root cause SSL Exception, got: " + ex.toString(),
                   ex.getCause() instanceof SSLException);
      }
    } finally {
      cluster.shutdown();
    }


    
  }

  /**
   * General purpose cluster sanity check...
   * <ol>
   * <li>Upload a config set</li>
   * <li>verifies a collection can be created</li>
   * <li>verifies many things that should succeed/fail when communicating with the cluster according to the specified sslConfig</li>
   * <li>shutdown a server &amp; startup a new one in it's place</li>
   * <li>repeat the verifications of ssl / no-ssl communication</li>
   * <li>create a second collection</li>
   * </ol>
   * @see #CONF_NAME
   * @see #NUM_SERVERS
   */
  public static void checkClusterWithCollectionCreations(final MiniSolrCloudCluster cluster,
                                                         final SSLTestConfig sslConfig) throws Exception {

    cluster.uploadConfigSet(SolrTestCaseJ4.TEST_PATH().resolve("collection1").resolve("conf"), CONF_NAME);
    
    checkCreateCollection(cluster, "first_collection");
    
    checkClusterJettys(cluster, sslConfig);
    
    // shut down a server
    JettySolrRunner stoppedServer = cluster.stopJettySolrRunner(0);
    cluster.waitForJettyToStop(stoppedServer);
    assertTrue(stoppedServer.isStopped());
    assertEquals(NUM_SERVERS - 1, cluster.getJettySolrRunners().size());
    
    // create a new server
    JettySolrRunner startedServer = cluster.startJettySolrRunner();
    cluster.waitForAllNodes(30);
    assertTrue(startedServer.isRunning());
    assertEquals(NUM_SERVERS, cluster.getJettySolrRunners().size());
    
    checkClusterJettys(cluster, sslConfig);
    
    checkCreateCollection(cluster, "second_collection");
  }
  
  /**
   * Verify that we can create a collection that involves one replica per node using the
   * CloudSolrClient available for the cluster
   */
  private static void checkCreateCollection(final MiniSolrCloudCluster cluster,
                                            final String collection) throws Exception {
    final CloudSolrClient cloudClient = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collection, CONF_NAME, NUM_SERVERS, 1)
        .withProperty("config", "solrconfig-tlog.xml")
        .process(cloudClient);
    cluster.waitForActiveCollection(collection, NUM_SERVERS, NUM_SERVERS);
    assertEquals("sanity query", 0, cloudClient.query(collection, params("q","*:*")).getStatus());
  }
  
  /** 
   * verify that we can query all of the Jetty instances the specified cluster using the expected
   * options (based on the sslConfig), and that we can <b>NOT</b> query the Jetty instances in 
   * specified cluster in the ways that should fail (based on the sslConfig)
   *
   * @see #getRandomizedHttpSolrClient
   */
  private static void checkClusterJettys(final MiniSolrCloudCluster cluster,
                                         final SSLTestConfig sslConfig) throws Exception {

    final boolean ssl = sslConfig.isSSLMode();
    List<JettySolrRunner> jettys = cluster.getJettySolrRunners();

    for (JettySolrRunner jetty : jettys) {
      final String baseURL = jetty.getBaseUrl().toString();

      // basic base URL sanity checks
      assertTrue("WTF baseURL: " + baseURL, null != baseURL && 10 < baseURL.length());
      assertEquals("http vs https: " + baseURL,
                   ssl ? "https" : "http:", baseURL.substring(0,5));
      
      // verify solr client success with expected protocol
      try (HttpSolrClient client = getRandomizedHttpSolrClient(baseURL)) {
        assertEquals(0, CoreAdminRequest.getStatus(/* all */ null, client).getStatus());
      }
      
      // sanity check the HttpClient used under the hood by our the cluster's CloudSolrClient
      // ensure it has the necessary protocols/credentials for each jetty server
      //
      // NOTE: we're not responsible for closing the cloud client
      final HttpClient cloudClient = cluster.getSolrClient().getLbClient().getHttpClient();
      try (HttpSolrClient client = getRandomizedHttpSolrClient(baseURL)) {
        assertEquals(0, CoreAdminRequest.getStatus(/* all */ null, client).getStatus());
      }

      final String wrongBaseURL = baseURL.replaceFirst((ssl ? "https://" : "http://"),
                                                       (ssl ? "http://" : "https://"));
          
      // verify solr client using wrong protocol can't talk to server
      expectThrows(SolrServerException.class, () -> {
          try (HttpSolrClient client = getRandomizedHttpSolrClient(wrongBaseURL)) {
            CoreAdminRequest req = new CoreAdminRequest();
            req.setAction( CoreAdminAction.STATUS );
            client.request(req);
          }
        });
      
      if (! sslConfig.isClientAuthMode()) {
        // verify simple HTTP(S) client can't do HEAD request for URL with wrong protocol
        try (CloseableHttpClient client = getSslAwareClientWithNoClientCerts()) {
          final String wrongUrl = wrongBaseURL + "/admin/cores";
          // vastly diff exception details between plain http vs https, not worried about details here
          expectThrows(IOException.class, () -> {
              doHeadRequest(client, wrongUrl);
            });
        }
      }
      
      if (ssl) {
        // verify expected results for a HEAD request to valid URL from HTTP(S) client w/o client certs
        try (CloseableHttpClient client = getSslAwareClientWithNoClientCerts()) {
          final String url = baseURL + "/admin/cores";
          if (sslConfig.isClientAuthMode()) {
            // w/o a valid client cert, SSL connection should fail

            expectThrows(IOException.class, () -> {
                doHeadRequest(client, url);
              });
          } else {
            assertEquals("Wrong status for head request ("+url+") when clientAuth="
                         + sslConfig.isClientAuthMode(),
                         200, doHeadRequest(client, url));
          }
        }
      }

    }
  }

  /** 
   * Trivial helper method for doing a HEAD request of the specified URL using the specified client 
   * and getting the HTTP statusCode from the response
   */
  private static int doHeadRequest(final CloseableHttpClient client, final String url) throws Exception {
    return client.execute(new HttpHead(url)).getStatusLine().getStatusCode();
  }
  
  /**
   * Returns a new HttpClient that supports both HTTP and HTTPS (with the default test truststore), but 
   * has no keystore -- so servers requiring client authentication should fail.
   */
  private static CloseableHttpClient getSslAwareClientWithNoClientCerts() throws Exception {
    
    // NOTE: This method explicitly does *NOT* use HttpClientUtil code because that
    // will muck with the global static HttpClientBuilder / SchemeRegistryProvider
    // and we can't do that and still test the entire purpose of what we are trying to test here.

    final SSLTestConfig clientConfig = new SSLTestConfig(true, false);
    
    final SSLConnectionSocketFactory sslFactory = clientConfig.buildClientSSLConnectionSocketFactory();
    assert null != sslFactory;

    final Registry<ConnectionSocketFactory> socketFactoryReg = 
      RegistryBuilder.<ConnectionSocketFactory> create()
      .register("https", sslFactory)
      .register("http", PlainConnectionSocketFactory.INSTANCE )
      .build();
    
    final HttpClientBuilder builder = HttpClientBuilder.create();
    builder.setConnectionManager(new PoolingHttpClientConnectionManager(socketFactoryReg));

    return builder.build();
  }

  /** 
   * Generates an HttpSolrClient, either by using the test framework helper method or by direct 
   * instantiation (determined randomly)
   * @see #getHttpSolrClient
   */
  public static HttpSolrClient getRandomizedHttpSolrClient(String url) {
    // NOTE: at the moment, SolrTestCaseJ4 already returns "new HttpSolrClient" most of the time,
    // so this method may seem redundant -- but the point here is to sanity check 2 things:
    // 1) a direct test that "new HttpSolrClient" works given the current JVM/sysprop defaults
    // 2) a sanity check that whatever getHttpSolrClient(String) returns will work regardless of
    //    current test configuration.
    // ... so we are hopefully future proofing against possible changes to SolrTestCaseJ4.getHttpSolrClient
    // that "optimize" the test client construction in a way that would prevent us from finding bugs with
    // regular HttpSolrClient instantiation.
    if (random().nextBoolean()) {
      return (new HttpSolrClient.Builder(url)).build();
    } // else...
    return getHttpSolrClient(url);
  }
}
