package org.apache.solr;

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

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.util.ExternalPaths;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.solr.util.RESTfulServerProvider;
import org.apache.solr.util.RestTestHarness;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.restlet.ext.servlet.ServerServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class SolrJettyTestBase extends SolrTestCaseJ4 
{
  private static Logger log = LoggerFactory.getLogger(SolrJettyTestBase.class);

  // Try not introduce a dependency on the example schema or config unless you need to.
  // using configs in the test directory allows more flexibility to change "example"
  // without breaking configs.
  public String getSolrHome() { return ExternalPaths.EXAMPLE_HOME; }

  private static boolean manageSslProps = true;
  private static final File TEST_KEYSTORE = new File(ExternalPaths.SOURCE_HOME, 
                                                     "example/etc/solrtest.keystore");
  private static final Map<String,String> SSL_PROPS = new HashMap<String,String>();
  static {
    SSL_PROPS.put("tests.jettySsl","false");
    SSL_PROPS.put("tests.jettySsl.clientAuth","false");
    SSL_PROPS.put("javax.net.ssl.keyStore", TEST_KEYSTORE.getAbsolutePath());
    SSL_PROPS.put("javax.net.ssl.keyStorePassword","secret");
    SSL_PROPS.put("javax.net.ssl.trustStore", TEST_KEYSTORE.getAbsolutePath());
    SSL_PROPS.put("javax.net.ssl.trustStorePassword","secret");
  }

  @BeforeClass
  public static void beforeSolrJettyTestBase() throws Exception {

    // consume the same amount of random no matter what
    final boolean trySsl = random().nextBoolean();
    final boolean trySslClientAuth = random().nextBoolean();
    
    // only randomize SSL if none of the SSL_PROPS are already set
    final Map<Object,Object> sysprops = System.getProperties();
    for (String prop : SSL_PROPS.keySet()) {
      if (sysprops.containsKey(prop)) {
        log.info("System property explicitly set, so skipping randomized ssl properties: " + prop);
        manageSslProps = false;
        break;
      }
    }

    assertTrue("test keystore does not exist, can't be used for randomized " +
               "ssl testing: " + TEST_KEYSTORE.getAbsolutePath(), 
               TEST_KEYSTORE.exists() );

    if (manageSslProps) {
      log.info("Randomized ssl ({}) and clientAuth ({})", trySsl, trySslClientAuth);
      for (String prop : SSL_PROPS.keySet()) {
        System.setProperty(prop, SSL_PROPS.get(prop));
      }
      // now explicitly re-set the two random values
      System.setProperty("tests.jettySsl", String.valueOf(trySsl));
      System.setProperty("tests.jettySsl.clientAuth", String.valueOf(trySslClientAuth));
    }
  }

  public static JettySolrRunner jetty;
  public static int port;
  public static SolrServer server = null;
  public static String context;

  public static JettySolrRunner createJetty(String solrHome, String configFile, String schemaFile, String context,
                                            boolean stopAtShutdown, SortedMap<ServletHolder,String> extraServlets) 
      throws Exception { 
    // creates the data dir
    initCore(null, null, solrHome);

    ignoreException("maxWarmingSearchers");

    // this sets the property for jetty starting SolrDispatchFilter
    System.setProperty( "solr.data.dir", dataDir.getCanonicalPath() );

    context = context==null ? "/solr" : context;
    SolrJettyTestBase.context = context;
    jetty = new JettySolrRunner(solrHome, context, 0, configFile, schemaFile, stopAtShutdown, extraServlets);

    jetty.start();
    port = jetty.getLocalPort();
    log.info("Jetty Assigned Port#" + port);
    return jetty;
  }

  public static JettySolrRunner createJetty(String solrHome, String configFile, String context) throws Exception {
    return createJetty(solrHome, configFile, null, context, true, null);
  }


  @AfterClass
  public static void afterSolrJettyTestBase() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
    server = null;
    if (manageSslProps) {
      for (String prop : SSL_PROPS.keySet()) {
        System.clearProperty(prop);
      }
    }
  }


  public SolrServer getSolrServer() {
    {
      if (server == null) {
        server = createNewSolrServer();
      }
      return server;
    }
  }

  /**
   * Create a new solr server.
   * If createJetty was called, an http implementation will be created,
   * otherwise an embedded implementation will be created.
   * Subclasses should override for other options.
   */
  public SolrServer createNewSolrServer() {
    if (jetty != null) {
      try {
        // setup the server...
        String url = jetty.getBaseUrl().toString();
        HttpSolrServer s = new HttpSolrServer( url );
        s.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
        s.setDefaultMaxConnectionsPerHost(100);
        s.setMaxTotalConnections(100);
        return s;
      }
      catch( Exception ex ) {
        throw new RuntimeException( ex );
      }
    } else {
      return new EmbeddedSolrServer( h.getCoreContainer(), "" );
    }
  }
}
