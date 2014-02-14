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

import java.io.File;
import java.util.SortedMap;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


abstract public class SolrJettyTestBase extends SolrTestCaseJ4 
{
  private static Logger log = LoggerFactory.getLogger(SolrJettyTestBase.class);


  @BeforeClass
  public static void beforeSolrJettyTestBase() throws Exception {

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
    jetty = new JettySolrRunner(solrHome, context, 0, configFile, schemaFile, stopAtShutdown, extraServlets, sslConfig);

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
        String url = jetty.getBaseUrl().toString() + "/" + "collection1";
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

  // Sets up the necessary config files for Jetty. At least some tests require that the solrconfig from the test
  // file directory are used, but some also require that the solr.xml file be explicitly there as of SOLR-4817
  public static void setupJettyTestHome(File solrHome, String collection) throws Exception {
    if (solrHome.exists()) {
      FileUtils.deleteDirectory(solrHome);
    }
    copySolrHomeToTemp(solrHome, collection);
  }

  public static void cleanUpJettyHome(File solrHome) throws Exception {
    if (solrHome.exists()) {
      FileUtils.deleteDirectory(solrHome);
    }
  }


}
