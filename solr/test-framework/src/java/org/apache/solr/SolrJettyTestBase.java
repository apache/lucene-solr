package org.apache.solr;

/**
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
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.util.ExternalPaths;
import org.junit.AfterClass;

abstract public class SolrJettyTestBase extends SolrTestCaseJ4 
{
  // Try not introduce a dependency on the example schema or config unless you need to.
  // using configs in the test directory allows more flexibility to change "example"
  // without breaking configs.

  public String getSolrHome() { return ExternalPaths.EXAMPLE_HOME; }

  public static JettySolrRunner jetty;
  public static int port;
  public static SolrServer server;
  public static String context;

  public static JettySolrRunner createJetty(String solrHome, String configFile, String context) throws Exception {
    // creates the data dir
    initCore(null, null);

    ignoreException("maxWarmingSearchers");

    // this sets the property for jetty starting SolrDispatchFilter
    System.setProperty( "solr.solr.home", solrHome);
    System.setProperty( "solr.data.dir", dataDir.getCanonicalPath() );

    context = context==null ? "/solr" : context;
    SolrJettyTestBase.context = context;
    jetty = new JettySolrRunner( context, 0, configFile );

    jetty.start();
    port = jetty.getLocalPort();
    log.info("Jetty Assigned Port#" + port);
    return jetty;
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
        String url = "http://localhost:"+port+context;
        CommonsHttpSolrServer s = new CommonsHttpSolrServer( url );
        s.setConnectionTimeout(100); // 1/10th sec
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
