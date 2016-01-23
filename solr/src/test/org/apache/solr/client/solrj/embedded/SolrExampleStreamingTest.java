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

package org.apache.solr.client.solrj.embedded;

import org.apache.solr.client.solrj.SolrExampleTests;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.StreamingUpdateSolrServer;


/**
 * 
 * @version $Id: SolrExampleJettyTest.java 724175 2008-12-07 19:07:11Z ryan $
 * @since solr 1.3
 */
public class SolrExampleStreamingTest extends SolrExampleTests {

  SolrServer server;
  JettySolrRunner jetty;

  int port = 0;
  static final String context = "/example";
  
  @Override public void setUp() throws Exception 
  {
    super.setUp();
    
    jetty = new JettySolrRunner( context, 0 );
    jetty.start();
    port = jetty.getLocalPort();
    System.out.println("Assigned Port#" + port);
    server = this.createNewSolrServer();
  }

  @Override public void tearDown() throws Exception 
  {
    super.tearDown();
    jetty.stop();  // stop the server
  }
  
  
  @Override
  protected SolrServer getSolrServer()
  {
    return server;
  }

  @Override
  protected SolrServer createNewSolrServer()
  {
    try {
      // setup the server...
      String url = "http://localhost:"+port+context;       // smaller queue size hits locks more often
      CommonsHttpSolrServer s = new StreamingUpdateSolrServer( url, 2, 5 ) {
        @Override
        public void handleError(Throwable ex) {
          // do somethign...
        }
      };
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
}
