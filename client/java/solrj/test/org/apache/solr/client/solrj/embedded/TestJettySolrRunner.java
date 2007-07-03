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

import org.apache.solr.client.solrj.SolrExampleTestBase;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 * @author ryan
 * @version $Id$
 * @since solr 1.3
 */
public class TestJettySolrRunner extends SolrExampleTestBase {

  SolrServer server;
  JettySolrRunner jetty;

  @Override public void setUp() throws Exception 
  {
    super.setUp();
    
    int port = 8984; // not 8983
    String context = "/example";
    
    jetty = new JettySolrRunner( context, port );
    jetty.start();
    
    // setup the server...
    String url = "http://localhost:"+port+context;
    server = new CommonsHttpSolrServer( url );
      ((CommonsHttpSolrServer)server).setConnectionTimeout(5);
      ((CommonsHttpSolrServer)server).setDefaultMaxConnectionsPerHost(100);
      ((CommonsHttpSolrServer)server).setMaxTotalConnections(100);
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
}
