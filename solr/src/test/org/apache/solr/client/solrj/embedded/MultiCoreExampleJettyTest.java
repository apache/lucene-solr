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

import org.apache.solr.client.solrj.MultiCoreExampleTestBase;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest.ACTION;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.search.SolrIndexSearcher.QueryResult;
import org.junit.Test;

/**
 * TODO? perhaps use:
 *  http://docs.codehaus.org/display/JETTY/ServletTester
 * rather then open a real connection?
 * 
 * @version $Id$
 * @since solr 1.3
 */
public class MultiCoreExampleJettyTest extends MultiCoreExampleTestBase {

  JettySolrRunner jetty;

  int port = 0;
  static final String context = "/example";
  
  @Override public void setUp() throws Exception 
  {    
    // TODO: fix this test to use MockDirectoryFactory
    System.clearProperty("solr.directoryFactory");
    super.setUp();

    jetty = new JettySolrRunner( context, 0 );
    jetty.start(false);
    port = jetty.getLocalPort();

    h.getCoreContainer().setPersistent(false);    
  }

  @Override public void tearDown() throws Exception 
  {
    super.tearDown();
    jetty.stop();  // stop the server
  }
  

  @Override
  protected SolrServer getSolrCore(String name)
  {
    return createServer(name);
  }

  @Override
  protected SolrServer getSolrCore0()
  {
    return createServer( "core0" );
  }

  @Override
  protected SolrServer getSolrCore1()
  {
    return createServer( "core1" );
  }

  @Override
  protected SolrServer getSolrAdmin()
  {
    return createServer( "" );
  } 
  
  private SolrServer createServer( String name )
  {
    try {
      // setup the server...
      String url = "http://localhost:"+port+context+"/"+name;
      CommonsHttpSolrServer s = new CommonsHttpSolrServer( url );
      s.setConnectionTimeout(100); // 1/10th sec
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }
  
  

  @Test
  public void testDistributed() throws Exception
  {
    UpdateRequest up = new UpdateRequest();
    up.setAction( ACTION.COMMIT, true, true );
    up.deleteByQuery( "*:*" );
    up.process( getSolrCore0() );
    up.process( getSolrCore1() );
    up.clear();
    
    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    // Add to core0
    doc.setField( "id", "core0" );
    up.add( doc );
    up.process( getSolrCore0() );
    up.clear();

    // Add to core1
    doc.setField( "id", "core1" );
    up.add( doc );
    up.process( getSolrCore1() );
    up.clear();
    
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest( q );
    q.setQuery( "*:*" );
    assertEquals( 1, r.process( getSolrCore0() ).getResults().size() );
    assertEquals( 1, r.process( getSolrCore1() ).getResults().size() );
    
    // Distributed
    String baseURL = "localhost:"+port+context+"/";
    q = new SolrQuery( "*:*" );
    q.set( ShardParams.SHARDS, baseURL+"core0,"+baseURL+"core1" );
    q.set( "fl", "id,s:[shard]" );
    r = new QueryRequest( q );
    SolrDocumentList docs = r.process( getSolrCore0() ).getResults();
    assertEquals( 2, docs.size() );
    for( SolrDocument d : docs ) {
      String id = (String)d.get("id");
      String shard = (String)d.get("s");
      assertEquals(baseURL+id, shard); // The shard ends with the core name
    }
  }
}
