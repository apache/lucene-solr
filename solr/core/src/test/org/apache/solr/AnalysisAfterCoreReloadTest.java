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
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;

public class AnalysisAfterCoreReloadTest extends AbstractSolrTestCase {
  private File homeDir;
  int port = 0;
  static final String context = "/solr";
  JettySolrRunner jetty;
  static final String collection = "collection1";
  
  @After
  public void cleanUp() throws Exception {
    jetty.stop();
    if (homeDir != null && homeDir.isDirectory() && homeDir.exists())
      recurseDelete(homeDir);
  }
  
  @Override
  public String getSolrHome() { 
    return homeDir.getAbsolutePath(); 
  }

  @Override
  public void setUp() throws Exception {
    homeDir = new File(TEMP_DIR + File.separator + "solr-test-home-" + System.nanoTime());
    homeDir.mkdirs();
    FileUtils.copyDirectory(new File(getFile("solr/" + collection).getParent()), homeDir, false);

    super.setUp();
    
    jetty = new JettySolrRunner(getSolrHome(), context, 0 );
    jetty.start(false);
    port = jetty.getLocalPort();
  }
  
  public void testStopwordsAfterCoreReload() throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField( "id", "42" );
    doc.setField( "teststop", "terma stopworda stopwordb stopwordc" );
    
    // default stopwords - stopworda and stopwordb
    
    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.add( doc );
    up.process( getSolrCore() );

    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest( q );
    q.setQuery( "teststop:terma" );
    assertEquals( 1, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopworda" );
    assertEquals( 0, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopwordb" );
    assertEquals( 0, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopwordc" );
    assertEquals( 1, r.process( getSolrCore() ).getResults().size() );

    // overwrite stopwords file with stopword list ["stopwordc"] and reload the core
    overwriteStopwords("stopwordc\n");
    SolrServer coreadmin = getSolrAdmin();
    CoreAdminRequest.reloadCore(collection, coreadmin);

    up.process( getSolrCore() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:terma" );
    assertEquals( 1, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopworda" );
    // stopworda is no longer a stopword
    assertEquals( 1, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopwordb" );
    // stopwordb is no longer a stopword
    assertEquals( 1, r.process( getSolrCore() ).getResults().size() );

    q = new SolrQuery();
    r = new QueryRequest( q );
    q.setQuery( "teststop:stopwordc" );
    // stopwordc should be a stopword
    assertEquals( 0, r.process( getSolrCore() ).getResults().size() );
  }
  
  private void overwriteStopwords(String stopwords) throws IOException {
    SolrCore core = h.getCoreContainer().getCore(collection);
    try {
      String configDir = core.getResourceLoader().getConfigDir();
      File file = new File(configDir, "stopwords.txt");
      FileUtils.writeStringToFile(file, stopwords);
    } finally {
      core.close();
    }
  }
  
  protected SolrServer getSolrAdmin() {
    return createServer("");
  }
  protected SolrServer getSolrCore() {
    return createServer(collection);
  }
  private SolrServer createServer( String name ) {
    try {
      // setup the server...
      String url = "http://127.0.0.1:"+port+context+"/"+name;
      HttpSolrServer s = new HttpSolrServer( url );
      s.setConnectionTimeout(SolrTestCaseJ4.DEFAULT_CONNECTION_TIMEOUT);
      s.setDefaultMaxConnectionsPerHost(100);
      s.setMaxTotalConnections(100);
      return s;
    }
    catch( Exception ex ) {
      throw new RuntimeException( ex );
    }
  }

  @Override
  public String getSchemaFile() {
    return "schema.xml";
  }

  @Override
  public String getSolrConfigFile() {
    return "solrconfig.xml";
  }

}