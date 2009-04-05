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
package org.apache.solr.client.solrj;

import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.client.solrj.beans.Field;
import org.apache.solr.common.SolrInputDocument;

import java.util.Iterator;
import java.io.IOException;

/**
 * Test for SOLR-1038
 *
 * @since solr 1.4
 * @version $Id$
 */
public class TestBatchUpdate extends SolrExampleTestBase {
  static final int numdocs = 1000;

  SolrServer server;
  JettySolrRunner jetty;

  int port = 0;
  static final String context = "/example";

  public void testWithXml() throws Exception {
    CommonsHttpSolrServer commonsHttpSolrServer = (CommonsHttpSolrServer) getSolrServer();
    commonsHttpSolrServer.setRequestWriter(new RequestWriter());
    commonsHttpSolrServer.deleteByQuery( "*:*" ); // delete everything!    
    doIt(commonsHttpSolrServer);
  }

  public void testWithBinary()throws Exception{
    CommonsHttpSolrServer commonsHttpSolrServer = (CommonsHttpSolrServer) getSolrServer();
    commonsHttpSolrServer.setRequestWriter(new BinaryRequestWriter());
    commonsHttpSolrServer.deleteByQuery( "*:*" ); // delete everything!
    doIt(commonsHttpSolrServer);
  }

  public void testWithBinaryBean()throws Exception{
    CommonsHttpSolrServer commonsHttpSolrServer = (CommonsHttpSolrServer) getSolrServer();
    commonsHttpSolrServer.setRequestWriter(new BinaryRequestWriter());
    commonsHttpSolrServer.deleteByQuery( "*:*" ); // delete everything!
    final int[] counter = new int[1];
    counter[0] = 0;
    commonsHttpSolrServer.addBeans(new Iterator<Bean>() {

      public boolean hasNext() {
        return counter[0] < numdocs;
      }

      public Bean next() {
        Bean bean = new Bean();
        bean.id = "" + (++counter[0]);
        bean.cat = "foocat";
        return bean;
      }

      public void remove() {
        //do nothing
      }
    });
    commonsHttpSolrServer.commit();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = commonsHttpSolrServer.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(numdocs, response.getResults().getNumFound());
  }

  public static class Bean{
    @Field
    String id;
    @Field
    String cat;
  }
       
  private void doIt(CommonsHttpSolrServer commonsHttpSolrServer) throws SolrServerException, IOException {
    final int[] counter = new int[1];
    counter[0] = 0;
    commonsHttpSolrServer.add(new Iterator<SolrInputDocument>() {

      public boolean hasNext() {
        return counter[0] < numdocs;
      }

      public SolrInputDocument next() {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "" + (++counter[0]));
        doc.addField("cat", "foocat");
        return doc;
      }

      public void remove() {
        //do nothing

      }
    });
    commonsHttpSolrServer.commit();
    SolrQuery query = new SolrQuery("*:*");
    QueryResponse response = commonsHttpSolrServer.query(query);
    assertEquals(0, response.getStatus());
    assertEquals(numdocs, response.getResults().getNumFound());
  }

  @Override public void setUp() throws Exception
  {
    super.setUp();

    jetty = new JettySolrRunner( context, 0 );
    jetty.start();
    port = jetty.getLocalPort();

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
  }
}
