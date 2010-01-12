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

import junit.framework.TestCase;
import junit.framework.Assert;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.AbstractSolrTestCase;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Test for LBHttpSolrServer
 *
 * @version $Id$
 * @since solr 1.4
 */
public class TestLBHttpSolrServer extends TestCase {
  SolrInstance[] solr = new SolrInstance[3];
  // HttpClient httpClient = new HttpClient(new MultiThreadedHttpConnectionManager());

  public void setUp() throws Exception {
    for (int i = 0; i < solr.length; i++) {
      solr[i] = new SolrInstance("solr" + i, 0);
      solr[i].setUp();
      solr[i].startJetty();
      addDocs(solr[i]);
    }
  }

  private void addDocs(SolrInstance solrInstance) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", solrInstance.name);
      docs.add(doc);
    }
    CommonsHttpSolrServer solrServer = new CommonsHttpSolrServer(solrInstance.getUrl());
    UpdateResponse resp = solrServer.add(docs);
    assertEquals(0, resp.getStatus());
    resp = solrServer.commit();
    assertEquals(0, resp.getStatus());
  }

  @Override
  public void tearDown() throws Exception {
    for (SolrInstance aSolr : solr) {
      aSolr.tearDown();
    }
  }

  public void testSimple() throws Exception {
    LinkedList<String> serverList = new LinkedList<String>();
    for (int i = 0; i < solr.length; i++) {
      serverList.add(solr[i].getUrl());
    }
    String[] servers = serverList.toArray(new String[serverList.size()]);

    LBHttpSolrServer lb = new LBHttpSolrServer(servers);
    lb.setAliveCheckInterval(500);
    LBHttpSolrServer lb2 = new LBHttpSolrServer();
    lb2.setAliveCheckInterval(500);


    SolrQuery solrQuery = new SolrQuery("*:*");
    SolrRequest solrRequest = new QueryRequest(solrQuery);
    Set<String> names = new HashSet<String>();
    QueryResponse resp = null;
    for (String server : servers) {
      resp = lb.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    assertEquals(3, names.size());

    // Now test through the advanced API
    names.clear();
    for (String server : servers) {
      LBHttpSolrServer.Rsp rsp = lb2.request(new LBHttpSolrServer.Req(solrRequest, serverList));
      // make sure the response came from the first in the list
      assertEquals(rsp.getServer(), serverList.getFirst());
      resp = new QueryResponse(rsp.getResponse(), lb);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());

      // rotate the server list
      serverList.addLast(serverList.removeFirst());
    }
    assertEquals(3, names.size());


    // Kill a server and test again
    solr[1].jetty.stop();
    solr[1].jetty = null;
    names.clear();
    for (String server : servers) {
      resp = lb.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    assertEquals(2, names.size());
    assertFalse(names.contains("solr1"));


    // Now test through the advanced API    
    names.clear();
    for (String server : servers) {
      LBHttpSolrServer.Rsp rsp = lb2.request(new LBHttpSolrServer.Req(solrRequest, serverList));
      resp = new QueryResponse(rsp.getResponse(), lb);
      assertFalse(rsp.getServer().contains("solr1"));      
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());

      // rotate the server list
      serverList.addLast(serverList.removeFirst());
    }
    assertEquals(2, names.size());
    assertFalse(names.contains("solr1"));

    // Start the killed server once again
    solr[1].startJetty(true);
    // Wait for the alive check to complete
    Thread.sleep(600);
    names.clear();
    for (String value : servers) {
      resp = lb.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    // System.out.println("SERVERNAMES="+names);
    assertEquals(3, names.size());

    // Now test through the advanced API
    names.clear();
    for (String server : servers) {
      LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(solrRequest, serverList);
      LBHttpSolrServer.Rsp rsp = lb2.request(req);
      // make sure the response came from the first in the list
      assertEquals(rsp.getServer(), serverList.getFirst());
      resp = new QueryResponse(rsp.getResponse(), lb);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());

      // rotate the server list
      serverList.addLast(serverList.removeFirst());
    }
    assertEquals(3, names.size());

    
    // slow LB for Simple API
    LBHttpSolrServer slowLB = new LBHttpSolrServer(servers);
    slowLB.setAliveCheckInterval(1000000000);

    // slow LB for advanced API
    LBHttpSolrServer slowLB2 = new LBHttpSolrServer();
    slowLB2.setAliveCheckInterval(1000000000);

    // stop all solr servers
    for (SolrInstance solrInstance : solr) {
      solrInstance.jetty.stop();
      solrInstance.jetty = null;
    }

    try {
      resp = slowLB.query(solrQuery);
      TestCase.fail(); // all servers should be down
    } catch (SolrServerException e) {
      // expected      
    }

    try {
      LBHttpSolrServer.Rsp rsp = slowLB2.request(new LBHttpSolrServer.Req(solrRequest, serverList));
      TestCase.fail(); // all servers should be down
    } catch (SolrServerException e) {
      // expected
    }

    // Start the killed server once again
    solr[1].startJetty(true);

    // even though one of the servers is now up, the loadbalancer won't yet know this unless we ask
    // it to try dead servers.
    try {
      LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(solrRequest, serverList);
      req.setNumDeadServersToTry(0);
      LBHttpSolrServer.Rsp rsp = slowLB2.request(req);
      TestCase.fail(); // all servers still should be marked as down
    } catch (SolrServerException e) {
      // expected
    }

    // the default is to try dead servers if there are no live servers
    {
      resp = slowLB.query(solrQuery);
    }

    // the default is to try dead servers if there are no live servers
    {
      LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(solrRequest, serverList);
      LBHttpSolrServer.Rsp rsp = slowLB2.request(req);
    }

    // the last success should have removed the server from the dead server list, so
    // the next request should succeed even if it doesn't try dead servers.
    {
      LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(solrRequest, serverList);
      req.setNumDeadServersToTry(0);
      LBHttpSolrServer.Rsp rsp = slowLB2.request(req);
    }

  }

  // this test is a subset of testSimple and is no longer needed
  public void XtestTwoServers() throws Exception {
    LBHttpSolrServer lbHttpSolrServer = new LBHttpSolrServer(solr[0].getUrl(), solr[1].getUrl());
    lbHttpSolrServer.setAliveCheckInterval(500);
    SolrQuery solrQuery = new SolrQuery("*:*");
    Set<String> names = new HashSet<String>();
    QueryResponse resp = null;
    solr[0].jetty.stop();
    solr[0].jetty = null;
    resp = lbHttpSolrServer.query(solrQuery);
    String name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr1", name);
    resp = lbHttpSolrServer.query(solrQuery);
    name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr1", name);
    solr[1].jetty.stop();
    solr[1].jetty = null;
    solr[0].startJetty(true);
    Thread.sleep(600);
    resp = lbHttpSolrServer.query(solrQuery);
    name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr0", name);
  }

  private class SolrInstance {
    String name;
    File homeDir;
    File dataDir;
    File confDir;
    int port;
    JettySolrRunner jetty;

    public SolrInstance(String name, int port) {
      this.name = name;
      this.port = port;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getUrl() {
      return "http://localhost:" + port + "/solr";
    }

    public String getSchemaFile() {
      return "." + File.separator + "solr" + File.separator + "conf" + File.separator + "schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      String fname = "";
      fname = "." + File.separator + "solr" + File.separator + "conf" + File.separator + "solrconfig-slave1.xml";
      return fname;
    }

    public void setUp() throws Exception {
      String home = System.getProperty("java.io.tmpdir")
              + File.separator
              + getClass().getName() + "-" + System.currentTimeMillis();


      homeDir = new File(home, name);
      dataDir = new File(homeDir, "data");
      confDir = new File(homeDir, "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(new File(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");
      FileUtils.copyFile(new File(getSchemaFile()), f);

    }

    public void tearDown() throws Exception {
      try {
        jetty.stop();
      } catch (Exception e) {
      }
      AbstractSolrTestCase.recurseDelete(homeDir);
    }

    public void startJetty() throws Exception {
      startJetty(false);
    }


    public void startJetty(boolean waitUntilUp) throws Exception {
      jetty = new JettySolrRunner("/solr", port);
      System.setProperty("solr.solr.home", getHomeDir());
      System.setProperty("solr.data.dir", getDataDir());
      jetty.start(waitUntilUp);
      int newPort = jetty.getLocalPort();
      if (port != 0 && newPort != port) {
        TestCase.fail("TESTING FAILURE: could not grab requested port.");
      }
      this.port = newPort;
//      System.out.println("waiting.........");
//      Thread.sleep(5000);
    }
  }
}
