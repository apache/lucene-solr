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

package org.apache.solr.client.solrj;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.lucene.util.QuickPatchThreadsFilter;
import org.apache.solr.SolrIgnoredThreadsFilter;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

/**
 * Test for LBHttpSolrServer
 *
 * @since solr 1.4
 */
@Slow
@ThreadLeakFilters(defaultFilters = true, filters = {
    SolrIgnoredThreadsFilter.class,
    QuickPatchThreadsFilter.class
})
public class TestLBHttpSolrServer extends LuceneTestCase {
  private static final Logger log = LoggerFactory
      .getLogger(TestLBHttpSolrServer.class);
  SolrInstance[] solr = new SolrInstance[3];
  HttpClient httpClient;

  // TODO: fix this test to not require FSDirectory
  static String savedFactory;

  @BeforeClass
  public static void beforeClass() {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));
  }

  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
    System.clearProperty("tests.shardhandler.randomSeed");
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    httpClient = HttpClientUtil.createClient(null);
    HttpClientUtil.setConnectionTimeout(httpClient,  1000);
    for (int i = 0; i < solr.length; i++) {
      solr[i] = new SolrInstance("solr/collection1" + i, 0);
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
    HttpSolrServer solrServer = new HttpSolrServer(solrInstance.getUrl(), httpClient);
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
    httpClient.getConnectionManager().shutdown();
    super.tearDown();
  }

  public void testSimple() throws Exception {
    String[] s = new String[solr.length];
    for (int i = 0; i < solr.length; i++) {
      s[i] = solr[i].getUrl();
    }
    LBHttpSolrServer lbHttpSolrServer = new LBHttpSolrServer(httpClient, s);
    lbHttpSolrServer.setAliveCheckInterval(500);
    SolrQuery solrQuery = new SolrQuery("*:*");
    Set<String> names = new HashSet<String>();
    QueryResponse resp = null;
    for (String value : s) {
      resp = lbHttpSolrServer.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    assertEquals(3, names.size());

    // Kill a server and test again
    solr[1].jetty.stop();
    solr[1].jetty = null;
    names.clear();
    for (String value : s) {
      resp = lbHttpSolrServer.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    assertEquals(2, names.size());
    assertFalse(names.contains("solr1"));

    // Start the killed server once again
    solr[1].startJetty();
    // Wait for the alive check to complete
    Thread.sleep(1200);
    names.clear();
    for (String value : s) {
      resp = lbHttpSolrServer.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
    assertEquals(3, names.size());
  }

  public void testTwoServers() throws Exception {
    LBHttpSolrServer lbHttpSolrServer = new LBHttpSolrServer(httpClient, solr[0].getUrl(), solr[1].getUrl());
    lbHttpSolrServer.setAliveCheckInterval(500);
    SolrQuery solrQuery = new SolrQuery("*:*");
    QueryResponse resp = null;
    solr[0].jetty.stop();
    solr[0].jetty = null;
    resp = lbHttpSolrServer.query(solrQuery);
    String name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr/collection11", name);
    resp = lbHttpSolrServer.query(solrQuery);
    name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr/collection11", name);
    solr[1].jetty.stop();
    solr[1].jetty = null;
    solr[0].startJetty();
    Thread.sleep(1200);
    try {
      resp = lbHttpSolrServer.query(solrQuery);
    } catch(SolrServerException e) {
      // try again after a pause in case the error is lack of time to start server
      Thread.sleep(3000);
      resp = lbHttpSolrServer.query(solrQuery);
    }
    name = resp.getResults().get(0).getFieldValue("name").toString();
    Assert.assertEquals("solr/collection10", name);
  }

  public void testReliability() throws Exception {
    String[] s = new String[solr.length];
    for (int i = 0; i < solr.length; i++) {
      s[i] = solr[i].getUrl();
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_CONNECTION_TIMEOUT, 250);
    params.set(HttpClientUtil.PROP_SO_TIMEOUT, 250);
    HttpClient myHttpClient = HttpClientUtil.createClient(params);

    LBHttpSolrServer lbHttpSolrServer = new LBHttpSolrServer(myHttpClient, s);
    lbHttpSolrServer.setAliveCheckInterval(500);

    // Kill a server and test again
    solr[1].jetty.stop();
    solr[1].jetty = null;

    // query the servers
    for (String value : s)
      lbHttpSolrServer.query(new SolrQuery("*:*"));

    // Start the killed server once again
    solr[1].startJetty();
    // Wait for the alive check to complete
    waitForServer(30000, lbHttpSolrServer, 3, "solr1");
  }
  
  // wait maximum ms for serverName to come back up
  private void waitForServer(int maximum, LBHttpSolrServer server, int nServers, String serverName) throws Exception {
    long endTime = System.currentTimeMillis() + maximum;
    while (System.currentTimeMillis() < endTime) {
      QueryResponse resp;
      try {
        resp = server.query(new SolrQuery("*:*"));
      } catch (Exception e) {
        log.warn("", e);
        continue;
      }
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      if (name.equals(serverName))
        return;
    }
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
      return "http://127.0.0.1:" + port + "/solr";
    }

    public String getSchemaFile() {
      return "solrj/solr/collection1/conf/schema-replication1.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return "solrj/solr/collection1/conf/solrconfig-slave1.xml";
    }

    public String getSolrXmlFile() {
      return "solrj/solr/solr.xml";
    }


    public void setUp() throws Exception {
      File home = new File(LuceneTestCase.TEMP_DIR,
              getClass().getName() + "-" + System.currentTimeMillis());


      homeDir = new File(home, name);
      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSolrXmlFile()), new File(homeDir, "solr.xml"));

      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");
      FileUtils.copyFile(SolrTestCaseJ4.getFile(getSchemaFile()), f);

    }

    public void tearDown() throws Exception {
      try {
        jetty.stop();
      } catch (Exception e) {
      }
      AbstractSolrTestCase.recurseDelete(homeDir);
    }

    public void startJetty() throws Exception {
      jetty = new JettySolrRunner(getHomeDir(), "/solr", port, "bad_solrconfig.xml", null);
      System.setProperty("solr.data.dir", getDataDir());
      jetty.start();
      int newPort = jetty.getLocalPort();
      if (port != 0 && newPort != port) {
        fail("TESTING FAILURE: could not grab requested port.");
      }
      this.port = newPort;
//      System.out.println("waiting.........");
//      Thread.sleep(5000);
    }
  }
}
