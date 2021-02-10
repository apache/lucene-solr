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

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Test for LBHttpSolrClient
 *
 * @since solr 1.4
 */
@Slow
public class TestLBHttpSolrClient extends SolrTestCaseJ4 {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  SolrInstance[] solr = new SolrInstance[3];

  // TODO: fix this test to not require FSDirectory-UUIDUpdateProcessorFallbackTest
  static String savedFactory;

  @Before
  public void beforeTest() throws Exception {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    //System.setProperty("solr.directoryFactory", "org.apache.solr.core.MockFSDirectoryFactory");
    useFactory(null);
    System.setProperty("tests.shardhandler.randomSeed", Long.toString(random().nextLong()));

    initCore();

    for (int i = 0; i < solr.length; i++) {
      solr[i] = new SolrInstance("solr/collection1" + i, SolrTestUtil.createTempDir("instance-" + i).toFile(), 0);
      solr[i].setUp();
      solr[i].startJetty();
      addDocs(solr[i]);
    }
  }

  @After
  public void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
    System.clearProperty("tests.shardhandler.randomSeed");
    deleteCore();
  }

  private void addDocs(SolrInstance solrInstance) throws IOException, SolrServerException {
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", i);
      doc.addField("name", solrInstance.name);
      docs.add(doc);
    }
    SolrResponseBase resp;
    try (Http2SolrClient client = getHttpSolrClient(solrInstance.getUrl())) {
      resp = client.add(docs);
      assertEquals(0, resp.getStatus());
      resp = client.commit();
      assertEquals(0, resp.getStatus());
    }
  }

  @Override
  public void tearDown() throws Exception {
    for (SolrInstance aSolr : solr) {
      if (aSolr != null)  {
        aSolr.tearDown();
      }
    }
    super.tearDown();
  }

  @LuceneTestCase.Nightly
  public void testSimple() throws Exception {
    String[] s = new String[solr.length];
    for (int i = 0; i < solr.length; i++) {
      s[i] = solr[i].getUrl();
    }
    try (LBHttp2SolrClient client = getLBHttpSolrClient(s)) {
      client.setAliveCheckInterval(50);
      SolrQuery solrQuery = new SolrQuery("*:*");
      Set<String> names = new HashSet<>();
      QueryResponse resp = null;
      for (String value : s) {
        resp = client.query(solrQuery);
        assertEquals(10, resp.getResults().getNumFound());
        names.add(resp.getResults().get(0).getFieldValue("name").toString());
      }
      assertEquals(3, names.size());

      // Kill a server and test again
      solr[1].jetty.stop();
      solr[1].jetty = null;
      getNamesSize(s, client, solrQuery, names);
      assertEquals(2, names.size());
      assertFalse(names.contains("solr1"));

      // Start the killed server once again
      solr[1].startJetty();
      // Wait for the alive check to complete

      TimeOut timeout = new TimeOut(2, TimeUnit.SECONDS, TimeSource.NANO_TIME);
      while (!timeout.hasTimedOut()) {
        getNamesSize(s, client, solrQuery, names);
        if (names.size() == 3) {
         break;
        }
        Thread.sleep(100);
      }
      getNamesSize(s, client, solrQuery, names);
      assertEquals(3, names.size());
    }
  }

  private void getNamesSize(String[] s, LBHttp2SolrClient client, SolrQuery solrQuery, Set<String> names) throws SolrServerException, IOException {
    QueryResponse resp;
    names.clear();
    for (String value : s) {
      resp = client.query(solrQuery);
      assertEquals(10, resp.getResults().getNumFound());
      names.add(resp.getResults().get(0).getFieldValue("name").toString());
    }
  }

  @Test
  @LuceneTestCase.Nightly // very slow ~ not sure if this is by design ...
  public void testTwoServers() throws Exception {
    try (LBHttp2SolrClient client = getLBHttpSolrClient(solr[0].getUrl(), solr[1].getUrl())) {
      client.setAliveCheckInterval(50);
      SolrQuery solrQuery = new SolrQuery("*:*");
      QueryResponse resp = null;
      solr[0].jetty.stop();
      solr[0].jetty = null;
      resp = client.query(solrQuery);
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      Assert.assertEquals("solr/collection11", name);
      resp = client.query(solrQuery);
      name = resp.getResults().get(0).getFieldValue("name").toString();
      Assert.assertEquals("solr/collection11", name);
      solr[1].jetty.stop();
      solr[1].jetty = null;
      solr[0].startJetty();

      resp = client.query(solrQuery);

      name = resp.getResults().get(0).getFieldValue("name").toString();
      Assert.assertEquals("solr/collection10", name);
    }
  }

  @LuceneTestCase.Nightly
  public void testReliability() throws Exception {
    String[] s = new String[solr.length];
    for (int i = 0; i < solr.length; i++) {
      s[i] = solr[i].getUrl();
    }

    CloseableHttpClient myHttpClient = HttpClientUtil.createClient(null);
    try {
      try (LBHttpSolrClient client = getLBHttpSolrClient(myHttpClient, 1000, 1500, s)) {
        client.setAliveCheckInterval(50);

        // Kill a server and test again
        solr[1].jetty.stop();
        solr[1].jetty = null;

        // query the servers
        for (String value : s)
          client.query(new SolrQuery("*:*"));

        // Start the killed server once again
        solr[1].startJetty();
        // Wait for the alive check to complete
        waitForServer(10, client, 3, solr[1].name);
      }
    } finally {
      HttpClientUtil.close(myHttpClient);
    }
  }
  
  // wait maximum ms for serverName to come back up
  private void waitForServer(int maxSeconds, LBHttpSolrClient client, int nServers, String serverName) throws Exception {
    final TimeOut timeout = new TimeOut(maxSeconds, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    while (! timeout.hasTimedOut()) {
      QueryResponse resp;
      try {
        resp = client.query(new SolrQuery("*:*"));
      } catch (Exception e) {
        log.warn("", e);
        continue;
      }
      String name = resp.getResults().get(0).getFieldValue("name").toString();
      if (name.equals(serverName))
        return;
      
      Thread.sleep(50);
    }
  }
  
  private static class SolrInstance {
    String name;
    File homeDir;
    File dataDir;
    File confDir;
    int port;
    JettySolrRunner jetty;

    public SolrInstance(String name, File homeDir, int port) {
      this.name = name;
      this.homeDir = homeDir;
      this.port = port;

      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getUrl() {
      return buildUrl(port, "/solr/collection1");
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
      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      FileUtils.copyFile(SolrTestUtil.getFile(getSolrXmlFile()), new File(homeDir, "solr.xml"));

      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(SolrTestUtil.getFile(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");
      FileUtils.copyFile(SolrTestUtil.getFile(getSchemaFile()), f);
      Files.createFile(homeDir.toPath().resolve("collection1/core.properties"));
    }

    public void tearDown() throws Exception {
      if (jetty != null) jetty.stop();
      IOUtils.rm(homeDir.toPath());
    }

    public void startJetty() throws Exception {

      Properties props = new Properties();
      props.setProperty("solrconfig", "bad_solrconfig.xml");
      props.setProperty("solr.data.dir", getDataDir());

      JettyConfig jettyConfig = JettyConfig.builder(buildJettyConfig("/solr")).setPort(port).build();

      jetty = new JettySolrRunner(getHomeDir(), props, jettyConfig);
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
