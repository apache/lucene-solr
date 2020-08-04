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
package org.apache.solr.handler.dataimport;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Properties;

/**
 * Test for ContentStreamDataSource
 *
 *
 * @since solr 1.4
 */
public class TestContentStreamDataSource extends AbstractDataImportHandlerTestCase {
  private static final String CONF_DIR = "dih/solr/collection1/conf/";
  private static final String ROOT_DIR = "dih/solr/";
  SolrInstance instance = null;
  JettySolrRunner jetty;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    instance = new SolrInstance("inst", null);
    instance.setUp();
    jetty = createAndStartJetty(instance);
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    if (null != jetty) {
      jetty.stop();
      jetty = null;
    }
    super.tearDown();
  }

  @Test
  public void testSimple() throws Exception {
    DirectXmlRequest req = new DirectXmlRequest("/dataimport", xml);
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("command", "full-import");
    params.set("clean", "false");
    req.setParams(params);
    try (HttpSolrClient solrClient = getHttpSolrClient(buildUrl(jetty.getLocalPort(), "/solr/collection1"))) {
      solrClient.request(req);
      ModifiableSolrParams qparams = new ModifiableSolrParams();
      qparams.add("q", "*:*");
      QueryResponse qres = solrClient.query(qparams);
      SolrDocumentList results = qres.getResults();
      assertEquals(2, results.getNumFound());
      SolrDocument doc = results.get(0);
      assertEquals("1", doc.getFieldValue("id"));
      assertEquals("Hello C1", ((List) doc.getFieldValue("desc")).get(0));
    }
  }

  @Test
  public void testCommitWithin() throws Exception {
    DirectXmlRequest req = new DirectXmlRequest("/dataimport", xml);
    ModifiableSolrParams params = params("command", "full-import", 
        "clean", "false", UpdateParams.COMMIT, "false", 
        UpdateParams.COMMIT_WITHIN, "1000");
    req.setParams(params);
    try (HttpSolrClient solrServer = getHttpSolrClient(buildUrl(jetty.getLocalPort(), "/solr/collection1"))) {
      solrServer.request(req);
      Thread.sleep(100);
      ModifiableSolrParams queryAll = params("q", "*", "df", "desc");
      QueryResponse qres = solrServer.query(queryAll);
      SolrDocumentList results = qres.getResults();
      assertEquals(0, results.getNumFound());
      Thread.sleep(1000);
      for (int i = 0; i < 10; i++) {
        qres = solrServer.query(queryAll);
        results = qres.getResults();
        if (2 == results.getNumFound()) {
          return;
        }
        Thread.sleep(500);
      }
    }
    fail("Commit should have occurred but it did not");
  }
  
  private static class SolrInstance {
    String name;
    Integer port;
    File homeDir;
    File confDir;
    File dataDir;
    
    /**
     * if leaderPort is null, this instance is a leader -- otherwise this instance is a follower, and assumes the leader is
     * on localhost at the specified port.
     */
    public SolrInstance(String name, Integer port) {
      this.name = name;
      this.port = port;
    }

    public String getHomeDir() {
      return homeDir.toString();
    }

    public String getSchemaFile() {
      return CONF_DIR + "dataimport-schema.xml";
    }

    public String getConfDir() {
      return confDir.toString();
    }

    public String getDataDir() {
      return dataDir.toString();
    }

    public String getSolrConfigFile() {
      return CONF_DIR + "contentstream-solrconfig.xml";
    }

    public String getSolrXmlFile() {
      return ROOT_DIR + "solr.xml";
    }


    public void setUp() throws Exception {
      homeDir = createTempDir("inst").toFile();
      dataDir = new File(homeDir + "/collection1", "data");
      confDir = new File(homeDir + "/collection1", "conf");

      homeDir.mkdirs();
      dataDir.mkdirs();
      confDir.mkdirs();

      FileUtils.copyFile(getFile(getSolrXmlFile()), new File(homeDir, "solr.xml"));
      File f = new File(confDir, "solrconfig.xml");
      FileUtils.copyFile(getFile(getSolrConfigFile()), f);
      f = new File(confDir, "schema.xml");

      FileUtils.copyFile(getFile(getSchemaFile()), f);
      f = new File(confDir, "data-config.xml");
      FileUtils.copyFile(getFile(CONF_DIR + "dataconfig-contentstream.xml"), f);

      Files.createFile(homeDir.toPath().resolve("collection1/core.properties"));
    }

  }

  private JettySolrRunner createAndStartJetty(SolrInstance instance) throws Exception {
    Properties nodeProperties = new Properties();
    nodeProperties.setProperty("solr.data.dir", instance.getDataDir());
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), nodeProperties, buildJettyConfig("/solr"));
    jetty.start();
    return jetty;
  }

  static String xml = "<root>\n"
          + "<b>\n"
          + "  <id>1</id>\n"
          + "  <c>Hello C1</c>\n"
          + "</b>\n"
          + "<b>\n"
          + "  <id>2</id>\n"
          + "  <c>Hello C2</c>\n"
          + "</b>\n" + "</root>";
}
