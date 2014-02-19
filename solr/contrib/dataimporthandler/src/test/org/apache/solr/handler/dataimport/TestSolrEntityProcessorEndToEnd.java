package org.apache.solr.handler.dataimport;

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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end test of SolrEntityProcessor. "Real" test using embedded Solr
 */
public class TestSolrEntityProcessorEndToEnd extends AbstractDataImportHandlerTestCase {
  
  private static Logger LOG = LoggerFactory.getLogger(TestSolrEntityProcessorEndToEnd.class);
  
  private static final String SOLR_CONFIG = "dataimport-solrconfig.xml";
  private static final String SOLR_SCHEMA = "dataimport-schema.xml";
  private static final String SOURCE_CONF_DIR = "dih" + File.separator + "solr" + File.separator + "collection1" + File.separator + "conf" + File.separator;
  private static final String ROOT_DIR = "dih" + File.separator + "solr" + File.separator;

  private static final String DEAD_SOLR_SERVER = "http://[ff01::114]:33332/solr";
  
  private static final List<Map<String,Object>> DB_DOCS = new ArrayList<Map<String,Object>>();
  private static final List<Map<String,Object>> SOLR_DOCS = new ArrayList<Map<String,Object>>();
  
  static {
    // dynamic fields in the destination schema
    Map<String,Object> dbDoc = new HashMap<String,Object>();
    dbDoc.put("dbid_s", "1");
    dbDoc.put("dbdesc_s", "DbDescription");
    DB_DOCS.add(dbDoc);

    Map<String,Object> solrDoc = new HashMap<String,Object>();
    solrDoc.put("id", "1");
    solrDoc.put("desc", "SolrDescription");
    SOLR_DOCS.add(solrDoc);
  }

  
  private SolrInstance instance = null;
  private JettySolrRunner jetty;
  
  private String getDihConfigTagsInnerEntity() {
    return  "<dataConfig>\r\n"
        + "  <dataSource type='MockDataSource' />\r\n"
        + "  <document>\r\n"
        + "    <entity name='db' query='select * from x'>\r\n"
        + "      <field column='dbid_s' />\r\n"
        + "      <field column='dbdesc_s' />\r\n"
        + "      <entity name='se' processor='SolrEntityProcessor' query='id:${db.dbid_s}'\n"
        + "     url='" + getSourceUrl() + "' fields='id,desc'>\r\n"
        + "        <field column='id' />\r\n"
        + "        <field column='desc' />\r\n" + "      </entity>\r\n"
        + "    </entity>\r\n" + "  </document>\r\n" + "</dataConfig>\r\n";
  }
  
  private String generateDIHConfig(String options, boolean useDeadServer) {
    return "<dataConfig>\r\n" + "  <document>\r\n"
        + "    <entity name='se' processor='SolrEntityProcessor'" + "   url='"
        + (useDeadServer ? DEAD_SOLR_SERVER : getSourceUrl()) + "' " + options + " />\r\n" + "  </document>\r\n"
        + "</dataConfig>\r\n";
  }
  
  private String getSourceUrl() {
    return buildUrl(jetty.getLocalPort(), "/solr");
  }
  
  //TODO: fix this test to close its directories
  static String savedFactory;
  @BeforeClass
  public static void beforeClass() {
    savedFactory = System.getProperty("solr.DirectoryFactory");
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
  }
  
  @AfterClass
  public static void afterClass() {
    if (savedFactory == null) {
      System.clearProperty("solr.directoryFactory");
    } else {
      System.setProperty("solr.directoryFactory", savedFactory);
    }
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    // destination solr core
    initCore(SOLR_CONFIG, SOLR_SCHEMA);
    // data source solr instance
    instance = new SolrInstance();
    instance.setUp();
    jetty = createJetty(instance);
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    try {
      deleteCore();
    } catch (Exception e) {
      LOG.error("Error deleting core", e);
    }
    jetty.stop();
    instance.tearDown();
    super.tearDown();
  }
  
  public void testFullImport() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      addDocumentsToSolr(SOLR_DOCS);
      runFullImport(generateDIHConfig("query='*:*' rows='2' fl='id,desc' onError='skip'", false));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
    
    assertQ(req("*:*"), "//result[@numFound='1']");
    assertQ(req("id:1"), "//result/doc/str[@name='id'][.='1']",
        "//result/doc/arr[@name='desc'][.='SolrDescription']");
  }
  
  public void testFullImportFqParam() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      addDocumentsToSolr(generateSolrDocuments(30));
      Map<String,String> map = new HashMap<String,String>();
      map.put("rows", "50");
      runFullImport(generateDIHConfig("query='*:*' fq='desc:Description1*,desc:Description*2' rows='2'", false), map);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
    
    assertQ(req("*:*"), "//result[@numFound='1']");
    assertQ(req("id:12"), "//result[@numFound='1']", "//result/doc/arr[@name='desc'][.='Description12']");
  }
  
  public void testFullImportFieldsParam() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      addDocumentsToSolr(generateSolrDocuments(7));
      runFullImport(generateDIHConfig("query='*:*' fl='id' rows='2'", false));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
    
    assertQ(req("*:*"), "//result[@numFound='7']");
    assertQ(req("id:1"), "//result[@numFound='1']");
    try {
      assertQ(req("id:1"), "//result/doc/arr[@name='desc']");
      fail("The document has a field with name desc");
    } catch(Exception e) {
      
    }
    
  }
  
  /**
   * Receive a row from SQL (Mock) and fetch a row from Solr
   */
  public void testFullImportInnerEntity() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      List<Map<String,Object>> DOCS = new ArrayList<Map<String,Object>>(DB_DOCS);
      Map<String, Object> doc = new HashMap<String, Object>();
      doc.put("dbid_s", "2");
      doc.put("dbdesc_s", "DbDescription2");
      DOCS.add(doc);
      MockDataSource.setIterator("select * from x", DOCS.iterator());

      DOCS = new ArrayList<Map<String,Object>>(SOLR_DOCS);
      Map<String,Object> solrDoc = new HashMap<String,Object>();
      solrDoc.put("id", "2");
      solrDoc.put("desc", "SolrDescription2");
      DOCS.add(solrDoc);
      addDocumentsToSolr(DOCS);
      runFullImport(getDihConfigTagsInnerEntity());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    } finally {
      MockDataSource.clearCache();
    }
    
    assertQ(req("*:*"), "//result[@numFound='2']");
    assertQ(req("id:1"), "//result/doc/str[@name='id'][.='1']",
        "//result/doc/str[@name='dbdesc_s'][.='DbDescription']",
        "//result/doc/str[@name='dbid_s'][.='1']",
        "//result/doc/arr[@name='desc'][.='SolrDescription']");
    assertQ(req("id:2"), "//result/doc/str[@name='id'][.='2']",
        "//result/doc/str[@name='dbdesc_s'][.='DbDescription2']",
        "//result/doc/str[@name='dbid_s'][.='2']",
        "//result/doc/arr[@name='desc'][.='SolrDescription2']");
  }
  
  public void testFullImportWrongSolrUrl() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      runFullImport(generateDIHConfig("query='*:*' rows='2' fl='id,desc' onError='skip'", true /* use dead server */));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
    
    assertQ(req("*:*"), "//result[@numFound='0']");
  }
  
  public void testFullImportBadConfig() {
    assertQ(req("*:*"), "//result[@numFound='0']");
    
    try {
      runFullImport(generateDIHConfig("query='bogus:3' rows='2' fl='id,desc' onError='abort'", false));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      fail(e.getMessage());
    }
    
    assertQ(req("*:*"), "//result[@numFound='0']");
  }
    
  private static List<Map<String,Object>> generateSolrDocuments(int num) {
    List<Map<String,Object>> docList = new ArrayList<Map<String,Object>>();
    for (int i = 1; i <= num; i++) {
      Map<String,Object> map = new HashMap<String,Object>();
      map.put("id", i);
      map.put("desc", "Description" + i);
      docList.add(map);
    }
    return docList;
  }
  
  private void addDocumentsToSolr(List<Map<String,Object>> docs) throws SolrServerException, IOException {
    List<SolrInputDocument> sidl = new ArrayList<SolrInputDocument>();
    for (Map<String,Object> doc : docs) {
      SolrInputDocument sd = new SolrInputDocument();
      for (Entry<String,Object> entry : doc.entrySet()) {
        sd.addField(entry.getKey(), entry.getValue());
      }
      sidl.add(sd);
    }
    
    HttpSolrServer solrServer = new HttpSolrServer(getSourceUrl());
    try {
      solrServer.setConnectionTimeout(15000);
      solrServer.setSoTimeout(30000);
      solrServer.add(sidl);
      solrServer.commit(true, true);
    } finally {
      solrServer.shutdown();
    }
  }
  
  private static class SolrInstance {

    File homeDir;
    File confDir;
    
    public String getHomeDir() {
      return homeDir.toString();
    }
    
    public String getSchemaFile() {
      return SOURCE_CONF_DIR + "dataimport-schema.xml";
    }
    
    public String getDataDir() {
      return dataDir.toString();
    }
    
    public String getSolrConfigFile() {
      return SOURCE_CONF_DIR + "dataimport-solrconfig.xml";
    }

    public String getSolrXmlFile() {
      return ROOT_DIR + "solr.xml";
    }

    public void setUp() throws Exception {
      
      File home = new File(TEMP_DIR, getClass().getName() + "-"
          + System.currentTimeMillis());
      
      homeDir = new File(home + "inst");
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
      FileUtils.copyFile(getFile(SOURCE_CONF_DIR + "dataconfig-contentstream.xml"), f);
    }
    
    public void tearDown() throws Exception {
      recurseDelete(homeDir);
    }
    
  }
  
  private JettySolrRunner createJetty(SolrInstance instance) throws Exception {
    System.setProperty("solr.data.dir", instance.getDataDir());
    JettySolrRunner jetty = new JettySolrRunner(instance.getHomeDir(), "/solr", 0, null, null, true, null, sslConfig);
    jetty.start();
    return jetty;
  }
  
}
