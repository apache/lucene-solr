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

import java.io.File;
import java.io.FileInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.UpdateRequest.ACTION;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FileUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.AbstractSolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

/**
 *
 * @since solr 1.3
 */
public class TestSolrProperties extends LuceneTestCase {
  protected static Logger log = LoggerFactory.getLogger(TestSolrProperties.class);
  protected CoreContainer cores = null;
  private File home;
  private File solrXml;
  
  private static final XPathFactory xpathFactory = XPathFactory.newInstance();

  public String getSolrHome() {
    return "solr/shared";
  }

  public String getOrigSolrXml() {
    return "solr.xml";
  }

  public String getSolrXml() {
    return "test-solr.xml";
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    System.setProperty("solr.solr.home", getSolrHome());
    
    home = SolrTestCaseJ4.getFile(getSolrHome());
    System.setProperty("solr.solr.home", home.getAbsolutePath());

    log.info("pwd: " + (new File(".")).getAbsolutePath());
    File origSolrXml = new File(home, getOrigSolrXml());
    solrXml = new File(home, getSolrXml());
    FileUtils.copyFile(origSolrXml, solrXml);
    cores = new CoreContainer(home.getAbsolutePath(), solrXml);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    if (cores != null)
      cores.shutdown();
    File dataDir = new File(home,"data");
    String skip = System.getProperty("solr.test.leavedatadir");
    if (null != skip && 0 != skip.trim().length()) {
      log.info("NOTE: per solr.test.leavedatadir, dataDir will not be removed: " + dataDir.getAbsolutePath());
    } else {
      if (!AbstractSolrTestCase.recurseDelete(dataDir)) {
        log.warn("!!!! WARNING: best effort to remove " + dataDir.getAbsolutePath() + " FAILED !!!!!");
      }
    }
    File persistedFile = new File(home,"solr-persist.xml");
    assertTrue("Failed to delete "+persistedFile, persistedFile.delete());
    assertTrue("Failed to delete "+solrXml, solrXml.delete());
    super.tearDown();
  }

  protected SolrServer getSolrCore0() {
    return new EmbeddedSolrServer(cores, "core0");
  }


  protected SolrServer getSolrCore1() {
    return new EmbeddedSolrServer(cores, "core1");
  }

  protected SolrServer getSolrAdmin() {
    return new EmbeddedSolrServer(cores, "core0");
  }
  
  protected SolrServer getRenamedSolrAdmin() {
    return new EmbeddedSolrServer(cores, "renamed_core");
  }

  protected SolrServer getSolrCore(String name) {
    return new EmbeddedSolrServer(cores, name);
  }

  @Test
  public void testProperties() throws Exception {
    UpdateRequest up = new UpdateRequest();
    up.setAction(ACTION.COMMIT, true, true);
    up.deleteByQuery("*:*");
    up.process(getSolrCore0());
    up.process(getSolrCore1());
    up.clear();

    // Add something to each core
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField("id", "AAA");
    doc.setField("core0", "yup stopfra stopfrb stopena stopenb");

    // Add to core0
    up.add(doc);
    up.process(getSolrCore0());

    SolrTestCaseJ4.ignoreException("unknown field");

    // You can't add it to core1
    try {
      up.process(getSolrCore1());
      fail("Can't add core0 field to core1!");
    }
    catch (Exception ex) {
    }

    // Add to core1
    doc.setField("id", "BBB");
    doc.setField("core1", "yup stopfra stopfrb stopena stopenb");
    doc.removeField("core0");
    up.add(doc);
    up.process(getSolrCore1());

    // You can't add it to core1
    try {
      SolrTestCaseJ4.ignoreException("core0");
      up.process(getSolrCore0());
      fail("Can't add core1 field to core0!");
    }
    catch (Exception ex) {
    }
    
    SolrTestCaseJ4.resetExceptionIgnores();

    // now Make sure AAA is in 0 and BBB in 1
    SolrQuery q = new SolrQuery();
    QueryRequest r = new QueryRequest(q);
    q.setQuery("id:AAA");
    assertEquals(1, r.process(getSolrCore0()).getResults().size());
    assertEquals(0, r.process(getSolrCore1()).getResults().size());

    // Now test Changing the default core
    assertEquals(1, getSolrCore0().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(0, getSolrCore0().query(new SolrQuery("id:BBB")).getResults().size());

    assertEquals(0, getSolrCore1().query(new SolrQuery("id:AAA")).getResults().size());
    assertEquals(1, getSolrCore1().query(new SolrQuery("id:BBB")).getResults().size());

    // Now test reloading it should have a newer open time
    String name = "core0";
    SolrServer coreadmin = getSolrAdmin();
    CoreAdminResponse mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long before = mcr.getStartTime(name).getTime();
    CoreAdminRequest.reloadCore(name, coreadmin);

    mcr = CoreAdminRequest.getStatus(name, coreadmin);
    long after = mcr.getStartTime(name).getTime();
    assertTrue("should have more recent time: " + after + "," + before, after > before);

    mcr = CoreAdminRequest.persist("solr-persist.xml", coreadmin);
    
    //System.out.println(IOUtils.toString(new FileInputStream(new File(solrXml.getParent(), "solr-persist.xml"))));
    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    FileInputStream fis = new FileInputStream(new File(solrXml.getParent(), "solr-persist.xml"));
    try {
      Document document = builder.parse(fis);
      assertTrue(exists("/solr/cores[@defaultCoreName='core0']", document));
      assertTrue(exists("/solr/cores[@host='127.0.0.1']", document));
      assertTrue(exists("/solr/cores[@hostPort='8983']", document));
      assertTrue(exists("/solr/cores[@zkClientTimeout='8000']", document));
      assertTrue(exists("/solr/cores[@hostContext='solr']", document));
      
    } finally {
      fis.close();
    }
    
    CoreAdminRequest.renameCore(name, "renamed_core", coreadmin);
    mcr = CoreAdminRequest.persist("solr-persist.xml", getRenamedSolrAdmin());
    
    fis = new FileInputStream(new File(solrXml.getParent(), "solr-persist.xml"));
    try {
      Document document = builder.parse(fis);
      assertTrue(exists("/solr/cores/core[@name='renamed_core']", document));
    } finally {
      fis.close();
    }
  }
  
  public static boolean exists(String xpathStr, Node node)
      throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    return (Boolean) xpath.evaluate(xpathStr, node, XPathConstants.BOOLEAN);
  }
}
