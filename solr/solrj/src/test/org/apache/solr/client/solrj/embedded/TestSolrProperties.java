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

package org.apache.solr.client.solrj.embedded;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.*;

import org.apache.commons.io.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest.ACTION;
import org.apache.solr.client.solrj.request.*;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;

/**
 *
 * @since solr 1.3
 */
public class TestSolrProperties extends AbstractEmbeddedSolrServerTestCase {
  protected static Logger log = LoggerFactory.getLogger(TestSolrProperties.class);

  private static final String SOLR_XML = "solr.xml";
  private static final String SOLR_PERSIST_XML = "solr-persist.xml";

  @Rule
  public TestRule solrTestRules = 
    RuleChain.outerRule(new SystemPropertiesRestoreRule());

  private static final XPathFactory xpathFactory = XPathFactory.newInstance();

  @Override
  protected File getSolrXml() throws Exception {
    //This test writes on the directory where the solr.xml is located. Better to copy the solr.xml to
    //the temporary directory where we store the index
    File origSolrXml = new File(SOLR_HOME, SOLR_XML);
    File solrXml = new File(tempDir, SOLR_XML);
    FileUtils.copyFile(origSolrXml, solrXml);
    return solrXml;
  }

  @Override
  protected void deleteAdditionalFiles() {
    super.deleteAdditionalFiles();

    //Cleans the solr.xml persisted while testing and the solr.xml copied to the temporary directory
    File persistedFile = new File(tempDir, SOLR_PERSIST_XML);
    assertTrue("Failed to delete "+persistedFile, persistedFile.delete());
    File solrXml = new File(tempDir, SOLR_XML);
    assertTrue("Failed to delete "+ solrXml, solrXml.delete());
  }

  protected SolrServer getSolrAdmin() {
    return new EmbeddedSolrServer(cores, "core0");
  }
  
  protected SolrServer getRenamedSolrAdmin() {
    return new EmbeddedSolrServer(cores, "renamed_core");
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

    mcr = CoreAdminRequest.persist(SOLR_PERSIST_XML, coreadmin);

    DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    FileInputStream fis = new FileInputStream(new File(tempDir, SOLR_PERSIST_XML));
    try {
      Document document = builder.parse(fis);
      fis.close();
      fis = new FileInputStream(new File(tempDir, SOLR_PERSIST_XML));
      String solrPersistXml = IOUtils.toString(new InputStreamReader(fis, "UTF-8"));
      //System.out.println("xml:" + solrPersistXml);
      assertTrue("\"/solr/cores[@defaultCoreName='core0']\" doesn't match in:\n" + solrPersistXml,
                 exists("/solr/cores[@defaultCoreName='core0']", document));
      assertTrue("\"/solr/cores[@host='127.0.0.1']\" doesn't match in:\n" + solrPersistXml,
                 exists("/solr/cores[@host='127.0.0.1']", document));
      assertTrue("\"/solr/cores[@hostPort='${hostPort:8983}']\" doesn't match in:\n" + solrPersistXml,
                 exists("/solr/cores[@hostPort='${hostPort:8983}']", document));
      assertTrue("\"/solr/cores[@zkClientTimeout='8000']\" doesn't match in:\n" + solrPersistXml,
                 exists("/solr/cores[@zkClientTimeout='8000']", document));
      assertTrue("\"/solr/cores[@hostContext='solr']\" doesn't match in:\n" + solrPersistXml,
                 exists("/solr/cores[@hostContext='solr']", document));
      
    } finally {
      fis.close();
    }
    
    CoreAdminRequest.renameCore(name, "renamed_core", coreadmin);
    mcr = CoreAdminRequest.persist(SOLR_PERSIST_XML, getRenamedSolrAdmin());
    
//    fis = new FileInputStream(new File(solrXml.getParent(), SOLR_PERSIST_XML));
//    String solrPersistXml = IOUtils.toString(fis);
//    System.out.println("xml:" + solrPersistXml);
//    fis.close();
    
    fis = new FileInputStream(new File(tempDir, SOLR_PERSIST_XML));
    try {
      Document document = builder.parse(fis);
      assertTrue(exists("/solr/cores/core[@name='renamed_core']", document));
      assertTrue(exists("/solr/cores/core[@instanceDir='${theInstanceDir:./}']", document));
      assertTrue(exists("/solr/cores/core[@collection='${collection:acollection}']", document));
      
    } finally {
      fis.close();
    }
    
    coreadmin = getRenamedSolrAdmin();
    CoreAdminRequest.createCore("newCore", SOLR_HOME.getAbsolutePath(), coreadmin);
    
//    fis = new FileInputStream(new File(solrXml.getParent(), SOLR_PERSIST_XML));
//    solrPersistXml = IOUtils.toString(fis);
//    System.out.println("xml:" + solrPersistXml);
//    fis.close();
    
    mcr = CoreAdminRequest.persist(SOLR_PERSIST_XML, getRenamedSolrAdmin());
    
//    fis = new FileInputStream(new File(solrXml.getParent(), SOLR_PERSIST_XML));
//    solrPersistXml = IOUtils.toString(fis);
//    System.out.println("xml:" + solrPersistXml);
//    fis.close();
    
    fis = new FileInputStream(new File(tempDir, SOLR_PERSIST_XML));
    try {
      Document document = builder.parse(fis);
      assertTrue(exists("/solr/cores/core[@name='collection1' and (@instanceDir='./' or @instanceDir='.\\')]", document));
    } finally {
      fis.close();
    }
    
    // test reload and parse
    cores.shutdown();
    
    cores = new CoreContainer(SOLR_HOME.getAbsolutePath(), new File(tempDir, SOLR_PERSIST_XML));
 
    
    mcr = CoreAdminRequest.persist(SOLR_PERSIST_XML, getRenamedSolrAdmin());
    
//     fis = new FileInputStream(new File(solrXml.getParent(),
//     SOLR_PERSIST_XML));
//     solrPersistXml = IOUtils.toString(fis);
//     System.out.println("xml:" + solrPersistXml);
//     fis.close();
  }
  
  public static boolean exists(String xpathStr, Node node)
      throws XPathExpressionException {
    XPath xpath = xpathFactory.newXPath();
    return (Boolean) xpath.evaluate(xpathStr, node, XPathConstants.BOOLEAN);
  }
}
