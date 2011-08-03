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

package org.apache.solr.core;

import java.io.File;
import java.io.IOException;

import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

public class TestCoreContainer extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml", "schema.xml");
  }
  
  @Test
  public void testPersist() throws Exception {
    final File workDir = new File(TEMP_DIR, this.getClass().getName()
        + "_persist");
    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    
    final CoreContainer cores = h.getCoreContainer();
    cores.setPersistent(true); // is this needed since we make explicit calls?
    
    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }
    
    final File instDirFile = new File(instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());
    
    // sanity check the basic persistence of the default init
    
    final File oneXml = new File(workDir, "1.solr.xml");
    cores.persistFile(oneXml);
    
    assertXmlFile(oneXml, "/solr[@persistent='true']",
        "/solr/cores[@defaultCoreName='collection1']",
        "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
            + "']", "1=count(/solr/cores/core)");
    
    // create some new cores and sanity check the persistence
    
    final File dataXfile = new File(workDir, "dataX");
    final String dataX = dataXfile.getAbsolutePath();
    assertTrue("dataXfile mkdirs failed: " + dataX, dataXfile.mkdirs());
    
    final File instYfile = new File(workDir, "instY");
    FileUtils.copyDirectory(instDirFile, instYfile);
    
    // :HACK: dataDir leaves off trailing "/", but instanceDir uses it
    final String instY = instYfile.getAbsolutePath() + "/";
    
    final CoreDescriptor xd = new CoreDescriptor(cores, "X", instDir);
    xd.setDataDir(dataX);
    
    final CoreDescriptor yd = new CoreDescriptor(cores, "Y", instY);
    
    SolrCore x = null;
    SolrCore y = null;
    try {
      x = cores.create(xd);
      y = cores.create(yd);
      cores.register(x, false);
      cores.register(y, false);
      
      assertEquals("cores not added?", 3, cores.getCoreNames().size());
      
      final File twoXml = new File(workDir, "2.solr.xml");
      cores.persistFile(twoXml);
      
      assertXmlFile(twoXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='X' and @instanceDir='" + instDir
              + "' and @dataDir='" + dataX + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "3=count(/solr/cores/core)");
      
      // delete a core, check persistence again
      assertNotNull("removing X returned null", cores.remove("X"));
      
      final File threeXml = new File(workDir, "3.solr.xml");
      cores.persistFile(threeXml);
      
      assertXmlFile(threeXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='Y' and @instanceDir='" + instY
              + "']", "2=count(/solr/cores/core)");
      
      // sanity check that persisting w/o changes has no changes
      
      final File fourXml = new File(workDir, "4.solr.xml");
      cores.persistFile(fourXml);
      
      assertTrue("3 and 4 should be identical files",
          FileUtils.contentEquals(threeXml, fourXml));
      
    } finally {
      if (x != null) {
        try {
          x.close();
        } catch (Exception e) {
          log.error("", e);
        }
      }
      if (y != null) {
        try {
          y.close();
        } catch (Exception e) {
          log.error("", e);
        }
      }
    }
  }
  
  public void assertXmlFile(final File file, String... xpath)
      throws IOException, SAXException {
    
    try {
      String xml = FileUtils.readFileToString(file, "UTF-8");
      String results = h.validateXPath(xml, xpath);
      if (null != results) {
        String msg = "File XPath failure: file=" + file.getPath() + " xpath="
            + results + "\n\nxml was: " + xml;
        fail(msg);
      }
    } catch (XPathExpressionException e2) {
      throw new RuntimeException("XPath is invalid", e2);
    }
  }
  
}
