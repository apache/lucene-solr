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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

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
  public void testNoCores() throws IOException, ParserConfigurationException, SAXException {
    //create solrHome
    File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName()
        + "_noCores");
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    try {
      File solrXmlFile = new File(solrHomeDirectory, "solr.xml");
      BufferedWriter out = new BufferedWriter(new FileWriter(solrXmlFile));
      out.write(EMPTY_SOLR_XML);
      out.close();
    } catch (IOException e) {
      FileUtils.deleteDirectory(solrHomeDirectory);
      throw e;
    }
    
    //init
    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
    CoreContainer.Initializer init = new CoreContainer.Initializer();
    CoreContainer cores = null;
    try {
      cores = init.initialize();
    }
    catch(Exception e) {
      fail("CoreContainer not created. " + e.getMessage());
    }
    try {
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      //add a new core
      CoreDescriptor coreDescriptor = new CoreDescriptor(cores, "core1", SolrTestCaseJ4.TEST_HOME());
      SolrCore newCore = cores.create(coreDescriptor);
      cores.register(newCore, false);
      
      //assert one registered core
      assertEquals("There core registered", 1, cores.getCores().size());
      
      newCore.close();
      cores.remove("core1");
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
    } finally {
      cores.shutdown();
      FileUtils.deleteDirectory(solrHomeDirectory);
    }

  }
  
  private static final String EMPTY_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\">\n" +
      "  </cores>\n" +
      "</solr>";
  
}
