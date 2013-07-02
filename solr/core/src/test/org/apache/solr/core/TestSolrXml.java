package org.apache.solr.core;

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

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class TestSolrXml extends SolrTestCaseJ4 {
  private final File solrHome = new File(TEMP_DIR, TestSolrXml.getClassName() + File.separator + "solrHome");

  @Test
  public void testAllInfoPresent() throws IOException, ParserConfigurationException, SAXException {
    CoreContainer cc = null;
    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));
    try {
      InputStream is = new FileInputStream(new File(solrHome, "solr.xml"));
      Config config = new Config(new SolrResourceLoader("solr/collection1"), null, new InputSource(is), null, false);
      boolean oldStyle = (config.getNode("solr/cores", false) != null);
      ConfigSolr cfg;
      if (oldStyle) {
        cfg = new ConfigSolrXmlOld(config);
      } else {
        cfg = new ConfigSolrXml(config, cc);
      }

      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_ADMINHANDLER, null), "testAdminHandler");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, 0), 11);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_COREROOTDIRECTORY, null), "testCoreRootDirectory");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, 0), 22);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, 0), 33);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_HOST, null), "testHost");
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_HOSTCONTEXT, null), "testHostContext");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_HOSTPORT, 0), 44);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_LEADERVOTEWAIT, 0), 55);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_CLASS, null), "testLoggingClass");
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_ENABLED, null), "testLoggingEnabled");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_SIZE, 0), 88);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, 0), 99);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_MANAGEMENTPATH, null), "testManagementPath");
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_SHAREDLIB, null), "testSharedLib");
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_CLASS, null), "testHttpShardHandlerFactory");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_CONNTIMEOUT, 0), 110);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_NAME, null), "testShardHandlerFactory");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_SOCKETTIMEOUT, 0), 100);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, null), "testShareSchema");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_TRANSIENTCACHESIZE, 0), 66);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_ZKCLIENTTIMEOUT, 0), 77);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_ZKHOST, null), "testZkHost");
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_PERSISTENT, null));
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, null));
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_ADMINPATH, null));
    } finally {
      if (cc != null) cc.shutdown();
    }
  }

  // Test  a few property substitutions that happen to be in solr-50-all.xml.
  public void testPropretySub() throws IOException, ParserConfigurationException, SAXException {

    String coreRoot = System.getProperty("coreRootDirectory");
    String hostPort = System.getProperty("hostPort");
    String shareSchema = System.getProperty("shareSchema");
    String socketTimeout = System.getProperty("socketTimeout");
    String connTimeout = System.getProperty("connTimeout");
    System.setProperty("coreRootDirectory", "myCoreRoot");
    System.setProperty("hostPort", "8888");
    System.setProperty("shareSchema", "newShareSchema");
    System.setProperty("socketTimeout", "220");
    System.setProperty("connTimeout", "200");

    CoreContainer cc = null;
    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));
    try {
      InputStream is = new FileInputStream(new File(solrHome, "solr.xml"));
      Config config = new Config(new SolrResourceLoader("solr/collection1"), null, new InputSource(is), null, false);
      boolean oldStyle = (config.getNode("solr/cores", false) != null);
      ConfigSolr cfg;
      if (oldStyle) {
        cfg = new ConfigSolrXmlOld(config);
      } else {
        cfg = new ConfigSolrXml(config, cc);
      }
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_COREROOTDIRECTORY, null), "myCoreRoot");
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_HOSTPORT, 0), 8888);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_CONNTIMEOUT, 0), 200);
      assertEquals("Did not find expected value", cfg.getInt(ConfigSolr.CfgProp.SOLR_SHARDHANDLERFACTORY_SOCKETTIMEOUT, 0), 220);
      assertEquals("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, null), "newShareSchema");

    } finally {
      if (cc != null) cc.shutdown();
      if (coreRoot != null) System.setProperty("coreRootDirectory", coreRoot);
      else System.clearProperty("coreRootDirectory");

      if (hostPort != null) System.setProperty("hostPort", hostPort);
      else System.clearProperty("hostPort");

      if (shareSchema != null) System.setProperty("shareSchema", shareSchema);
      else System.clearProperty("shareSchema");

      if (socketTimeout != null) System.setProperty("socketTimeout", socketTimeout);
      else System.clearProperty("socketTimeout");

      if (connTimeout != null) System.setProperty("connTimeout", connTimeout);
      else System.clearProperty("connTimeout");

    }
  }
}

