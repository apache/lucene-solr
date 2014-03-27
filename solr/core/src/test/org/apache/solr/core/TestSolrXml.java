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

import com.carrotsearch.randomizedtesting.rules.SystemPropertiesRestoreRule;
import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.IOException;

public class TestSolrXml extends SolrTestCaseJ4 {

  @Rule
  public TestRule solrTestRules = RuleChain.outerRule(new SystemPropertiesRestoreRule());

  private final File solrHome = new File(dataDir, TestSolrXml.getClassName() + File.separator + "solrHome");

  @Test
  public void testAllInfoPresent() throws IOException {

    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));

    SolrResourceLoader loader = null;
    try {
      loader = new SolrResourceLoader(solrHome.getAbsolutePath());
      ConfigSolr cfg = ConfigSolr.fromSolrHome(loader, solrHome.getAbsolutePath());

      assertEquals("Did not find expected value", "testAdminHandler", cfg.get(ConfigSolr.CfgProp.SOLR_ADMINHANDLER, null));
      assertEquals("Did not find expected value", "testCollectionsHandler", cfg.get(ConfigSolr.CfgProp.SOLR_COLLECTIONSHANDLER, null));
      assertEquals("Did not find expected value", "testInfoHandler", cfg.get(ConfigSolr.CfgProp.SOLR_INFOHANDLER, null));
      assertEquals("Did not find expected value", 11, cfg.getInt(ConfigSolr.CfgProp.SOLR_CORELOADTHREADS, 0));
      assertEquals("Did not find expected value", "testCoreRootDirectory", cfg.get(ConfigSolr.CfgProp.SOLR_COREROOTDIRECTORY, null));
      assertEquals("Did not find expected value", 22, cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATECONNTIMEOUT, 0));
      assertEquals("Did not find expected value", 33, cfg.getInt(ConfigSolr.CfgProp.SOLR_DISTRIBUPDATESOTIMEOUT, 0));
      assertEquals("Did not find expected value", 3, cfg.getInt(ConfigSolr.CfgProp.SOLR_MAXUPDATECONNECTIONS, 0));
      assertEquals("Did not find expected value", 37, cfg.getInt(ConfigSolr.CfgProp.SOLR_MAXUPDATECONNECTIONSPERHOST, 0));
      assertEquals("Did not find expected value", "testHost", cfg.get(ConfigSolr.CfgProp.SOLR_HOST, null));
      assertEquals("Did not find expected value", "testHostContext", cfg.get(ConfigSolr.CfgProp.SOLR_HOSTCONTEXT, null));
      assertEquals("Did not find expected value", 44, cfg.getInt(ConfigSolr.CfgProp.SOLR_HOSTPORT, 0));
      assertEquals("Did not find expected value", 55, cfg.getInt(ConfigSolr.CfgProp.SOLR_LEADERVOTEWAIT, 0));
      assertEquals("Did not find expected value", "testLoggingClass", cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_CLASS, null));
      assertEquals("Did not find expected value", "testLoggingEnabled", cfg.get(ConfigSolr.CfgProp.SOLR_LOGGING_ENABLED, null));
      assertEquals("Did not find expected value", 88, cfg.getInt(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_SIZE, 0));
      assertEquals("Did not find expected value", 99, cfg.getInt(ConfigSolr.CfgProp.SOLR_LOGGING_WATCHER_THRESHOLD, 0));
      assertEquals("Did not find expected value", "testManagementPath", cfg.get(ConfigSolr.CfgProp.SOLR_MANAGEMENTPATH, null));
      assertEquals("Did not find expected value", "testSharedLib", cfg.get(ConfigSolr.CfgProp.SOLR_SHAREDLIB, null));
      assertEquals("Did not find expected value", "testShareSchema", cfg.get(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, null));
      assertEquals("Did not find expected value", 66, cfg.getInt(ConfigSolr.CfgProp.SOLR_TRANSIENTCACHESIZE, 0));
      assertEquals("Did not find expected value", 77, cfg.getInt(ConfigSolr.CfgProp.SOLR_ZKCLIENTTIMEOUT, 0));
      assertEquals("Did not find expected value", "testZkHost", cfg.get(ConfigSolr.CfgProp.SOLR_ZKHOST, null));
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_PERSISTENT, null));
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_CORES_DEFAULT_CORE_NAME, null));
      assertNull("Did not find expected value", cfg.get(ConfigSolr.CfgProp.SOLR_ADMINPATH, null));
    }
    finally {
      loader.close();
    }

  }

  // Test  a few property substitutions that happen to be in solr-50-all.xml.
  public void testPropertySub() throws IOException {

    System.setProperty("coreRootDirectory", "myCoreRoot");
    System.setProperty("hostPort", "8888");
    System.setProperty("shareSchema", "newShareSchema");
    System.setProperty("socketTimeout", "220");
    System.setProperty("connTimeout", "200");

    File testSrcRoot = new File(SolrTestCaseJ4.TEST_HOME());
    FileUtils.copyFile(new File(testSrcRoot, "solr-50-all.xml"), new File(solrHome, "solr.xml"));

    SolrResourceLoader loader = null;
    try {
      loader = new SolrResourceLoader(solrHome.getAbsolutePath());
      ConfigSolr cfg = ConfigSolr.fromSolrHome(loader, solrHome.getAbsolutePath());
      assertEquals("Did not find expected value", "myCoreRoot", cfg.get(ConfigSolr.CfgProp.SOLR_COREROOTDIRECTORY, null));
      assertEquals("Did not find expected value", 8888, cfg.getInt(ConfigSolr.CfgProp.SOLR_HOSTPORT, 0));
      assertEquals("Did not find expected value", "newShareSchema", cfg.get(ConfigSolr.CfgProp.SOLR_SHARESCHEMA, null));
    }
    finally {
      loader.close();
    }
  }
}

