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

import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestCoreDiscovery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final File solrHomeDirectory = new File(TEMP_DIR, "org.apache.solr.core.TestCoreDiscovery" + File.separator + "solrHome");

  private void setMeUp(String alternateCoreDir) throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());

    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
    String xmlStr = SOLR_XML;
    if (alternateCoreDir != null) {
      xmlStr = xmlStr.replace("<solr>", "<solr> <str name=\"coreRootDirectory\">" + alternateCoreDir + "</str> ");
    }
    File tmpFile = new File(solrHomeDirectory, ConfigSolr.SOLR_XML_FILE);
    FileUtils.write(tmpFile, xmlStr, IOUtils.CHARSET_UTF_8.toString());

  }

  private void setMeUp() throws Exception {
    setMeUp(null);
  }

  private Properties makeCorePropFile(String name, boolean isLazy, boolean loadOnStartup, String... extraProps) {
    Properties props = new Properties();
    props.put(CoreDescriptor.CORE_NAME, name);
    props.put(CoreDescriptor.CORE_SCHEMA, "schema-tiny.xml");
    props.put(CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml");
    props.put(CoreDescriptor.CORE_TRANSIENT, Boolean.toString(isLazy));
    props.put(CoreDescriptor.CORE_LOADONSTARTUP, Boolean.toString(loadOnStartup));
    props.put(CoreDescriptor.CORE_DATADIR, "${core.dataDir:stuffandnonsense}");
    props.put(CoreDescriptor.CORE_INSTDIR, "totallybogus"); // For testing that this property is ignored if present.

    for (String extra : extraProps) {
      String[] parts = extra.split("=");
      props.put(parts[0], parts[1]);
    }

    return props;
  }

  private void addCoreWithProps(Properties stockProps, File propFile) throws Exception {
    if (!propFile.getParentFile().exists()) propFile.getParentFile().mkdirs();
    FileOutputStream out = new FileOutputStream(propFile);
    try {
      stockProps.store(out, null);
    } finally {
      out.close();
    }

    addConfFiles(new File(propFile.getParent(), "conf"));

  }

  private void addCoreWithProps(Properties stockProps) throws Exception {

    File propFile = new File(solrHomeDirectory,
        stockProps.getProperty(CoreDescriptor.CORE_NAME) + File.separator + SolrCoreDiscoverer.CORE_PROP_FILE);
    File parent = propFile.getParentFile();
    assertTrue("Failed to mkdirs for " + parent.getAbsolutePath(), parent.mkdirs());
    addCoreWithProps(stockProps, propFile);
  }

  private void addConfFiles(File confDir) throws Exception {
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    assertTrue("Failed to mkdirs for " + confDir.getAbsolutePath(), confDir.mkdirs());
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(confDir, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(confDir, "solrconfig-minimal.xml"));
  }

  private CoreContainer init() throws Exception {

    CoreContainer.Initializer init = new CoreContainer.Initializer();

    final CoreContainer cores = init.initialize();
    cores.setPersistent(false);
    return cores;
  }

  @After
  public void after() throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
  }

  // Test the basic setup, create some dirs with core.properties files in them, but solr.xml has discoverCores
  // set and insure that we find all the cores and can load them.
  @Test
  public void testDiscovery() throws Exception {
    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps(makeCorePropFile("core1", false, true, "dataDir=core1"));
    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"));

    // I suspect what we're adding in here is a "configset" rather than a schema or solrconfig.
    //
    addCoreWithProps(makeCorePropFile("lazy1", true, false, "dataDir=lazy1"));

    CoreContainer cc = init();
    try {
      assertNull("defaultCore no longer allowed in solr.xml", cc.getDefaultCoreName());

      TestLazyCores.checkInCores(cc, "core1");
      TestLazyCores.checkNotInCores(cc, "lazy1", "core2", "collection1");

      SolrCore core1 = cc.getCore("core1");

      // Let's assert we did the right thing for implicit properties too.
      CoreDescriptor desc = core1.getCoreDescriptor();
      assertEquals("core1", desc.getProperty("solr.core.name"));

      // Prove we're ignoring this even though it's set in the properties file
      assertFalse("InstanceDir should be ignored", desc.getProperty("solr.core.instanceDir").contains("totallybogus"));

      // This is too long and ugly to put in. Besides, it varies.
      assertNotNull(desc.getProperty("solr.core.instanceDir"));

      assertEquals("core1", desc.getProperty("solr.core.dataDir"));
      assertEquals("solrconfig-minimal.xml", desc.getProperty("solr.core.configName"));
      assertEquals("schema-tiny.xml", desc.getProperty("solr.core.schemaName"));

      SolrCore core2 = cc.getCore("core2");
      SolrCore lazy1 = cc.getCore("lazy1");
      TestLazyCores.checkInCores(cc, "core1", "core2", "lazy1");
      core1.close();
      core2.close();
      lazy1.close();

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testAlternateCoreDir() throws Exception {
    File alt = new File(TEMP_DIR, "alternateCoreDir");
    if (alt.exists()) FileUtils.deleteDirectory(alt);
    alt.mkdirs();
    setMeUp(alt.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true, "dataDir=core1"),
        new File(alt, "core1" + File.separator + SolrCoreDiscoverer.CORE_PROP_FILE));
    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"),
        new File(alt, "core2" + File.separator + SolrCoreDiscoverer.CORE_PROP_FILE));
    CoreContainer cc = init();
    try {
      SolrCore core1 = cc.getCore("core1");
      SolrCore core2 = cc.getCore("core2");
      assertNotNull(core1);
      assertNotNull(core2);
      core1.close();
      core2.close();
    } finally {
      cc.shutdown();
      if (alt.exists()) FileUtils.deleteDirectory(alt);
    }
  }

  // For testing whether finding a solr.xml overrides looking at solr.properties
  private final static String SOLR_XML = "<solr> " +
      "<int name=\"transientCacheSize\">2</int> " +
      "<solrcloud> " +
      "<str name=\"hostContext\">solrprop</str> " +
      "<int name=\"zkClientTimeout\">20</int> " +
      "<str name=\"host\">222.333.444.555</str> " +
      "<int name=\"hostPort\">6000</int>  " +
      "</solrcloud> " +
      "</solr>";
}
