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
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.util.Properties;
import java.util.Set;

public class TestCoreDiscovery extends SolrTestCaseJ4 {
  private static String NEW_LINE = System.getProperty("line.separator");

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final File solrHomeDirectory = new File(TEMP_DIR, "org.apache.solr.core.TestCoreDiscovery" + File.separator + "solrHome");

  private void setMeUp() throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
  }

  private void addSolrXml() throws Exception {
    File tmpFile = new File(solrHomeDirectory, ConfigSolr.SOLR_XML_FILE);
    FileUtils.write(tmpFile, SOLR_XML, IOUtils.CHARSET_UTF_8.toString());
  }

  private Properties makeCorePropFile(String name, boolean isLazy, boolean loadOnStartup, String... extraProps) {
    Properties props = new Properties();
    props.put(CoreDescriptor.CORE_NAME, name);
    props.put(CoreDescriptor.CORE_SCHEMA, "schema-tiny.xml");
    props.put(CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml");
    props.put(CoreDescriptor.CORE_TRANSIENT, Boolean.toString(isLazy));
    props.put(CoreDescriptor.CORE_LOADONSTARTUP, Boolean.toString(loadOnStartup));
    props.put(CoreDescriptor.CORE_DATADIR, "${core.dataDir:stuffandnonsense}");

    for (String extra : extraProps) {
      String[] parts = extra.split("=");
      props.put(parts[0], parts[1]);
    }

    return props;
  }

  private void addCoreWithProps(Properties stockProps) throws Exception {

    File propFile = new File(solrHomeDirectory,
        stockProps.getProperty(CoreDescriptor.CORE_NAME) + File.separator + ConfigSolr.CORE_PROP_FILE);
    File parent = propFile.getParentFile();
    assertTrue("Failed to mkdirs for " + parent.getAbsolutePath(), parent.mkdirs());

    FileOutputStream out = new FileOutputStream(propFile);
    try {
      stockProps.store(out, null);
    } finally {
      out.close();
    }

    addConfFiles(new File(parent, "conf"));
  }

  // For testing error condition of having multiple cores with the same name.
  private void addCoreWithPropsDir(String coreDir, Properties stockProps) throws Exception {

    File propFile = new File(solrHomeDirectory, coreDir + File.separator + ConfigSolr.CORE_PROP_FILE);
    File parent = propFile.getParentFile();
    assertTrue("Failed to mkdirs for " + parent.getAbsolutePath(), parent.mkdirs());

    FileOutputStream out = new FileOutputStream(propFile);
    try {
      stockProps.store(out, null);
    } finally {
      out.close();
    }

    addConfFiles(new File(parent, "conf"));
  }


  private void addConfFiles(File confDir) throws Exception {
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    assertTrue("Failed to mkdirs for " + confDir.getAbsolutePath(), confDir.mkdirs());
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(confDir, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(confDir, "solrconfig-minimal.xml"));
  }

  private void addConfigsForBackCompat() throws Exception {
    addConfFiles(new File(solrHomeDirectory, "collection1" + File.separator + "conf"));
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
  public void testDiscover() throws Exception {
    setMeUp();
    addSolrXml();
    // name, isLazy, loadOnStartup
    addCoreWithProps(makeCorePropFile("core1", false, true));
    addCoreWithProps(makeCorePropFile("core2", false, false));

    // I suspect what we're adding in here is a "configset" rather than a schema or solrconfig.
    //
    addCoreWithProps(makeCorePropFile("lazy1", true, false));

    CoreContainer cc = init();
    try {
      Properties props = cc.containerProperties;

      assertEquals("/admin/cores", cc.getAdminPath());
      assertEquals("defcore", cc.getDefaultCoreName());
      assertEquals("222.333.444.555", cc.getHost());
      assertEquals("6000", cc.getHostPort());
      assertEquals("solrprop", cc.getHostContext());
      assertEquals(20, cc.getZkClientTimeout());

      TestLazyCores.checkInCores(cc, "core1");
      TestLazyCores.checkNotInCores(cc, "lazy1", "core2", "collection1");

      SolrCore core1 = cc.getCore("core1");

      // Let's assert we did the right thing for implicit properties too.
      CoreDescriptor desc = core1.getCoreDescriptor();
      assertEquals("core1", desc.getProperty("solr.core.name"));

      // This is too long and ugly to put in. Besides, it varies.
      assertNotNull(desc.getProperty("solr.core.instanceDir"));

      assertEquals("stuffandnonsense", desc.getProperty("solr.core.dataDir"));
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


  // Check that the various flavors of persistence work, including saving the state of a core when it's being swapped
  // out. Added a test in here to insure that files that have config variables are saved with the config vars not the
  // substitutions.
  @Test
  public void testPersistTrue() throws Exception {
    setMeUp();
    addSolrXml();
    System.setProperty("solr.persistent", "true");

    Properties special = makeCorePropFile("core1", false, true);
    special.put(CoreDescriptor.CORE_INSTDIR, "${core1inst:anothersillypath}");
    addCoreWithProps(special);
    addCoreWithProps(makeCorePropFile("core2", false, false));
    addCoreWithProps(makeCorePropFile("lazy1", true, true));
    addCoreWithProps(makeCorePropFile("lazy2", true, true));
    addCoreWithProps(makeCorePropFile("lazy3", true, false));

    System.setProperty("core1inst", "core1");
    CoreContainer cc = init();
    SolrCore coreC1 = cc.getCore("core1");
    addCoreProps(coreC1, "addedPropC1=addedC1", "addedPropC1B=foo", "addedPropC1C=bar");

    SolrCore coreC2 = cc.getCore("core2");
    addCoreProps(coreC2, "addedPropC2=addedC2", "addedPropC2B=foo", "addedPropC2C=bar");

    SolrCore coreL1 = cc.getCore("lazy1");
    addCoreProps(coreL1, "addedPropL1=addedL1", "addedPropL1B=foo", "addedPropL1C=bar");

    SolrCore coreL2 = cc.getCore("lazy2");
    addCoreProps(coreL2, "addedPropL2=addedL2", "addedPropL2B=foo", "addedPropL2C=bar");

    SolrCore coreL3 = cc.getCore("lazy3");
    addCoreProps(coreL3, "addedPropL3=addedL3", "addedPropL3B=foo", "addedPropL3C=bar");

    try {
      cc.persist();

      // Insure that one of the loaded cores was swapped out, with a cache size of 2 lazy1 should be gone.
      TestLazyCores.checkInCores(cc, "core1", "core2", "lazy2", "lazy3");
      TestLazyCores.checkNotInCores(cc, "lazy1");

      Properties orig = makeCorePropFile("core1", false, true);
      orig.put(CoreDescriptor.CORE_INSTDIR, "${core1inst:anothersillypath}");
      checkCoreProps(orig, "addedPropC1=addedC1", "addedPropC1B=foo", "addedPropC1C=bar");

      orig = makeCorePropFile("core2", false, false);
      checkCoreProps(orig, "addedPropC2=addedC2", "addedPropC2B=foo", "addedPropC2C=bar");

      // This test insures that a core that was swapped out has its properties file persisted. Currently this happens
      // as the file is removed from the cache.
      orig = makeCorePropFile("lazy1", true, true);
      checkCoreProps(orig, "addedPropL1=addedL1", "addedPropL1B=foo", "addedPropL1C=bar");

      orig = makeCorePropFile("lazy2", true, true);
      checkCoreProps(orig, "addedPropL2=addedL2", "addedPropL2B=foo", "addedPropL2C=bar");

      orig = makeCorePropFile("lazy3", true, false);
      checkCoreProps(orig, "addedPropL3=addedL3", "addedPropL3B=foo", "addedPropL3C=bar");

      coreC1.close();
      coreC2.close();
      coreL1.close();
      coreL2.close();
      coreL3.close();

    } finally {
      cc.shutdown();
    }
  }

  // Make sure that, even if we do call persist, nothing's saved unless the flag is set in solr.properties.
  @Test
  public void testPersistFalse() throws Exception {
    setMeUp();
    addSolrXml();

    addCoreWithProps(makeCorePropFile("core1", false, true));
    addCoreWithProps(makeCorePropFile("core2", false, false));
    addCoreWithProps(makeCorePropFile("lazy1", true, true));
    addCoreWithProps(makeCorePropFile("lazy2", false, true));

    CoreContainer cc = init();
    SolrCore coreC1 = cc.getCore("core1");
    addCoreProps(coreC1, "addedPropC1=addedC1", "addedPropC1B=foo", "addedPropC1C=bar");

    SolrCore coreC2 = cc.getCore("core2");
    addCoreProps(coreC2, "addedPropC2=addedC2", "addedPropC2B=foo", "addedPropC2C=bar");

    SolrCore coreL1 = cc.getCore("lazy1");
    addCoreProps(coreL1, "addedPropL1=addedL1", "addedPropL1B=foo", "addedPropL1C=bar");

    SolrCore coreL2 = cc.getCore("lazy2");
    addCoreProps(coreL2, "addedPropL2=addedL2", "addedPropL2B=foo", "addedPropL2C=bar");


    try {
      cc.persist();

      checkCoreProps(makeCorePropFile("core1", false, true));
      checkCoreProps(makeCorePropFile("core2", false, false));
      checkCoreProps(makeCorePropFile("lazy1", true, true));
      checkCoreProps(makeCorePropFile("lazy2", false, true));

      coreC1.close();
      coreC2.close();
      coreL1.close();
      coreL2.close();
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCoresWithSameNameError() throws Exception {
    setMeUp();
    addSolrXml();
    addCoreWithPropsDir("core1_1", makeCorePropFile("core1", false, true));
    addCoreWithPropsDir("core1_2", makeCorePropFile("core1", false, true));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core defined for core named core1") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testCoresWithSameDataDirError() throws Exception{
    setMeUp();
    addSolrXml();
    addCoreWithProps(makeCorePropFile("core1", false, true, "dataDir=" + solrHomeDirectory + "datadir"));
    addCoreWithProps(makeCorePropFile("core2", false, true, "dataDir=" + solrHomeDirectory + "datadir"));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core points to data dir") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testCoresWithSameNameErrorTransient() throws Exception {
    setMeUp();
    addSolrXml();
    addCoreWithPropsDir("core1_1", makeCorePropFile("core1", true, false));
    addCoreWithPropsDir("core1_2", makeCorePropFile("core1", true, false));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core defined for core named core1") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testCoresWithSameDataDirErrorTransient() throws Exception{
    setMeUp();
    addSolrXml();
    addCoreWithProps(makeCorePropFile("core1", true, false, "dataDir=" + solrHomeDirectory + "datadir"));
    addCoreWithProps(makeCorePropFile("core2", true, false, "dataDir=" + solrHomeDirectory + "datadir"));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core points to data dir") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }


  @Test
  public void testCoresWithSameNameErrorboth() throws Exception {
    setMeUp();
    addSolrXml();
    addCoreWithPropsDir("core1_1", makeCorePropFile("core1", true,  false));
    addCoreWithPropsDir("core1_2", makeCorePropFile("core1", false, false));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core defined for core named core1") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testCoresWithSameDataDirErrorBoth() throws Exception{
    setMeUp();
    addSolrXml();
    addCoreWithProps(makeCorePropFile("core1", false, false, "dataDir=" + solrHomeDirectory + "datadir"));
    addCoreWithProps(makeCorePropFile("core2", true,  false, "dataDir=" + solrHomeDirectory + "datadir"));
    // Should just blow up here.
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException se) {
      assertEquals("Should be returning proper error code of 500", 500, se.code());
      assertTrue(se.getCause().getMessage().indexOf("More than one core points to data dir") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  void addCoreProps(SolrCore core, String... propPairs) {
    for (String keyval : propPairs) {
      String[] pair = keyval.split("=");
      core.getCoreDescriptor().putProperty(pair[0], pair[1]);
    }
  }

  // Insure that the properties in the core passed in are exactly what's in the default core.properties below plus
  // whatever extra is passed in.
  void checkCoreProps(Properties orig, String... extraProps) throws Exception {
    // Read the persisted file.
    Properties props = new Properties();
    File propParent = new File(solrHomeDirectory, orig.getProperty(CoreDescriptor.CORE_NAME));
    FileInputStream in = new FileInputStream(new File(propParent, ConfigSolr.CORE_PROP_FILE));
    try {
      props.load(in);
    } finally {
      in.close();
    }
    Set<String> propSet = props.stringPropertyNames();

    assertEquals("Persisted properties should NOT contain extra properties", propSet.size(), orig.size());

    for (String prop : orig.stringPropertyNames()) {
      assertEquals("Original and new properties should be equal for " + prop, props.getProperty(prop), orig.getProperty(prop));
    }
    for (String prop : extraProps) {
      String[] pair = prop.split("=");
      assertNull("Modified parameters should not be present for " + prop, props.getProperty(pair[0]));
    }
  }

  // For testing whether finding a solr.xml overrides looking at solr.properties
  private final static String SOLR_XML = " <solr persistent=\"${persistent:false}\"> " +
      "<cores autoDiscoverCores=\"true\" adminPath=\"/admin/cores\" defaultCoreName=\"defcore\" transientCacheSize=\"2\" " +
       " hostContext=\"solrprop\" zkClientTimeout=\"20\" host=\"222.333.444.555\" hostPort=\"6000\" />  " +
      "</solr>";
}
