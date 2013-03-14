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
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.StringReader;
import java.util.Properties;
import java.util.Set;

public class TestSolrDiscoveryProperties extends SolrTestCaseJ4 {
  private static String NEW_LINE = System.getProperty("line.separator");

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final File solrHomeDirectory = new File(TEMP_DIR, "org.apache.solr.core.TestSolrDiscoveryProperties" + File.separator + "solrHome");

  private void setMeUp() throws Exception {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
  }

  private void addSolrPropertiesFile(String... extras) throws Exception {
    File solrProps = new File(solrHomeDirectory, SolrProperties.SOLR_PROPERTIES_FILE);
    Properties props = new Properties();
    props.load(new StringReader(SOLR_PROPERTIES));
    for (String extra : extras) {
      String[] parts = extra.split("=");
      props.put(parts[0], parts[1]);
    }
    FileOutputStream out = new FileOutputStream(solrProps.getAbsolutePath());
    try {
      props.store(out, null);
    } finally {
      out.close();
    }
  }

  private void addSolrXml() throws Exception {
    File tmpFile = new File(solrHomeDirectory, SolrProperties.SOLR_XML_FILE);
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
        stockProps.getProperty(CoreDescriptor.CORE_NAME) + File.separator + SolrProperties.CORE_PROP_FILE);
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

  // Test the basic setup, create some dirs with core.properties files in them, but no solr.xml (a solr.properties
  // instead) and insure that we find all the cores and can load them.
  @Test
  public void testPropertiesFile() throws Exception {
    setMeUp();
    addSolrPropertiesFile();
    // name, isLazy, loadOnStartup
    addCoreWithProps(makeCorePropFile("core1", false, true));
    addCoreWithProps(makeCorePropFile("core2", false, false));

    // I suspect what we're adding in here is a "configset" rather than a schema or solrconfig.
    //
    addCoreWithProps(makeCorePropFile("lazy1", true, false));

    CoreContainer cc = init();
    try {
      Properties props = cc.containerProperties;

      assertEquals("/admin/cores/props", props.getProperty("cores.adminPath"));
      assertEquals("/admin/cores/props", cc.getAdminPath());
      assertEquals("defcore", props.getProperty("cores.defaultCoreName"));
      assertEquals("defcore", cc.getDefaultCoreName());
      assertEquals("222.333.444.555", props.getProperty("host"));
      assertEquals("6000", props.getProperty("port")); // getProperty actually looks at original props.
      assertEquals("/solrprop", props.getProperty("cores.hostContext"));
      assertEquals("20", props.getProperty("cores.zkClientTimeout"));

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
    addSolrPropertiesFile();
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

      checkSolrProperties(cc);

      File xmlFile = new File(solrHomeDirectory, "solr.xml");
      assertFalse("Solr.xml should NOT exist", xmlFile.exists());

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
    addSolrPropertiesFile();

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
      checkSolrProperties(cc);

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

  void addCoreProps(SolrCore core, String... propPairs) {
    for (String keyval : propPairs) {
      String[] pair = keyval.split("=");
      core.getCoreDescriptor().putProperty(pair[0], pair[1]);
    }
  }

  // Insure that the solr.properties is as it should be after persisting _and_, in some cases, different than
  // what's in memory
  void checkSolrProperties(CoreContainer cc, String... checkMemPairs) throws Exception {
    Properties orig = new Properties();
    orig.load(new StringReader(SOLR_PROPERTIES));

    Properties curr = cc.getContainerProperties();

    Properties persisted = new Properties();
    FileInputStream in = new FileInputStream(new File(solrHomeDirectory, SolrProperties.SOLR_PROPERTIES_FILE));
    try {
      persisted.load(in);
    } finally {
      in.close();
    }

    assertEquals("Persisted and original should be the same size", orig.size(), persisted.size());

    for (String prop : orig.stringPropertyNames()) {
      assertEquals("Values of original should match current", orig.getProperty(prop), persisted.getProperty(prop));
    }

    Properties specialProps = new Properties();
    for (String special : checkMemPairs) {
      String[] pair = special.split("=");
      specialProps.put(pair[0], pair[1]);
    }
    // OK, current should match original except if the property is "special"
    for (String prop : curr.stringPropertyNames()) {
      String val = specialProps.getProperty(prop);
      if (val != null) { // Compare curr and val
        assertEquals("Modified property should be in current container properties", val, curr.getProperty(prop));
      }
    }
  }

  // Insure that the properties in the core passed in are exactly what's in the default core.properties below plus
  // whatever extra is passed in.
  void checkCoreProps(Properties orig, String... extraProps) throws Exception {
    // Read the persisted file.
    Properties props = new Properties();
    File propParent = new File(solrHomeDirectory, orig.getProperty(CoreDescriptor.CORE_NAME));
    FileInputStream in = new FileInputStream(new File(propParent, SolrProperties.CORE_PROP_FILE));
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

  // If there's a solr.xml AND a properties file, make sure that the xml file is loaded and the properties file
  // is ignored.

  @Test
  public void testBackCompatXml() throws Exception {
    setMeUp();
    addSolrPropertiesFile();
    addSolrXml();
    addConfigsForBackCompat();

    CoreContainer cc = init();
    try {
      Properties props = cc.getContainerProperties();

      assertEquals("/admin/cores", cc.getAdminPath());
      assertEquals("collectionLazy2", cc.getDefaultCoreName());

      // Shouldn't get these in properties at this point
      assertNull(props.getProperty("cores.adminPath"));
      assertNull(props.getProperty("cores.defaultCoreName"));
      assertNull(props.getProperty("host"));
      assertNull(props.getProperty("port")); // getProperty actually looks at original props.
      assertNull(props.getProperty("cores.hostContext"));
      assertNull(props.getProperty("cores.zkClientTimeout"));

      SolrCore core1 = cc.getCore("collection1");
      CoreDescriptor desc = core1.getCoreDescriptor();

      assertEquals("collection1", desc.getProperty("solr.core.name"));

      // This is too long and ugly to put in. Besides, it varies.
      assertNotNull(desc.getProperty("solr.core.instanceDir"));

      assertEquals("data/", desc.getProperty("solr.core.dataDir"));
      assertEquals("solrconfig-minimal.xml", desc.getProperty("solr.core.configName"));
      assertEquals("schema-tiny.xml", desc.getProperty("solr.core.schemaName"));
      core1.close();
    } finally {
      cc.shutdown();
    }
  }

  // For this test I want some of these to be different than what would be in solr.xml by default.
  private final static String SOLR_PROPERTIES =
      "persistent=${persistent:false}" + NEW_LINE +
          "cores.adminPath=/admin/cores/props" + NEW_LINE +
          "cores.defaultCoreName=defcore" + NEW_LINE +
          "host=222.333.444.555" + NEW_LINE +
          "port=6000" + NEW_LINE +
          "cores.hostContext=/solrprop" + NEW_LINE +
          "cores.zkClientTimeout=20" + NEW_LINE +
          "cores.transientCacheSize=2";

  // For testing whether finding a solr.xml overrides looking at solr.properties
  private final static String SOLR_XML = " <solr persistent=\"false\"> " +
      "<cores adminPath=\"/admin/cores\" defaultCoreName=\"collectionLazy2\" transientCacheSize=\"4\">  " +
      "<core name=\"collection1\" instanceDir=\"collection1\" config=\"solrconfig-minimal.xml\" schema=\"schema-tiny.xml\" /> " +
      "</cores> " +
      "</solr>";
}
