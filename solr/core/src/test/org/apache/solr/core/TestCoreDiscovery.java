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
package org.apache.solr.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.solr.core.CoreContainer.CORE_DISCOVERY_COMPLETE;
import static org.apache.solr.core.CoreContainer.INITIAL_CORE_LOAD_COMPLETE;
import static org.apache.solr.core.CoreContainer.LOAD_COMPLETE;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.core.StringContains.containsString;

public class TestCoreDiscovery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final Path solrHomeDirectory = createTempDir();

  private void setMeUp(String alternateCoreDir) throws Exception {
    String xmlStr = SOLR_XML;
    if (alternateCoreDir != null) {
      xmlStr = xmlStr.replace("<solr>", "<solr> <str name=\"coreRootDirectory\">" + alternateCoreDir + "</str> ");
    }
    File tmpFile = new File(solrHomeDirectory.toFile(), SolrXmlConfig.SOLR_XML_FILE);
    FileUtils.write(tmpFile, xmlStr, IOUtils.UTF_8);

  }

  private void setMeUp() throws Exception {
    setMeUp(null);
  }

  private Properties makeCoreProperties(String name, boolean isTransient, boolean loadOnStartup, String... extraProps) {
    Properties props = new Properties();
    props.put(CoreDescriptor.CORE_NAME, name);
    props.put(CoreDescriptor.CORE_SCHEMA, "schema-tiny.xml");
    props.put(CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml");
    props.put(CoreDescriptor.CORE_TRANSIENT, Boolean.toString(isTransient));
    props.put(CoreDescriptor.CORE_LOADONSTARTUP, Boolean.toString(loadOnStartup));
    props.put(CoreDescriptor.CORE_DATADIR, "${core.dataDir:stuffandnonsense}");

    for (String extra : extraProps) {
      String[] parts = extra.split("=");
      props.put(parts[0], parts[1]);
    }

    return props;
  }

  private void addCoreWithProps(Properties stockProps, File propFile) throws Exception {
    if (!propFile.getParentFile().exists()) propFile.getParentFile().mkdirs();
    Writer out = new OutputStreamWriter(new FileOutputStream(propFile), StandardCharsets.UTF_8);
    try {
      stockProps.store(out, null);
    } finally {
      out.close();
    }
    addConfFiles(new File(propFile.getParent(), "conf"));
  }

  private void addCoreWithProps(String name, Properties stockProps) throws Exception {

    File propFile = new File(new File(solrHomeDirectory.toFile(), name), CorePropertiesLocator.PROPERTIES_FILENAME);
    File parent = propFile.getParentFile();
    assertTrue("Failed to mkdirs for " + parent.getAbsolutePath(), parent.mkdirs());
    addCoreWithProps(stockProps, propFile);
  }

  private void addConfFiles(File confDir) throws Exception {
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    assertTrue("Failed to mkdirs for " + confDir.getAbsolutePath(), confDir.mkdirs());
    FileUtils.copyFile(new File(top, "schema-tiny.xml"), new File(confDir, "schema-tiny.xml"));
    FileUtils.copyFile(new File(top, "solrconfig-minimal.xml"), new File(confDir, "solrconfig-minimal.xml"));
    FileUtils.copyFile(new File(top, "solrconfig.snippet.randomindexconfig.xml"), new File(confDir, "solrconfig.snippet.randomindexconfig.xml"));
  }

  private CoreContainer init() throws Exception {
    final CoreContainer container = new CoreContainer(solrHomeDirectory, new Properties());
    try {
      container.load();
    } catch (Exception e) {
      container.shutdown();
      throw e;
    }

    long status = container.getStatus();

    assertTrue("Load complete flag should be set", 
        (status & LOAD_COMPLETE) == LOAD_COMPLETE);
    assertTrue("Core discovery should be complete", 
        (status & CORE_DISCOVERY_COMPLETE) == CORE_DISCOVERY_COMPLETE);
    assertTrue("Initial core loading should be complete", 
        (status & INITIAL_CORE_LOAD_COMPLETE) == INITIAL_CORE_LOAD_COMPLETE);
    return container;
  }

  @After
  public void after() throws Exception {

  }

  // Test the basic setup, create some dirs with core.properties files in them, but solr.xml has discoverCores
  // set and insure that we find all the cores and can load them.
  @Test
  @SuppressWarnings({"try"})
  public void testDiscovery() throws Exception {
    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps("core1", makeCoreProperties("core1", false, true, "dataDir=core1"));
    addCoreWithProps("core2", makeCoreProperties("core2", false, false, "dataDir=core2"));

    // I suspect what we're adding in here is a "configset" rather than a schema or solrconfig.
    //
    addCoreWithProps("lazy1", makeCoreProperties("lazy1", true, false, "dataDir=lazy1"));

    CoreContainer cc = init();
    try {

      TestLazyCores.checkLoadedCores(cc, "core1");
      TestLazyCores.checkCoresNotLoaded(cc, "lazy1", "core2");

      // force loading of core2 and lazy1 by getting them from the CoreContainer
      try (SolrCore core1 = cc.getCore("core1");
           SolrCore core2 = cc.getCore("core2");
           SolrCore lazy1 = cc.getCore("lazy1")) {

        // Let's assert we did the right thing for implicit properties too.
        CoreDescriptor desc = core1.getCoreDescriptor();
        assertEquals("core1", desc.getName());

        // This is too long and ugly to put in. Besides, it varies.
        assertNotNull(desc.getInstanceDir());

        assertEquals("core1", desc.getDataDir());
        assertEquals("solrconfig-minimal.xml", desc.getConfigName());
        assertEquals("schema-tiny.xml", desc.getSchemaName());

        TestLazyCores.checkLoadedCores(cc, "core1", "core2", "lazy1");
        // Can we persist an existing core's properties?

        // Insure we can persist a new properties file if we want.
        CoreDescriptor cd1 = core1.getCoreDescriptor();
        Properties persistable = cd1.getPersistableUserProperties();
        persistable.setProperty("bogusprop", "bogusval");
        cc.getCoresLocator().persist(cc, cd1);
        File propFile = new File(new File(solrHomeDirectory.toFile(), "core1"), CorePropertiesLocator.PROPERTIES_FILENAME);
        Properties newProps = new Properties();
        try (InputStreamReader is = new InputStreamReader(new FileInputStream(propFile), StandardCharsets.UTF_8)) {
          newProps.load(is);
        }
        // is it there?
        assertEquals("Should have persisted bogusprop to disk", "bogusval", newProps.getProperty("bogusprop"));
        // is it in the user properties?
        CorePropertiesLocator cpl = new CorePropertiesLocator(solrHomeDirectory);
        List<CoreDescriptor> cores = cpl.discover(cc);
        boolean found = false;
        for (CoreDescriptor cd : cores) {
          if (cd.getName().equals("core1")) {
            found = true;
            assertEquals("Should have persisted bogusprop to disk in user properties",
                "bogusval",
                cd.getPersistableUserProperties().getProperty("bogusprop"));
            break;
          }
        }
        assertTrue("Should have found core descriptor for core1", found);
      }

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testPropFilePersistence() throws Exception {
    setMeUp();

    // Test that an existing core.properties file is _not_ deleted if the core fails to load.
    Properties badProps = makeCoreProperties("corep1", false, true);
    badProps.setProperty(CoreDescriptor.CORE_SCHEMA, "not-there.xml");

    addCoreWithProps("corep1", badProps);
    // Sanity check that a core did get loaded
    addCoreWithProps("corep2", makeCoreProperties("corep2", false, true));

    Path coreP1PropFile = Paths.get(solrHomeDirectory.toString(), "corep1", "core.properties");
    assertTrue("Core.properties file should exist for before core load failure core corep1",
        Files.exists(coreP1PropFile));

    CoreContainer cc = init();
    try {
      Exception thrown = expectThrows(SolrCoreInitializationException.class, () -> cc.getCore("corep1"));
      assertTrue(thrown.getMessage().contains("init failure"));
      try (SolrCore sc = cc.getCore("corep2")) {
        assertNotNull("Core corep2 should be loaded", sc);
      }
      assertTrue("Core.properties file should still exist for core corep1", Files.exists(coreP1PropFile));

      // Creating a core successfully should create a core.properties file
      Path corePropFile = Paths.get(solrHomeDirectory.toString(), "corep3", "core.properties");
      assertFalse("Should not be a properties file yet", Files.exists(corePropFile));
      cc.create("corep3", ImmutableMap.of("configSet", "minimal"));
      assertTrue("Should be a properties file for newly created core", Files.exists(corePropFile));

      // Failing to create a core should _not_ leave a core.properties file hanging around.
      corePropFile = Paths.get(solrHomeDirectory.toString(), "corep4", "core.properties");
      assertFalse("Should not be a properties file yet for corep4", Files.exists(corePropFile));

      thrown = expectThrows(SolrException.class, () -> {
        cc.create("corep4", ImmutableMap.of(
            CoreDescriptor.CORE_NAME, "corep4",
            CoreDescriptor.CORE_SCHEMA, "not-there.xml",
            CoreDescriptor.CORE_CONFIG, "solrconfig-minimal.xml",
            CoreDescriptor.CORE_TRANSIENT, "false",
            CoreDescriptor.CORE_LOADONSTARTUP, "true"));
      });
      assertTrue(thrown.getMessage().contains("Can't find resource"));
      assertFalse("Failed corep4 should not have left a core.properties file around", Files.exists(corePropFile));

      // Finally, just for yucks, let's determine that a this create path also leaves a prop file.

      corePropFile = Paths.get(solrHomeDirectory.toString(), "corep5", "core.properties");
      assertFalse("Should not be a properties file yet for corep5", Files.exists(corePropFile));

      cc.create("corep5", ImmutableMap.of("configSet", "minimal"));
      
      assertTrue("corep5 should have left a core.properties file on disk", Files.exists(corePropFile));

    } finally {
      cc.shutdown();
    }
  }


  // Insure that if the number of transient cores that are loaded on startup is greater than the cache size that Solr
  // "does the right thing". Which means
  // 1> stop loading cores after transient cache size is reached, in this case that magic number is 3
  //    one non-transient and two transient.
  // 2> still loads cores as time passes.
  //
  // This seems like a silly test, but it hangs forever on 4.10 so let's guard against it in future. The behavior
  // has gone away with the removal of the complexity around the old-style solr.xml files.
  //
  // NOTE: The order that cores are loaded depends upon how the core discovery is traversed. I don't think we can
  //       make the test depend on that order, so after load just insure that the cores counts are correct.

  @Test
  public void testTooManyTransientCores() throws Exception {

    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps("coreLOS", makeCoreProperties("coreLOS", false, true, "dataDir=coreLOS"));
    addCoreWithProps("coreT1", makeCoreProperties("coreT1", true, true, "dataDir=coreT1"));
    addCoreWithProps("coreT2", makeCoreProperties("coreT2", true, true, "dataDir=coreT2"));
    addCoreWithProps("coreT3", makeCoreProperties("coreT3", true, true, "dataDir=coreT3"));
    addCoreWithProps("coreT4", makeCoreProperties("coreT4", true, true, "dataDir=coreT4"));
    addCoreWithProps("coreT5", makeCoreProperties("coreT5", true, true, "dataDir=coreT5"));
    addCoreWithProps("coreT6", makeCoreProperties("coreT6", true, true, "dataDir=coreT6"));

    // Do this specially since we need to search.
    final CoreContainer cc = new CoreContainer(solrHomeDirectory, new Properties());
    try {
      cc.load();
      // Just check that the proper number of cores are loaded since making the test depend on order would be fragile
      assertEquals("There should only be 3 cores loaded, coreLOS and two coreT? cores",
          3, cc.getLoadedCoreNames().size());

      SolrCore c1 = cc.getCore("coreT1");
      assertNotNull("Core T1 should NOT BE NULL", c1);
      SolrCore c2 = cc.getCore("coreT2");
      assertNotNull("Core T2 should NOT BE NULL", c2);
      SolrCore c3 = cc.getCore("coreT3");
      assertNotNull("Core T3 should NOT BE NULL", c3);
      SolrCore c4 = cc.getCore("coreT4");
      assertNotNull("Core T4 should NOT BE NULL", c4);
      SolrCore c5 = cc.getCore("coreT5");
      assertNotNull("Core T5 should NOT BE NULL", c5);
      SolrCore c6 = cc.getCore("coreT6");
      assertNotNull("Core T6 should NOT BE NULL", c6);

      c1.close();
      c2.close();
      c3.close();
      c4.close();
      c5.close();
      c6.close();
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }

  @Test
  public void testDuplicateNames() throws Exception {
    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps("core1", makeCoreProperties("core1", false, true));
    addCoreWithProps("core2", makeCoreProperties("core2", false, false, "name=core1"));
    SolrException thrown = expectThrows(SolrException.class, () -> {
      CoreContainer cc = null;
      try { cc = init(); }
      finally { if (cc != null) cc.shutdown(); }
    });
    final String message = thrown.getMessage();
    assertTrue("Wrong exception thrown on duplicate core names",
        message.indexOf("Found multiple cores with the name [core1]") != -1);
    assertTrue(File.separator + "core1 should have been mentioned in the message: " + message,
        message.indexOf(File.separator + "core1") != -1);
    assertTrue(File.separator + "core2 should have been mentioned in the message:" + message,
        message.indexOf(File.separator + "core2") != -1);
  }


  @Test
  public void testAlternateCoreDir() throws Exception {

    File alt = createTempDir().toFile();

    setMeUp(alt.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true, "dataDir=core1"),
        new File(alt, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(makeCoreProperties("core2", false, false, "dataDir=core2"),
        new File(alt, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testAlternateRelativeCoreDir() throws Exception {

    String relative = "relativeCoreDir";

    setMeUp(relative);
    // two cores under the relative directory
    addCoreWithProps(makeCoreProperties("core1", false, true, "dataDir=core1"),
        solrHomeDirectory.resolve(relative).resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());
    addCoreWithProps(makeCoreProperties("core2", false, false, "dataDir=core2"),
        solrHomeDirectory.resolve(relative).resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());
    // one core *not* under the relative directory
    addCoreWithProps(makeCoreProperties("core0", false, true, "datadir=core0"),
        solrHomeDirectory.resolve("core0").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());

    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);

      assertNull(cc.getCore("core0"));

      SolrCore core3 = cc.create("core3", ImmutableMap.of("configSet", "minimal"));
      assertThat(core3.getCoreDescriptor().getInstanceDir().toString(), containsString("relative"));

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testNoCoreDir() throws Exception {
    File noCoreDir = createTempDir().toFile();
    setMeUp(noCoreDir.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true),
        new File(noCoreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(makeCoreProperties("core2", false, false),
        new File(noCoreDir, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCoreDirCantRead() throws Exception {
    File coreDir = solrHomeDirectory.toFile();
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    // Insure that another core is opened successfully
    addCoreWithProps(makeCoreProperties("core2", false, false, "dataDir=core2"),
        new File(coreDir, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(coreDir, "core1");
    assumeTrue("Cannot make " + toSet + " non-readable. Test aborted.", toSet.setReadable(false, false));
    assumeFalse("Appears we are a super user, skip test", toSet.canRead());
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNull(core1);
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    toSet.setReadable(true, false);
  }

  @Test
  public void testNonCoreDirCantRead() throws Exception {
    File coreDir = solrHomeDirectory.toFile();
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    addCoreWithProps(makeCoreProperties("core2", false, false, "dataDir=core2"),
        new File(coreDir, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(solrHomeDirectory.toFile(), "cantReadDir");
    assertTrue("Should have been able to make directory '" + toSet.getAbsolutePath() + "' ", toSet.mkdirs());
    assumeTrue("Cannot make " + toSet + " non-readable. Test aborted.", toSet.setReadable(false, false));
    assumeFalse("Appears we are a super user, skip test", toSet.canRead());
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1); // Should be able to open the perfectly valid core1 despite a non-readable directory
      assertNotNull(core2);
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    toSet.setReadable(true, false);

  }

  @Test
  public void testFileCantRead() throws Exception {
    File coreDir = solrHomeDirectory.toFile();
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(solrHomeDirectory.toFile(), "cantReadFile");
    assertTrue("Should have been able to make file '" + toSet.getAbsolutePath() + "' ", toSet.createNewFile());
    assumeTrue("Cannot make " + toSet + " non-readable. Test aborted.", toSet.setReadable(false, false));
    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1")) {
      assertNotNull(core1); // Should still be able to create core despite r/o file.
    } finally {
      cc.shutdown();
    }
    // So things can be cleaned up by the framework!
    toSet.setReadable(true, false);
  }

  @Test
  public void testSolrHomeDoesntExist() throws Exception {
    File homeDir = solrHomeDirectory.toFile();
    IOUtils.rm(homeDir.toPath());
    CoreContainer cc = null;
    try {
      cc = init();
    } catch (SolrException ex) {
      assertTrue("Core init doesn't report if solr home directory doesn't exist " + ex.getMessage(),
          0 <= ex.getMessage().indexOf("solr.xml does not exist"));
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }


  @Test
  public void testSolrHomeNotReadable() throws Exception {
    File homeDir = solrHomeDirectory.toFile();
    setMeUp(homeDir.getAbsolutePath());
    addCoreWithProps(makeCoreProperties("core1", false, true),
        new File(homeDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    assumeTrue("Cannot make " + homeDir + " non-readable. Test aborted.", homeDir.setReadable(false, false));
    assumeFalse("Appears we are a super user, skip test", homeDir.canRead());
    Exception thrown = expectThrows(Exception.class, () -> {
      CoreContainer cc = null;
      try { cc = init(); }
      finally { if (cc != null) cc.shutdown(); }
    });
    assertThat(thrown.getMessage(), containsString("Error reading core root directory"));
    // So things can be cleaned up by the framework!
    homeDir.setReadable(true, false);

  }

  // For testing whether finding a solr.xml overrides looking at solr.properties
  private final static String SOLR_XML = "<solr> " +
      "<int name=\"transientCacheSize\">2</int> " +
      "<str name=\"configSetBaseDir\">" + Paths.get(TEST_HOME()).resolve("configsets").toString() + "</str>" +
      "<solrcloud> " +
      "<str name=\"hostContext\">solrprop</str> " +
      "<int name=\"zkClientTimeout\">20</int> " +
      "<str name=\"host\">222.333.444.555</str> " +
      "<int name=\"hostPort\">6000</int>  " +
      "</solrcloud> " +
      "</solr>";

  @Test
  public void testRootDirectoryResolution() {
    NodeConfig config = SolrXmlConfig.fromString(solrHomeDirectory, "<solr><str name=\"coreRootDirectory\">relative</str></solr>");
    assertThat(config.getCoreRootDirectory().toString(), containsString(solrHomeDirectory.toAbsolutePath().toString()));

    NodeConfig absConfig = SolrXmlConfig.fromString(solrHomeDirectory, "<solr><str name=\"coreRootDirectory\">/absolute</str></solr>");
    assertThat(absConfig.getCoreRootDirectory().toString(), not(containsString(solrHomeDirectory.toAbsolutePath().toString())));
  }
  
}
