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
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.internal.matchers.StringContains.containsString;

public class TestCoreDiscovery extends SolrTestCaseJ4 {

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore();
  }

  private final File solrHomeDirectory = createTempDir().toFile();

  private void setMeUp(String alternateCoreDir) throws Exception {
    System.setProperty("solr.solr.home", solrHomeDirectory.getAbsolutePath());
    String xmlStr = SOLR_XML;
    if (alternateCoreDir != null) {
      xmlStr = xmlStr.replace("<solr>", "<solr> <str name=\"coreRootDirectory\">" + alternateCoreDir + "</str> ");
    }
    File tmpFile = new File(solrHomeDirectory, SolrXmlConfig.SOLR_XML_FILE);
    FileUtils.write(tmpFile, xmlStr, IOUtils.UTF_8);

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

    File propFile = new File(new File(solrHomeDirectory, name), CorePropertiesLocator.PROPERTIES_FILENAME);
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
    final CoreContainer cores = new CoreContainer();
    try {
      cores.load();
    } catch (Exception e) {
      cores.shutdown();
      throw e;
    }
    return cores;
  }

  @After
  public void after() throws Exception {

  }

  // Test the basic setup, create some dirs with core.properties files in them, but solr.xml has discoverCores
  // set and insure that we find all the cores and can load them.
  @Test
  public void testDiscovery() throws Exception {
    setMeUp();

    // name, isLazy, loadOnStartup
    addCoreWithProps("core1", makeCorePropFile("core1", false, true, "dataDir=core1"));
    addCoreWithProps("core2", makeCorePropFile("core2", false, false, "dataDir=core2"));

    // I suspect what we're adding in here is a "configset" rather than a schema or solrconfig.
    //
    addCoreWithProps("lazy1", makeCorePropFile("lazy1", true, false, "dataDir=lazy1"));

    CoreContainer cc = init();
    try {

      TestLazyCores.checkInCores(cc, "core1");
      TestLazyCores.checkNotInCores(cc, "lazy1", "core2", "collection1");

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

        TestLazyCores.checkInCores(cc, "core1", "core2", "lazy1");
      }

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
    addCoreWithProps("coreLOS", makeCorePropFile("coreLOS", false, true, "dataDir=coreLOS"));
    addCoreWithProps("coreT1", makeCorePropFile("coreT1", true, true, "dataDir=coreT1"));
    addCoreWithProps("coreT2", makeCorePropFile("coreT2", true, true, "dataDir=coreT2"));
    addCoreWithProps("coreT3", makeCorePropFile("coreT3", true, true, "dataDir=coreT3"));
    addCoreWithProps("coreT4", makeCorePropFile("coreT4", true, true, "dataDir=coreT4"));
    addCoreWithProps("coreT5", makeCorePropFile("coreT5", true, true, "dataDir=coreT5"));
    addCoreWithProps("coreT6", makeCorePropFile("coreT6", true, true, "dataDir=coreT6"));

    // Do this specially since we need to search.
    final CoreContainer cc = new CoreContainer(solrHomeDirectory.getPath().toString());
    try {
      cc.load();
      // Just check that the proper number of cores are loaded since making the test depend on order would be fragile
      assertEquals("There should only be 3 cores loaded, coreLOS and two coreT? cores",
          3, cc.getCoreNames().size());

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
    addCoreWithProps("core1", makeCorePropFile("core1", false, true));
    addCoreWithProps("core2", makeCorePropFile("core2", false, false, "name=core1"));
    CoreContainer cc = null;
    try {
      cc = init();
      fail("Should have thrown exception in testDuplicateNames");
    } catch (SolrException se) {
      String message = se.getMessage();
      assertTrue("Wrong exception thrown on duplicate core names",
          message.indexOf("Found multiple cores with the name [core1]") != -1);
      assertTrue(File.separator + "core1 should have been mentioned in the message: " + message,
          message.indexOf(File.separator + "core1") != -1);
      assertTrue(File.separator + "core2 should have been mentioned in the message:" + message,
          message.indexOf(File.separator + "core2") != -1);
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
  }


  @Test
  public void testAlternateCoreDir() throws Exception {

    File alt = createTempDir().toFile();

    setMeUp(alt.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true, "dataDir=core1"),
        new File(alt, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"),
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
    addCoreWithProps(makeCorePropFile("core1", false, true, "dataDir=core1"),
        solrHomeDirectory.toPath().resolve(relative).resolve("core1").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());
    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"),
        solrHomeDirectory.toPath().resolve(relative).resolve("core2").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());
    // one core *not* under the relative directory
    addCoreWithProps(makeCorePropFile("core0", false, true, "datadir=core0"),
        solrHomeDirectory.toPath().resolve("core0").resolve(CorePropertiesLocator.PROPERTIES_FILENAME).toFile());

    CoreContainer cc = init();
    try (SolrCore core1 = cc.getCore("core1");
         SolrCore core2 = cc.getCore("core2")) {
      assertNotNull(core1);
      assertNotNull(core2);

      assertNull(cc.getCore("core0"));

      SolrCore core3 = cc.create("core3", ImmutableMap.of("configSet", "minimal"));
      assertThat(core3.getCoreDescriptor().getInstanceDir().toAbsolutePath().toString(), containsString("relative"));

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testNoCoreDir() throws Exception {
    File noCoreDir = createTempDir().toFile();
    setMeUp(noCoreDir.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true),
        new File(noCoreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));
    addCoreWithProps(makeCorePropFile("core2", false, false),
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
    File coreDir = solrHomeDirectory;
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    // Insure that another core is opened successfully
    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"),
        new File(coreDir, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(coreDir, "core1");
    assumeTrue("Cannot make " + toSet + " non-readable. Test aborted.", toSet.setReadable(false, false));
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
    File coreDir = solrHomeDirectory;
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    addCoreWithProps(makeCorePropFile("core2", false, false, "dataDir=core2"),
        new File(coreDir, "core2" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(solrHomeDirectory, "cantReadDir");
    assertTrue("Should have been able to make directory '" + toSet.getAbsolutePath() + "' ", toSet.mkdirs());
    assumeTrue("Cannot make " + toSet + " non-readable. Test aborted.", toSet.setReadable(false, false));
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
    File coreDir = solrHomeDirectory;
    setMeUp(coreDir.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true),
        new File(coreDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    File toSet = new File(solrHomeDirectory, "cantReadFile");
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
    File homeDir = solrHomeDirectory;
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
    File homeDir = solrHomeDirectory;
    setMeUp(homeDir.getAbsolutePath());
    addCoreWithProps(makeCorePropFile("core1", false, true),
        new File(homeDir, "core1" + File.separator + CorePropertiesLocator.PROPERTIES_FILENAME));

    assumeTrue("Cannot make " + homeDir + " non-readable. Test aborted.", homeDir.setReadable(false, false));

    CoreContainer cc = null;
    try {
      cc = init();
      fail("Should have thrown an exception here");
    } catch (Exception ex) {
      assertThat(ex.getMessage(), containsString("Error reading core root directory"));
    } finally {
      if (cc != null) {
        cc.shutdown();
      }
    }
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

    SolrResourceLoader loader = new SolrResourceLoader(solrHomeDirectory.toPath());

    NodeConfig config = SolrXmlConfig.fromString(loader, "<solr><str name=\"coreRootDirectory\">relative</str></solr>");
    assertThat(config.getCoreRootDirectory().toString(), containsString(solrHomeDirectory.getAbsolutePath()));

    NodeConfig absConfig = SolrXmlConfig.fromString(loader, "<solr><str name=\"coreRootDirectory\">/absolute</str></solr>");
    assertThat(absConfig.getCoreRootDirectory().toString(), not(containsString(solrHomeDirectory.getAbsolutePath())));
  }
}
