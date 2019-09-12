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
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Pattern;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXParseException;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.matchers.JUnitMatchers.containsString;


public class TestCoreContainer extends SolrTestCaseJ4 {

  private static String oldSolrHome;
  private static final String SOLR_HOME_PROP = "solr.solr.home";

  @BeforeClass
  public static void beforeClass() throws Exception {
    oldSolrHome = System.getProperty(SOLR_HOME_PROP);
    System.setProperty("configsets", getFile("solr/configsets").getAbsolutePath());
  }

  @AfterClass
  public static void afterClass() {
    if (oldSolrHome != null) {
      System.setProperty(SOLR_HOME_PROP, oldSolrHome);
    } else {
      System.clearProperty(SOLR_HOME_PROP);
    }
  }

  private CoreContainer init(String xml) throws Exception {
    Path solrHomeDirectory = createTempDir();
    return init(solrHomeDirectory, xml);
  }

  private CoreContainer init(Path homeDirectory, String xml) throws Exception {
    SolrResourceLoader loader = new SolrResourceLoader(homeDirectory);
    CoreContainer ret = new CoreContainer(SolrXmlConfig.fromString(loader, xml));
    ret.load();
    return ret;
  }

  @Test
  public void testShareSchema() throws Exception {
    System.setProperty("shareSchema", "true");

    CoreContainer cores = init(CONFIGSETS_SOLR_XML);

    try {
      SolrCore core1 = cores.create("core1", ImmutableMap.of("configSet", "minimal"));
      SolrCore core2 = cores.create("core2", ImmutableMap.of("configSet", "minimal"));
      
      assertSame(core1.getLatestSchema(), core2.getLatestSchema());

    } finally {
      cores.shutdown();
      System.clearProperty("shareSchema");
    }
  }

  @Test
  public void testReloadSequential() throws Exception {
    final CoreContainer cc = init(CONFIGSETS_SOLR_XML);
    try {
      cc.create("core1", ImmutableMap.of("configSet", "minimal"));
      cc.reload("core1");
      cc.reload("core1");
      cc.reload("core1");
      cc.reload("core1");

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testReloadThreaded() throws Exception {
    final CoreContainer cc = init(CONFIGSETS_SOLR_XML);
    cc.create("core1", ImmutableMap.of("configSet", "minimal"));

    class TestThread extends Thread {
      @Override
      public void run() {
        cc.reload("core1");
      }
    }

    List<Thread> threads = new ArrayList<>();
    int numThreads = 4;
    for (int i = 0; i < numThreads; i++) {
      threads.add(new TestThread());
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    cc.shutdown();

  }

  @Test
  public void testNoCores() throws Exception {

    CoreContainer cores = init(CONFIGSETS_SOLR_XML);

    try {
      //assert zero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      //add a new core
      cores.create("core1", ImmutableMap.of("configSet", "minimal"));

      //assert one registered core

      assertEquals("There core registered", 1, cores.getCores().size());

      cores.unload("core1");
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      // try and remove a core that does not exist
      SolrException thrown = expectThrows(SolrException.class, () -> {
        cores.unload("non_existent_core");
      });
      assertThat(thrown.getMessage(), containsString("Cannot unload non-existent core [non_existent_core]"));


      // try and remove a null core
      thrown = expectThrows(SolrException.class, () -> {
        cores.unload(null);
      });
      assertThat(thrown.getMessage(), containsString("Cannot unload non-existent core [null]"));
    } finally {
      cores.shutdown();
    }

  }

  @Test
  public void testLogWatcherEnabledByDefault() throws Exception {
    CoreContainer cc = init("<solr></solr>");
    try {
      assertNotNull(cc.getLogging());
    }
    finally {
      cc.shutdown();
    }
  }

  @Test
  public void testDeleteBadCores() throws Exception {

    MockCoresLocator cl = new MockCoresLocator();

    SolrResourceLoader resourceLoader = new SolrResourceLoader(createTempDir());

    System.setProperty("configsets", getFile("solr/configsets").getAbsolutePath());

    final CoreContainer cc = new CoreContainer(SolrXmlConfig.fromString(resourceLoader, CONFIGSETS_SOLR_XML), new Properties(), cl);
    Path corePath = resourceLoader.getInstancePath().resolve("badcore");
    CoreDescriptor badcore = new CoreDescriptor("badcore", corePath, cc.getContainerProperties(), cc.isZooKeeperAware(),
        "configSet", "nosuchconfigset");

    cl.add(badcore);

    try {
      cc.load();
      assertThat(cc.getCoreInitFailures().size(), is(1));
      assertThat(cc.getCoreInitFailures().get("badcore").exception.getMessage(), containsString("nosuchconfigset"));
      cc.unload("badcore", true, true, true);
      assertThat(cc.getCoreInitFailures().size(), is(0));

      // can we create the core now with a good config?
      SolrCore core = cc.create("badcore", ImmutableMap.of("configSet", "minimal"));
      assertThat(core, not(nullValue()));

    }
    finally {
      cc.shutdown();
    }
  }

  @Test
  public void testClassLoaderHierarchy() throws Exception {
    final CoreContainer cc = init(CONFIGSETS_SOLR_XML);
    try {
      ClassLoader sharedLoader = cc.loader.getClassLoader();
      ClassLoader baseLoader = SolrResourceLoader.class.getClassLoader();
      assertSame(baseLoader, sharedLoader.getParent());

      SolrCore core1 = cc.create("core1", ImmutableMap.of("configSet", "minimal"));
      ClassLoader coreLoader = core1.getResourceLoader().getClassLoader();
      assertSame(sharedLoader, coreLoader.getParent());

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testSharedLib() throws Exception {
    Path tmpRoot = createTempDir("testSharedLib");

    File lib = new File(tmpRoot.toFile(), "lib");
    lib.mkdirs();

    try (JarOutputStream jar1 = new JarOutputStream(new FileOutputStream(new File(lib, "jar1.jar")))) {
      jar1.putNextEntry(new JarEntry("defaultSharedLibFile"));
      jar1.closeEntry();
    }

    File customLib = new File(tmpRoot.toFile(), "customLib");
    customLib.mkdirs();

    try (JarOutputStream jar2 = new JarOutputStream(new FileOutputStream(new File(customLib, "jar2.jar")))) {
      jar2.putNextEntry(new JarEntry("customSharedLibFile"));
      jar2.closeEntry();
    }

    final CoreContainer cc1 = init(tmpRoot, "<solr></solr>");
    try {
      cc1.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc1.shutdown();
    }

    final CoreContainer cc2 = init(tmpRoot, "<solr><str name=\"sharedLib\">lib</str></solr>");
    try {
      cc2.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc2.shutdown();
    }

    final CoreContainer cc3 = init(tmpRoot, "<solr><str name=\"sharedLib\">customLib</str></solr>");
    try {
      cc3.loader.openResource("customSharedLibFile").close();
    } finally {
      cc3.shutdown();
    }
  }

  private static final String CONFIGSETS_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr>\n" +
      "<str name=\"configSetBaseDir\">${configsets:configsets}</str>\n" +
      "<str name=\"shareSchema\">${shareSchema:false}</str>\n" +
      "</solr>";

  private static final String CUSTOM_HANDLERS_SOLR_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr>" +
      " <str name=\"collectionsHandler\">" + CustomCollectionsHandler.class.getName() + "</str>" +
      " <str name=\"infoHandler\">" + CustomInfoHandler.class.getName() + "</str>" +
      " <str name=\"adminHandler\">" + CustomCoreAdminHandler.class.getName() + "</str>" +
      " <str name=\"configSetsHandler\">" + CustomConfigSetsHandler.class.getName() + "</str>" +
      "</solr>";

  public static class CustomCollectionsHandler extends CollectionsHandler {
    public CustomCollectionsHandler(CoreContainer cc) {
      super(cc);
    }
  }

  public static class CustomInfoHandler extends InfoHandler {
    public CustomInfoHandler(CoreContainer cc) {
      super(cc);
    }
  }

  public static class CustomCoreAdminHandler extends CoreAdminHandler {
    public CustomCoreAdminHandler(CoreContainer cc) {
      super(cc);
    }
  }

  public static class CustomConfigSetsHandler extends ConfigSetsHandler {
    public CustomConfigSetsHandler(CoreContainer cc) {
      super(cc);
    }
  }

  @Test
  public void testCustomHandlers() throws Exception {

    CoreContainer cc = init(CUSTOM_HANDLERS_SOLR_XML);
    try {
      assertThat(cc.getCollectionsHandler(), is(instanceOf(CustomCollectionsHandler.class)));
      assertThat(cc.getInfoHandler(), is(instanceOf(CustomInfoHandler.class)));
      assertThat(cc.getMultiCoreHandler(), is(instanceOf(CustomCoreAdminHandler.class)));
    }
    finally {
      cc.shutdown();
    }

  }

  private static class MockCoresLocator implements CoresLocator {

    List<CoreDescriptor> cores = new ArrayList<>();

    void add(CoreDescriptor cd) {
      cores.add(cd);
    }

    @Override
    public void create(CoreContainer cc, CoreDescriptor... coreDescriptors) {
      // noop
    }

    @Override
    public void persist(CoreContainer cc, CoreDescriptor... coreDescriptors) {

    }

    @Override
    public void delete(CoreContainer cc, CoreDescriptor... coreDescriptors) {

    }

    @Override
    public void rename(CoreContainer cc, CoreDescriptor oldCD, CoreDescriptor newCD) {

    }

    @Override
    public void swap(CoreContainer cc, CoreDescriptor cd1, CoreDescriptor cd2) {

    }

    @Override
    public List<CoreDescriptor> discover(CoreContainer cc) {
      return cores;
    }

  }

  @Test
  public void testCoreInitFailuresFromEmptyContainer() throws Exception {
    // reused state
    Map<String,CoreContainer.CoreLoadFailure> failures = null;
    Collection<String> cores = null;
    Exception fail = null;

    // ----
    // init the CoreContainer
    CoreContainer cc = init(CONFIGSETS_SOLR_XML);

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 0, cores.size());

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 0, failures.size());

    // -----
    // try to add a collection with a configset that doesn't exist
    ignoreException(Pattern.quote("bogus_path"));
    SolrException thrown = expectThrows(SolrException.class, () -> {
      cc.create("bogus", ImmutableMap.of("configSet", "bogus_path"));
    });
    Throwable rootCause = Throwables.getRootCause(thrown);
    assertTrue("init exception doesn't mention bogus dir: " + rootCause.getMessage(),
        0 < rootCause.getMessage().indexOf("bogus_path"));

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 0, cores.size());

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("bogus").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getMessage(),
        0 < fail.getMessage().indexOf("bogus_path"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    thrown = expectThrows(SolrException.class, () -> {
      SolrCore c = cc.getCore("bogus");
    });
    assertEquals(500, thrown.code());
    String cause = Throwables.getRootCause(thrown).getMessage();
    assertTrue("getCore() ex cause doesn't mention init fail: " + cause,
        0 < cause.indexOf("bogus_path"));

    cc.shutdown();
  }

  @Test
  public void testCoreInitFailuresOnReload() throws Exception {

    // reused state
    Map<String,CoreContainer.CoreLoadFailure> failures = null;
    Collection<String> cores = null;
    Exception fail = null;

    // -----
    // init the  CoreContainer with the mix of ok/bad cores
    MockCoresLocator cl = new MockCoresLocator();

    SolrResourceLoader resourceLoader = new SolrResourceLoader(createTempDir());

    System.setProperty("configsets", getFile("solr/configsets").getAbsolutePath());

    final CoreContainer cc = new CoreContainer(SolrXmlConfig.fromString(resourceLoader, CONFIGSETS_SOLR_XML), new Properties(), cl);
    cl.add(new CoreDescriptor("col_ok", resourceLoader.getInstancePath().resolve("col_ok"),
        cc.getContainerProperties(), cc.isZooKeeperAware(), "configSet", "minimal"));
    cl.add(new CoreDescriptor("col_bad", resourceLoader.getInstancePath().resolve("col_bad"),
        cc.getContainerProperties(), cc.isZooKeeperAware(), "configSet", "bad-mergepolicy"));
    cc.load();

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 1, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("col_bad").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getMessage(),
        0 < fail.getMessage().indexOf("DummyMergePolicy"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    SolrException thrown = expectThrows(SolrException.class, () -> {
      SolrCore c = cc.getCore("col_bad");
    });
    assertEquals(500, thrown.code());
    String cause = thrown.getCause().getCause().getMessage();
    assertTrue("getCore() ex cause doesn't mention init fail: " + cause, 0 < cause.indexOf("DummyMergePolicy"));

    // -----
    // "fix" the bad collection
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-defaults.xml"),
        FileUtils.getFile(cc.getSolrHome(), "col_bad", "conf", "solrconfig.xml"));
    FileUtils.copyFile(getFile("solr/collection1/conf/schema-minimal.xml"),
        FileUtils.getFile(cc.getSolrHome(), "col_bad", "conf", "schema.xml"));
    cc.create("col_bad", ImmutableMap.of());

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 0, failures.size());


    // -----
    // try to add a collection with a path that doesn't exist
    ignoreException(Pattern.quote("bogus_path"));
    thrown = expectThrows(SolrException.class, () -> {
      cc.create("bogus", ImmutableMap.of("configSet", "bogus_path"));
    });
    assertTrue("init exception doesn't mention bogus dir: " + thrown.getCause().getCause().getMessage(),
        0 < thrown.getCause().getCause().getMessage().indexOf("bogus_path"));

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());
    fail = failures.get("bogus").exception;
    assertNotNull("null failure for test core", fail);
    assertTrue("init failure doesn't mention problem: " + fail.getMessage(),
        0 < fail.getMessage().indexOf("bogus_path"));

    // check that we get null accessing a non-existent core
    assertNull(cc.getCore("does_not_exist"));
    // check that we get a 500 accessing the core with an init failure
    thrown = expectThrows(SolrException.class, () -> {
      SolrCore c = cc.getCore("bogus");
    });
    assertEquals(500, thrown.code());
    cause = thrown.getCause().getMessage();
    assertTrue("getCore() ex cause doesn't mention init fail: " + cause,
        0 < cause.indexOf("bogus_path"));

    // -----
    // break col_bad's config and try to RELOAD to add failure

    final long col_bad_old_start = getCoreStartTime(cc, "col_bad");

    FileUtils.write
        (FileUtils.getFile(cc.getSolrHome(), "col_bad", "conf", "solrconfig.xml"),
            "This is giberish, not valid XML <",
            IOUtils.UTF_8);

    ignoreException(Pattern.quote("SAX"));
    thrown = expectThrows(SolrException.class,
        "corrupt solrconfig.xml failed to trigger exception from reload",
        () -> { cc.reload("col_bad"); });
    Throwable rootException = getWrappedException(thrown);
    assertTrue("We're supposed to have a wrapped SAXParserException here, but we don't",
        rootException instanceof SAXParseException);
    SAXParseException se = (SAXParseException) rootException;
    assertTrue("reload exception doesn't refer to slrconfig.xml " + se.getSystemId(),
        0 < se.getSystemId().indexOf("solrconfig.xml"));

    assertEquals("Failed core reload should not have changed start time",
        col_bad_old_start, getCoreStartTime(cc, "col_bad"));

    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 2, failures.size());
    Throwable ex = getWrappedException(failures.get("col_bad").exception);
    assertNotNull("null failure for test core", ex);
    assertTrue("init failure isn't SAXParseException",
        ex instanceof SAXParseException);
    SAXParseException saxEx = (SAXParseException) ex;
    assertTrue("init failure doesn't mention problem: " + saxEx.toString(), saxEx.getSystemId().contains("solrconfig.xml"));

    // ----
    // fix col_bad's config (again) and RELOAD to fix failure
    FileUtils.copyFile(getFile("solr/collection1/conf/solrconfig-defaults.xml"),
        FileUtils.getFile(cc.getSolrHome(), "col_bad", "conf", "solrconfig.xml"));
    cc.reload("col_bad");

    assertTrue("Core reload should have changed start time",
        col_bad_old_start < getCoreStartTime(cc, "col_bad"));


    // check that we have the cores we expect
    cores = cc.getLoadedCoreNames();
    assertNotNull("core names is null", cores);
    assertEquals("wrong number of cores", 2, cores.size());
    assertTrue("col_ok not found", cores.contains("col_ok"));
    assertTrue("col_bad not found", cores.contains("col_bad"));

    // check that we have the failures we expect
    failures = cc.getCoreInitFailures();
    assertNotNull("core failures is a null map", failures);
    assertEquals("wrong number of core failures", 1, failures.size());

    cc.shutdown();

  }

  private long getCoreStartTime(final CoreContainer cc, final String name) {
    try (SolrCore tmp = cc.getCore(name)) {
      return tmp.getStartTimeStamp().getTime();
    }
  }
}
