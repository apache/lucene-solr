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

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.lucene.util.Constants;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TestCoreContainer extends SolrTestCaseJ4 {

  private static String oldSolrHome;
  private static final String SOLR_HOME_PROP = "solr.solr.home";

  @BeforeClass
  public static void beforeClass() throws Exception {
    oldSolrHome = System.getProperty(SOLR_HOME_PROP);
    initCore("solrconfig.xml", "schema.xml");
  }

  @AfterClass
  public static void afterClass() {
    if (oldSolrHome != null) {
      System.setProperty(SOLR_HOME_PROP, oldSolrHome);
    } else {
      System.clearProperty(SOLR_HOME_PROP);
    }
  }

  private File solrHomeDirectory;

  private CoreContainer init(String dirName) throws Exception {

    solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName() + dirName);

    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());

    FileUtils.copyDirectory(new File(SolrTestCaseJ4.TEST_HOME()), solrHomeDirectory);
    System.out.println("Using solrconfig from " + new File(SolrTestCaseJ4.TEST_HOME()).getAbsolutePath());

    CoreContainer ret = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    ret.load();
    return ret;
  }

  @Test
  public void testShareSchema() throws Exception {
    System.setProperty("shareSchema", "true");
    final CoreContainer cores = init("_shareSchema");
    try {
      assertTrue(cores.isShareSchema());
      
      CoreDescriptor descriptor1 = new CoreDescriptor(cores, "core1", "./collection1");
      SolrCore core1 = cores.create(descriptor1);
      
      CoreDescriptor descriptor2 = new CoreDescriptor(cores, "core2", "./collection1");
      SolrCore core2 = cores.create(descriptor2);
      
      assertSame(core1.getLatestSchema(), core2.getLatestSchema());
      
      core1.close();
      core2.close();
    } finally {
      cores.shutdown();
      System.clearProperty("shareSchema");
    }
  }

  @Test
  public void testReloadSequential() throws Exception {
    final CoreContainer cc = init("_reloadSequential");
    try {
      cc.reload("collection1");
      cc.reload("collection1");
      cc.reload("collection1");
      cc.reload("collection1");

    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testReloadThreaded() throws Exception {
    final CoreContainer cc = init("_reloadThreaded");

      class TestThread extends Thread {
        @Override
        public void run() {
          cc.reload("collection1");
        }
      }

      List<Thread> threads = new ArrayList<Thread>();
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
  public void testNoCores() throws IOException, ParserConfigurationException, SAXException {
    //create solrHome
    File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName()
        + "_noCores");
    
    boolean oldSolrXml = random().nextBoolean();
    
    SetUpHome(solrHomeDirectory, oldSolrXml ? EMPTY_SOLR_XML : EMPTY_SOLR_XML2);
    CoreContainer cores = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    cores.load();
    try {
      //assert zero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      FileUtils.copyDirectory(new File(SolrTestCaseJ4.TEST_HOME(), "collection1"), solrHomeDirectory);
      //add a new core
      CoreDescriptor coreDescriptor = new CoreDescriptor(cores, "core1", solrHomeDirectory.getAbsolutePath());
      SolrCore newCore = cores.create(coreDescriptor);
      cores.register(newCore, false);
      
      //assert one registered core

      assertEquals("There core registered", 1, cores.getCores().size());

      if (oldSolrXml) {
        assertXmlFile(new File(solrHomeDirectory, "solr.xml"),
            "/solr/cores[@transientCacheSize='32']");
      }

      newCore.close();
      cores.remove("core1");
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
      
      // try and remove a core that does not exist
      SolrCore ret = cores.remove("non_existent_core");
      assertNull(ret);
    } finally {
      cores.shutdown();
      FileUtils.deleteDirectory(solrHomeDirectory);
    }

  }

  @Test
  public void testLogWatcherEnabledByDefault() {
    assertNotNull(h.getCoreContainer().getLogging());
  }
  
  private void SetUpHome(File solrHomeDirectory, String xmlFile) throws IOException {
    if (solrHomeDirectory.exists()) {
      FileUtils.deleteDirectory(solrHomeDirectory);
    }
    assertTrue("Failed to mkdirs workDir", solrHomeDirectory.mkdirs());
    try {
      File solrXmlFile = new File(solrHomeDirectory, "solr.xml");
      BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(solrXmlFile), IOUtils.CHARSET_UTF_8));
      out.write(xmlFile);
      out.close();
    } catch (IOException e) {
      FileUtils.deleteDirectory(solrHomeDirectory);
      throw e;
    }

    //init
    System.setProperty(SOLR_HOME_PROP, solrHomeDirectory.getAbsolutePath());
  }

  @Test
  public void testClassLoaderHierarchy() throws Exception {
    final CoreContainer cc = init("_classLoaderHierarchy");
    try {
      ClassLoader sharedLoader = cc.loader.getClassLoader();
      ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
      assertSame(contextLoader, sharedLoader.getParent());

      CoreDescriptor descriptor1 = new CoreDescriptor(cc, "core1", "./collection1");
      SolrCore core1 = cc.create(descriptor1);
      ClassLoader coreLoader = core1.getResourceLoader().getClassLoader();
      assertSame(sharedLoader, coreLoader.getParent());

      core1.close();
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testSharedLib() throws Exception {
    assumeTrue("needs URLClassLoader.close() support", Constants.WINDOWS == false || Constants.JRE_IS_MINIMUM_JAVA7);
    File tmpRoot = _TestUtil.getTempDir("testSharedLib");

    File lib = new File(tmpRoot, "lib");
    lib.mkdirs();

    JarOutputStream jar1 = new JarOutputStream(new FileOutputStream(new File(lib, "jar1.jar")));
    jar1.putNextEntry(new JarEntry("defaultSharedLibFile"));
    jar1.closeEntry();
    jar1.close();

    File customLib = new File(tmpRoot, "customLib");
    customLib.mkdirs();

    JarOutputStream jar2 = new JarOutputStream(new FileOutputStream(new File(customLib, "jar2.jar")));
    jar2.putNextEntry(new JarEntry("customSharedLibFile"));
    jar2.closeEntry();
    jar2.close();

    FileUtils.writeStringToFile(new File(tmpRoot, "default-lib-solr.xml"), "<solr><cores/></solr>", "UTF-8");
    FileUtils.writeStringToFile(new File(tmpRoot, "explicit-lib-solr.xml"), "<solr sharedLib=\"lib\"><cores/></solr>", "UTF-8");
    FileUtils.writeStringToFile(new File(tmpRoot, "custom-lib-solr.xml"), "<solr sharedLib=\"customLib\"><cores/></solr>", "UTF-8");

    final CoreContainer cc1 = CoreContainer.createAndLoad(tmpRoot.getAbsolutePath(), new File(tmpRoot, "default-lib-solr.xml"));
    try {
      cc1.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc1.shutdown();
    }

    final CoreContainer cc2 = CoreContainer.createAndLoad(tmpRoot.getAbsolutePath(), new File(tmpRoot, "explicit-lib-solr.xml"));
    try {
      cc2.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc2.shutdown();
    }

    final CoreContainer cc3 = CoreContainer.createAndLoad(tmpRoot.getAbsolutePath(), new File(tmpRoot, "custom-lib-solr.xml"));
    try {
      cc3.loader.openResource("customSharedLibFile").close();
    } finally {
      cc3.shutdown();
    }
  }
  
  private static final String EMPTY_SOLR_XML ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\" transientCacheSize=\"32\" >\n" +
      "  </cores>\n" +
      "</solr>";
  
  private static final String EMPTY_SOLR_XML2 ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr>\n" +
      "</solr>";

  private static final String CUSTOM_HANDLERS_SOLR_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr>" +
      " <str name=\"collectionsHandler\">" + CustomCollectionsHandler.class.getName() + "</str>" +
      " <str name=\"infoHandler\">" + CustomInfoHandler.class.getName() + "</str>" +
      " <str name=\"adminHandler\">" + CustomCoreAdminHandler.class.getName() + "</str>" +
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

  @Test
  public void testCustomHandlers() throws Exception {

    SolrResourceLoader loader = new SolrResourceLoader("solr/collection1");
    ConfigSolr config = ConfigSolr.fromString(loader, CUSTOM_HANDLERS_SOLR_XML);

    CoreContainer cc = new CoreContainer(loader, config);
    try {
      cc.load();
      assertThat(cc.getCollectionsHandler(), is(instanceOf(CustomCollectionsHandler.class)));
      assertThat(cc.getInfoHandler(), is(instanceOf(CustomInfoHandler.class)));
      assertThat(cc.getMultiCoreHandler(), is(instanceOf(CustomCoreAdminHandler.class)));
    }
    finally {
      cc.shutdown();
    }

  }
}
