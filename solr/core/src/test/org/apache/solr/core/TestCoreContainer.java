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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util._TestUtil;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

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

    CoreContainer ret = new CoreContainer(solrHomeDirectory.getAbsolutePath());
    ret.load(solrHomeDirectory.getAbsolutePath(), new File(solrHomeDirectory, "solr.xml"));
    return ret;
  }

  @Test
  public void testShareSchema() throws Exception {
    System.setProperty("shareSchema", "true");
    final CoreContainer cores = init("_shareSchema");
    try {
      cores.setPersistent(false);
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
  public void testPersist() throws Exception {
    final File workDir = new File(TEMP_DIR, this.getClass().getName()
        + "_persist");
    if (workDir.exists()) {
      FileUtils.deleteDirectory(workDir);
    }
    assertTrue("Failed to mkdirs workDir", workDir.mkdirs());
    
    final CoreContainer cores = h.getCoreContainer();

    cores.setPersistent(true); // is this needed since we make explicit calls?

    String instDir = null;
    {
      SolrCore template = null;
      try {
        template = cores.getCore("collection1");
        instDir = template.getCoreDescriptor().getInstanceDir();
      } finally {
        if (null != template) template.close();
      }
    }
    
    final File instDirFile = new File(instDir);
    assertTrue("instDir doesn't exist: " + instDir, instDirFile.exists());
    
    // sanity check the basic persistence of the default init
    
    final File oneXml = new File(workDir, "1.solr.xml");
    cores.persistFile(oneXml);

    assertXmlFile(oneXml, "/solr[@persistent='true']",
        "/solr/cores[@defaultCoreName='collection1' and not(@transientCacheSize)]",
        "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir +
        "' and @transient='false' and @loadOnStartup='true' ]", "1=count(/solr/cores/core)");

    // create some new cores and sanity check the persistence
    
    final File dataXfile = new File(workDir, "dataX");
    final String dataX = dataXfile.getAbsolutePath();
    assertTrue("dataXfile mkdirs failed: " + dataX, dataXfile.mkdirs());
    
    final File instYfile = new File(workDir, "instY");
    FileUtils.copyDirectory(instDirFile, instYfile);
    
    // :HACK: dataDir leaves off trailing "/", but instanceDir uses it
    final String instY = instYfile.getAbsolutePath() + "/";
    
    final CoreDescriptor xd = new CoreDescriptor(cores, "X", instDir);
    xd.setDataDir(dataX);
    
    final CoreDescriptor yd = new CoreDescriptor(cores, "Y", instY);
    
    SolrCore x = null;
    SolrCore y = null;
    try {
      x = cores.create(xd);
      y = cores.create(yd);
      cores.register(x, false);
      cores.register(y, false);
      
      assertEquals("cores not added?", 3, cores.getCoreNames().size());
      
      final File twoXml = new File(workDir, "2.solr.xml");

      cores.persistFile(twoXml);

      assertXmlFile(twoXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir
              + "']", "/solr/cores/core[@name='X' and @instanceDir='" + instDir
              + "' and @dataDir='" + dataX + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "3=count(/solr/cores/core)");

      // Test for saving implicit properties, we should not do this.
      assertXmlFile(twoXml, "/solr/cores/core[@name='X' and not(@solr.core.instanceDir) and not (@solr.core.configName)]");

      // delete a core, check persistence again
      assertNotNull("removing X returned null", cores.remove("X"));
      
      final File threeXml = new File(workDir, "3.solr.xml");
      cores.persistFile(threeXml);
      
      assertXmlFile(threeXml, "/solr[@persistent='true']",
          "/solr/cores[@defaultCoreName='collection1']",
          "/solr/cores/core[@name='collection1' and @instanceDir='" + instDir + "']",
          "/solr/cores/core[@name='Y' and @instanceDir='" + instY + "']",
          "2=count(/solr/cores/core)");
      
      // sanity check that persisting w/o changes has no changes
      
      final File fourXml = new File(workDir, "4.solr.xml");
      cores.persistFile(fourXml);
      
      assertTrue("3 and 4 should be identical files",
          FileUtils.contentEquals(threeXml, fourXml));
      
    } finally {
      // y is closed by the container, but
      // x has been removed from the container
      if (x != null) {
        try {
          x.close();
        } catch (Exception e) {
          log.error("", e);
        }
      }
    }
  }
  

  @Test
  public void testNoCores() throws IOException, ParserConfigurationException, SAXException {
    //create solrHome
    File solrHomeDirectory = new File(TEMP_DIR, this.getClass().getName()
        + "_noCores");
    SetUpHome(solrHomeDirectory, EMPTY_SOLR_XML);
    CoreContainer.Initializer init = new CoreContainer.Initializer();
    CoreContainer cores = null;
    try {
      cores = init.initialize();
    }
    catch(Exception e) {
      fail("CoreContainer not created" + e.getMessage());
    }
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


      assertXmlFile(new File(solrHomeDirectory, "solr.xml"),
          "/solr/cores[@transientCacheSize='32']");

      newCore.close();
      cores.remove("core1");
      //assert cero cores
      assertEquals("There should not be cores", 0, cores.getCores().size());
    } finally {
      cores.shutdown();
      FileUtils.deleteDirectory(solrHomeDirectory);
    }

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
      cc.setPersistent(false);
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

    final CoreContainer cc1 = new CoreContainer(tmpRoot.getAbsolutePath());
    cc1.load(tmpRoot.getAbsolutePath(), new File(tmpRoot, "default-lib-solr.xml"));
    try {
      cc1.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc1.shutdown();
    }

    final CoreContainer cc2 = new CoreContainer(tmpRoot.getAbsolutePath());
    cc2.load(tmpRoot.getAbsolutePath(), new File(tmpRoot, "explicit-lib-solr.xml"));
    try {
      cc2.loader.openResource("defaultSharedLibFile").close();
    } finally {
      cc2.shutdown();
    }

    final CoreContainer cc3 = new CoreContainer(tmpRoot.getAbsolutePath());
    cc3.load(tmpRoot.getAbsolutePath(), new File(tmpRoot, "custom-lib-solr.xml"));
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

  private static final String SOLR_XML_SAME_NAME ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\" transientCacheSize=\"32\" >\n" +
      "    <core name=\"core1\" instanceDir=\"core1\" dataDir=\"core1\"/> \n" +
      "    <core name=\"core1\" instanceDir=\"core2\" dataDir=\"core2\"/> \n " +
      "  </cores>\n" +
      "</solr>";

  private static final String SOLR_XML_SAME_DATADIR ="<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
      "<solr persistent=\"false\">\n" +
      "  <cores adminPath=\"/admin/cores\" transientCacheSize=\"32\" >\n" +
      "    <core name=\"core2\" instanceDir=\"core2\" dataDir=\"../samedatadir\" schema=\"schema-tiny.xml\" config=\"solrconfig-minimal.xml\" /> \n" +
      "    <core name=\"core1\" instanceDir=\"core2\" dataDir=\"../samedatadir\" schema=\"schema-tiny.xml\" config=\"solrconfig-minimal.xml\"  /> \n " +
      "  </cores>\n" +
      "</solr>";


}
