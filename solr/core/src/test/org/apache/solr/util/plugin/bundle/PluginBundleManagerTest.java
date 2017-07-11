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

package org.apache.solr.util.plugin.bundle;

import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.SearchComponent;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ro.fortsoft.pf4j.update.PluginInfo;

/**
 * Test the PF4J integration
 */
public class PluginBundleManagerTest extends SolrTestCaseJ4 {

  private PluginBundleManager pluginBundleManager;

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    System.clearProperty("solr.plugins.dir");
    pluginBundleManager = new PluginBundleManager(testFolder.getRoot().toPath());
  }

  @Test
  public void query() throws Exception {
    // NOCOMMIT: Get rid of GSON dependency?
    List<PluginInfo> res = pluginBundleManager.query("*");
    assertTrue(res.size()>0);
    assertNotNull(res.get(0).getRepositoryId());
//    assertEquals(4, res.size());
//    assertTrue(res.stream().map(p -> p.id).collect(Collectors.toList()).contains("extraction"));
//
//    assertEquals(1, modules.query("extract").size());
//
//    assertEquals(0, modules.query("fooxyz").size());
//
//    assertEquals(4, modules.query("").size());
//    assertEquals(4, modules.query(null).size());
  }

  @Test
  public void load() {
    pluginBundleManager.load();
  }

  @Test
  public void install() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    pluginBundleManager.install("dataimport");
    assertEquals(1, pluginBundleManager.listInstalled().size());
  }

  @Test
  public void uninstall() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("dataimport"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    assertFalse(pluginBundleManager.uninstall("nonexistent"));
    assertTrue(pluginBundleManager.uninstall("dataimport"));
    assertEquals(0, pluginBundleManager.listInstalled().size());
  }

  @Test(expected = SolrException.class)
  public void installTwice() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("dataimport"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    pluginBundleManager.install("dataimport");
  }

  @Test(expected = SolrException.class)
  public void installNonExisting() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("nonexistent"));
  }

  @Test
  public void update() throws Exception {
    // TODO: Update plugins
    assertTrue(pluginBundleManager.install("dataimport"));
    assertFalse(pluginBundleManager.update("dataimport"));
    pluginBundleManager.updateAll();
  }

  @Test(expected = SolrException.class)
  public void updateNonInstalled() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.update("dataimport"));
  }

  @Test
  public void jarPlugin() throws Exception {
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
  }

  @Test
  public void installAndCheckClassloading() throws Exception {
    assertTrue(pluginBundleManager.install("dataimport"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    try {
      this.getClass().getClassLoader().loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
      fail();
    } catch (Exception ignored) {}

    // PluginLoader
    ClassLoader pluginClassLoader = pluginBundleManager.getPluginManager().getPluginClassLoader("dataimport");
    assertEquals("DataImportHandler", pluginClassLoader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler").getSimpleName());

    // Load another class through uber loader
    pluginBundleManager.install("request-sanitizer");
    ClassLoader uberClassLoader = pluginBundleManager.getUberClassLoader(getClass().getClassLoader());
    uberClassLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent");
    uberClassLoader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
  }

  @Test
  public void testResourceLoading() throws Exception {
    // Spin up a Solr core with custom solr.plugins.dir (so it can be writeable)
    // Test that classes and resources are loaded through SolrResourceLoader
    System.setProperty("solr.plugins.dir", testFolder.getRoot().getAbsolutePath());
    initCore("solrconfig.xml","schema.xml");
    // Use coreContainer's instance of pluginBunldeManager, not the one created in init
    PluginBundleManager pluginBundleManagerFromSolr = h.getCoreContainer().getPluginBundleManager();
    
    assertTrue(pluginBundleManagerFromSolr.install("extraction"));
    assertTrue(pluginBundleManagerFromSolr.install("request-sanitizer"));
    assertEquals(2, pluginBundleManagerFromSolr.listInstalled().size());
    SolrResourceLoader l = h.getCoreContainer().getResourceLoader();
    assertNotNull(l.findClass("org.apache.solr.handler.extraction.ExtractingRequestHandler", RequestHandlerBase.class));
    assertNotNull(l.findClass("com.cominvent.solr.RequestSanitizerComponent", SearchComponent.class));

    // Now get a resource from extraction dependency jar tika-core-1.13.jar 
    assertNotNull(l.openResource("org/apache/tika/mime/tika-mimetypes.xml"));
  }
}