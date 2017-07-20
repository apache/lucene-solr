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

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrResourceLoader;
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
  private static final URL TEST_REPO = Thread.currentThread().getContextClassLoader().getResource("plugins-repository/");
  
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    System.setProperty("solr.plugins.active", "true");
    System.setProperty("solr.plugins.dir", testFolder.getRoot().getAbsolutePath());
    // Initialize plugin folder with one small plugin jar
    // Start with our controlled update repo
    pluginBundleManager = new PluginBundleManager(testFolder.getRoot().toPath());
    pluginBundleManager.getUpdateManager().setRepositories(new ArrayList<>());     
    pluginBundleManager.addUpdateRepository("testrepo", TEST_REPO);
  }

  @Test
  public void query() throws Exception {
    // NOCOMMIT: Get rid of GSON dependency?
    List<PluginInfo> res = pluginBundleManager.query("*");
    assertTrue(res.size()>0);
    assertEquals("testrepo", res.get(0).getRepositoryId());
  }

  @Test
  public void load() {
    pluginBundleManager.load();
  }

  @Test
  public void install() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    pluginBundleManager.install("request-sanitizer");
    assertEquals(1, pluginBundleManager.listInstalled().size());
  }

  @Test
  public void uninstall() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    assertFalse(pluginBundleManager.uninstall("nonexistent"));
    assertTrue(pluginBundleManager.uninstall("request-sanitizer"));
    assertEquals(0, pluginBundleManager.listInstalled().size());
  }

  @Test
  public void installTwice() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    assertFalse(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
  }

  @Test(expected = SolrException.class)
  public void installNonExisting() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.install("nonexistent"));
  }

  @Test
  public void update() throws Exception {
    // TODO: Update plugins
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertFalse(pluginBundleManager.update("request-sanitizer"));
    pluginBundleManager.updateAll();
  }

  @Test(expected = SolrException.class)
  public void updateNonInstalled() throws Exception {
    assertEquals(0, pluginBundleManager.listInstalled().size());
    assertTrue(pluginBundleManager.update("request-sanitizer"));
  }

  @Test
  public void jarPlugin() throws Exception {
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
  }

  @Test
  public void installAndCheckClassloading() throws Exception {
    assertTrue(pluginBundleManager.install("request-sanitizer"));
    assertEquals(1, pluginBundleManager.listInstalled().size());
    try {
      this.getClass().getClassLoader().loadClass("com.cominvent.solr.RequestSanitizerComponent");
      fail();
    } catch (Exception ignored) {}

    // PluginLoader
    ClassLoader pluginClassLoader = pluginBundleManager.getPluginManager().getPluginClassLoader("request-sanitizer");
    assertEquals("RequestSanitizerComponent", pluginClassLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent").getSimpleName());

    // Load another class through uber loader
    ClassLoader uberClassLoader = pluginBundleManager.getUberClassLoader();
    uberClassLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent");
    uberClassLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent$DefaultSolrParams");
  }

  @Test
  public void testResourceLoading() throws Exception {
    // Test that classes and resources are loaded through SolrResourceLoader
    initCore("solrconfig.xml","schema.xml");
    // Use coreContainer's instance of pluginBunldeManager, not the one created in init
    PluginBundleManager pluginBundleManagerFromSolr = h.getCoreContainer().getPluginBundleManager();
    pluginBundleManagerFromSolr.getUpdateManager().setRepositories(new ArrayList<>());
    pluginBundleManagerFromSolr.addUpdateRepository("testrepo", TEST_REPO);
    
    assertTrue(pluginBundleManagerFromSolr.install("plugin-with-resources"));
    assertTrue(pluginBundleManagerFromSolr.install("request-sanitizer"));
    assertEquals(2, pluginBundleManagerFromSolr.listInstalled().size());
    SolrResourceLoader l = h.getCoreContainer().getResourceLoader();
    assertNotNull(l.findClass("com.cominvent.solr.RequestSanitizerComponent", SearchComponent.class));

    // Now get a resource from the test plugin 
    assertNotNull(l.openResource("file.txt"));
  }
}