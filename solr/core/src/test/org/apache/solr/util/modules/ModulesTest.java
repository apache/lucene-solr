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

package org.apache.solr.util.modules;

import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import ro.fortsoft.pf4j.update.PluginInfo;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the PF4J integration
 */
public class ModulesTest {

  private Modules modules;

  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  @Before
  public void before() {
    modules = new Modules(testFolder.getRoot().toPath());
    modules.addUpdateRepository("folder", "file:/Users/janhoy/solr-repo/");
    modules.listInstalled().forEach(info -> modules.uninstall(info.getPluginId()));
  }

  @Test
  public void query() throws Exception {
    // NOCOMMIT: Get rid of GSON dependency
    List<PluginInfo> res = modules.query("*");
    assertTrue(res.size()>0);
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
    modules.load();
  }

  @Test
  public void install() throws Exception {
    assertEquals(0, modules.listInstalled().size());
    modules.install("dih");
    assertEquals(1, modules.listInstalled().size());
  }

  @Test
  public void uninstall() throws Exception {
    assertEquals(0, modules.listInstalled().size());
    assertTrue(modules.install("dih"));
    assertEquals(1, modules.listInstalled().size());
    assertFalse(modules.uninstall("nonexistent"));
    assertTrue(modules.uninstall("dih"));
    assertEquals(0, modules.listInstalled().size());
  }

  @Test
  public void update() throws Exception {
    // TODO: Update modules
    modules.updateAll();
  }

  @Test
  public void installAndCheckClassloading() throws Exception {
    assertTrue(modules.install("dih"));
    assertEquals(1, modules.listInstalled().size());
    try {
      this.getClass().getClassLoader().loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
      fail();
    } catch (Exception ignored) {}
    ClassLoader loader = modules.getPluginManager().getPluginClassLoader("dih");
    assertEquals("DataImportHandler",
        loader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler").getSimpleName());
    modules.install("request-sanitizer");
    ClassLoader uberLoader = modules.getUberClassLoader(getClass().getClassLoader());
    uberLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent");
    uberLoader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
  }

}