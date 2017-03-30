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

package org.apache.solr.util.pf4j;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;
import ro.fortsoft.pf4j.update.UpdateRepository;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the PF4J integration
 */
public class PluginsTest {

  private Plugins plugins;

  @Before
  public void before() {
    plugins = new Plugins();
    plugins.addUpdateRepository("folder", "file:/Users/janhoy/solr-repo/");
    plugins.listInstalled().forEach(id -> {
      plugins.uninstall(id);
    });
  }

  @Test
  public void query() throws Exception {
    // NOCOMMIT: Get rid of GSON dependency
    List<UpdateRepository.PluginInfo> res = plugins.query("*");
    assertEquals(4, res.size());
    assertTrue(res.stream().map(p -> p.id).collect(Collectors.toList()).contains("extraction"));

    assertEquals(1, plugins.query("extract").size());

    assertEquals(0, plugins.query("fooxyz").size());
  }

  @Test
  public void listInstalled() {
    assertEquals(0, plugins.listInstalled().size());
  }

  @Test
  public void load() {
    plugins.load();
  }

  @Test
  public void install() throws Exception {
    assertEquals(0, plugins.listInstalled().size());
    plugins.install("dih");
    assertEquals(1, plugins.listInstalled().size());
  }

  @Test
  public void installAndCheckClassloading() throws Exception {
    plugins.install("dih");
    assertEquals(1, plugins.listInstalled().size());
    try {
      this.getClass().getClassLoader().loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
      fail();
    } catch (Exception e) {}
    ClassLoader loader = plugins.getPluginManager().getPluginClassLoader("dih");
    assertEquals("DataImportHandler",
        loader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler").getSimpleName());
    plugins.install("request-sanitizer");
    ClassLoader uberLoader = plugins.getUberClassLoader(getClass().getClassLoader());
    uberLoader.loadClass("com.cominvent.solr.RequestSanitizerComponent");
    uberLoader.loadClass("org.apache.solr.handler.dataimport.DataImportHandler");
  }

}