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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.zafarkhaja.semver.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;

import static ro.fortsoft.pf4j.update.UpdateRepository.PluginInfo;
import static ro.fortsoft.pf4j.update.UpdateRepository.PluginRelease;

/**
 * Discovers and loads plugins from plugin folder using PF4J
 */
public class Plugins {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrPluginManager pluginManager;
  private final PluginUpdateManager updateManager;
  private final Version systemVersion;


  public Plugins() {
    pluginManager = new SolrPluginManager();
    updateManager = new PluginUpdateManager(pluginManager);
    systemVersion = pluginManager.getSystemVersion();
  }

  public void load() {
    pluginManager.loadPlugins();
    pluginManager.startPlugins();
  }

  private static Predicate<UpdateRepository.PluginInfo> filterPredicate(String q) {
    return p -> (q == null || q.length() == 0 || q.equals("*")) || p.id != null && p.id.contains(q);
  }

  public List<UpdateRepository.PluginInfo> query(String q) {
    // TODO: Allow install from any GitHub repo
    if (updateManager.hasAvailablePlugins()) {
      List<UpdateRepository.PluginInfo> plugins = updateManager.getAvailablePlugins()
          .stream().filter(filterPredicate(q)).collect(Collectors.toList());
      log.debug("Found these plugins in repos for query " + q + ": " + plugins);
      log.info("Found plugins for " + q + ": " + plugins.stream().map(p -> p.id +
          "(" + p.getLastRelease(systemVersion).version + ")").collect(Collectors.toList()));
      return plugins;
    } else {
      return Collections.emptyList();
    }
  }

  public List<String> listInstalled() {
    List<String> plugins = pluginManager.getPlugins().stream().map(p -> p.getDescriptor().toString()).collect(Collectors.toList());
    log.info("Currently installed plugins: " + plugins);
    return plugins;
  }

  public void updateAll() {
    if (updateManager.hasUpdates()) {
      List<UpdateRepository.PluginInfo> updates = updateManager.getUpdates();
      for (PluginInfo plugin : updates) {
        PluginRelease lastRelease = plugin.getLastRelease(systemVersion);
        String lastVersion = lastRelease.version;
        String installedVersion = pluginManager.getPlugin(plugin.id).getDescriptor().getVersion().toString();
        // TODO: Inspect whether we can use the plugin or not
        log.info("Updating plugin with id " + plugin.id);
        updateManager.updatePlugin(plugin.id, lastRelease.url);
    }
    }
  }

  public void install(String id) {
    log.info("Installing plugin with id "+id);
    PluginInfo info = query(id).get(0);
    updateManager.installPlugin(info.getLastRelease(systemVersion).url);
  }

  public PluginManager getPluginManager() {
    return pluginManager;
  }

  public ClassLoader getUberClassLoader(ClassLoader parent) {
    return pluginManager.getUberClassloader(parent);
  }
  
  public void addUpdateRepository(String id, String url) {
    updateManager.addRepository(id, url);
  }

  public UpdateManager getUpdateManager() {
    return updateManager;
  }

  public void uninstall(String id) {
    updateManager.uninstallPlugin(id);
  }
}
