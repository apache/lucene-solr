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

import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.zafarkhaja.semver.Version;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;

/**
 * Discovers and loads plugins from plugin folder using PF4J
 */
public class PluginBundleManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrPf4jPluginManager pluginManager;
  private final UpdateManager updateManager;
  public static final Version solrVersion = Version.valueOf(org.apache.lucene.util.Version.LATEST.toString());;
  private final Path pluginsRoot;
  private ClassLoader uberLoader;

  public PluginBundleManager(Path pluginsRoot) {
    try {
      this.pluginsRoot = pluginsRoot;
      pluginManager = new SolrPf4jPluginManager(pluginsRoot);
//      ApacheMirrorsUpdateRepository apacheRepo = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/" + systemVersion.toString() + "/");
      List<UpdateRepository> repos = new ArrayList<>();
      //repos.add(new PluginUpdateRepository("apache", new URL("http://people.apache.org/~janhoy/dist/plugins/")));
      repos.add(new PluginUpdateRepository("local", new URL("file:///Users/janhoy/Cominvent/Code/plugins/")));
      repos.add(new GitHubUpdateRepository("community","cominvent", "solr-plugins"));
      updateManager = new UpdateManager(pluginManager, (Path) null);
      updateManager.setRepositories(repos);
      pluginManager.setSystemVersion(solrVersion);
      setUberClassLoader(new PluginBundleClassLoader(getClass().getClassLoader(), pluginManager, null));
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void load() {
    pluginManager.loadPlugins();
    pluginManager.startPlugins();
    log.info("Loaded {} plugins: {}", listInstalled().size(),
        listInstalled().stream().map(PluginWrapper::getPluginId).collect(Collectors.toList()));
  }

  private static Predicate<PluginInfo> filterPredicate(String q) {
    return p -> p.id != null && p.id.contains(q);
  }

  public List<PluginInfo> query(String q) {
    // TODO: Allow install from any GitHub repo
    if (!updateManager.hasAvailablePlugins()) {
      return Collections.emptyList();
    } else {
      List<PluginInfo> plugins = updateManager.getAvailablePlugins().stream()
          .filter(p -> p.getLastRelease(solrVersion) != null).collect(Collectors.toList());
      if (plugins.size() > 0 && q != null && q.length() > 0 && !q.equals("*")) {
        plugins = plugins.stream().filter(filterPredicate(q)).collect(Collectors.toList());
      }
      if (plugins.size() > 0) {
        log.debug("Found plugins for " + q + ": " + plugins.stream().map(p -> p.id +
            "(" + p.getLastRelease(solrVersion) + ")").collect(Collectors.toList()));
        return plugins;
      }
      return Collections.emptyList();
    }
  }

  public List<PluginWrapper> listInstalled() {
    return pluginManager.getPlugins().stream().collect(Collectors.toList());
  }

  public boolean updateAll() {
    boolean successful = true;
    if (updateManager.hasUpdates()) {
      List<PluginInfo> updates = updateManager.getUpdates();
      for (PluginInfo plugin : updates) {
        try {
          updateManager.updatePlugin(plugin.id, null);
          log.info("Updatied plugin with id " + plugin.id);
        } catch (PluginException e) {
          log.warn("Failed to update plugin {}", plugin.id, e);
          successful = false;
        }
      }
    }
    return successful;
  }

  public boolean install(String id) {
    try {
      if (pluginManager.getPlugin(id) != null) {
        log.info("Plugin {} is already installed, doing nothing", id);
        return false;
      }
      boolean wasInstalled = updateManager.installPlugin(id, null);
      if (wasInstalled) {
        log.info("Installed plugin {}", id);
      }
      return wasInstalled;
    } catch (PluginException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Install of plugin " + id + " failed", e);
    }
  }

  public PluginManager getPluginManager() {
    return pluginManager;
  }

  public ClassLoader getUberClassLoader() {
    return uberLoader;
  }
  
  public void setUberClassLoader(ClassLoader loader) {
    uberLoader = loader;
  }
  
  public void addUpdateRepository(String id, URL url) {
    updateManager.addRepository(id, url);
  }

  public void removeUpdateRepository(String id) {
    UpdateRepository toRemove;
    for (UpdateRepository repo : updateManager.getRepositories()) {
      if (repo.getId().equals(id)) {
        updateManager.getRepositories().remove(repo);
        break;
      }
    }
  }

  public UpdateManager getUpdateManager() {
    return updateManager;
  }

  public boolean uninstall(String id) {
    if (getPluginManager().getPlugin(id) != null) {
      return updateManager.uninstallPlugin(id);
    } else {
      log.info("Cannot uninstall plugin {}, since it is not installed", id);
      return false;
    }     
  }

  public Path getPluginsRoot() {
    return pluginsRoot;
  }

  public boolean update(String id) {
    try {
      return updateManager.updatePlugin(id, null);
    } catch (PluginException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to update plugin with id " + id, e);
    }
  }

}
