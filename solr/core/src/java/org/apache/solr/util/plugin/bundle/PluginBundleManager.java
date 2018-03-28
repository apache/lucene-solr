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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.util.FileUtils;
import org.pf4j.DefaultVersionManager;
import org.pf4j.PluginException;
import org.pf4j.PluginManager;
import org.pf4j.PluginWrapper;
import org.pf4j.VersionManager;
import org.pf4j.update.PluginInfo;
import org.pf4j.update.UpdateManager;
import org.pf4j.update.UpdateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Discovers and loads plugins from plugin folder using PF4J
 */
public class PluginBundleManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String SOLR_INSTALL_DIR = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
  public static final VersionManager BUNDLE_VERSION_MANAGER = new DefaultVersionManager();

  private final SolrPf4jPluginManager pluginManager;
  private final UpdateManager updateManager;
  public static final String solrVersion = org.apache.lucene.util.Version.LATEST.toString();
  private final Path pluginsRoot;
  private ClassLoader uberLoader;

  public PluginBundleManager(Path pluginsRoot) {
    try {
      this.pluginsRoot = pluginsRoot;
      if (Files.exists(pluginsRoot)) {
        if (!Files.isDirectory(pluginsRoot)) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Existing plugins-root " + pluginsRoot + " is not a directory");
        }
      } else {
        log.info("Creating plugins root directory {}", pluginsRoot);
        FileUtils.createDirectories(pluginsRoot);
      }
      pluginManager = new SolrPf4jPluginManager(pluginsRoot);
      // NOCOMMIT: Persist plugin repo somewhere
      List<UpdateRepository> repos = new ArrayList<>();
      if (SOLR_INSTALL_DIR != null) {
        addIfAccessible(repos, UpdateRepositoryFactory.create(
            "contribs",
            Paths.get(SOLR_INSTALL_DIR).resolve("contrib").toAbsolutePath().toUri().toURL().toString()));
      }
      addIfAccessible(repos, UpdateRepositoryFactory.create(
          "community", "https://github.com/cominvent/solr-plugins"));
      //repos.add(UpdateRepositoryFactory.create("apache", "https://www.apache.org/dist/lucene/solr/" + solrVersion.toString() + "/"));
      updateManager = new UpdateManager(pluginManager, (Path) null);
      updateManager.setRepositories(repos);
      pluginManager.setSystemVersion(solrVersion);
      setUberClassLoader(new PluginBundleClassLoader(getClass().getClassLoader(), pluginManager, null));
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void addIfAccessible(List<UpdateRepository> repos, UpdateRepository updateRepository) {
    try {
      new URL(updateRepository.getUrl(), "plugins.json").openConnection();
      repos.add(updateRepository);
    } catch (IOException e) {
      log.warn("Not adding Update Repository {} at {} because it could not be accessed.", 
          updateRepository.getId(), updateRepository.getUrl());
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
    if (updateManager.getPlugins().size() == 0) {
      return Collections.emptyList();
    } else {
      List<PluginInfo> plugins = updateManager.getPlugins().stream()
          .filter(p -> p.getLastRelease(solrVersion, BUNDLE_VERSION_MANAGER) != null).collect(Collectors.toList());
      if (plugins.size() > 0 && q != null && q.length() > 0 && !q.equals("*")) {
        plugins = plugins.stream().filter(filterPredicate(q)).collect(Collectors.toList());
      }
      if (plugins.size() > 0) {
        log.debug("Found plugins for " + q + ": " + plugins.stream().map(p -> p.id +
            "(" + p.getLastRelease(solrVersion, BUNDLE_VERSION_MANAGER) + ")").collect(Collectors.toList()));
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
  
  public void addUpdateRepository(String id, URL url) throws PluginException {
    updateManager.addRepository(UpdateRepositoryFactory.create(id, url.toString()));
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
