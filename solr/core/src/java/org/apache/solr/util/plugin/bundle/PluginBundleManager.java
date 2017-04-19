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
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.github.zafarkhaja.semver.Version;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.DefaultPluginFactory;
import ro.fortsoft.pf4j.DefaultPluginLoader;
import ro.fortsoft.pf4j.DefaultPluginManager;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginClassLoader;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginDescriptorFinder;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.PluginFactory;
import ro.fortsoft.pf4j.PluginLoader;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.PropertiesPluginDescriptorFinder;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.PluginInfo.PluginRelease;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;
import ro.fortsoft.pf4j.util.StringUtils;

/**
 * Discovers and loads plugins from plugin folder using PF4J
 */
public class PluginBundleManager {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final SolrPluginManager pluginManager;
  private final UpdateManager updateManager;
  private final Version systemVersion;
  private final Path pluginsRoot;
  private ClassLoader uberLoader;

  public PluginBundleManager(Path pluginsRoot) {
    try {
      this.pluginsRoot = pluginsRoot;
      pluginManager = new SolrPluginManager(pluginsRoot);
      systemVersion = Version.valueOf(org.apache.lucene.util.Version.LATEST.toString());
      ApacheMirrorsUpdateRepository apacheRepo = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/" + systemVersion.toString() + "/");
      List<UpdateRepository> repos = new ArrayList<>();
      repos.add(new PluginUpdateRepository("janhoy", new URL("http://people.apache.org/~janhoy/dist/plugins/")));
      repos.add(new GitHubUpdateRepository("github","cominvent", "solr-plugins"));
      updateManager = new UpdateManager(pluginManager, repos);
      pluginManager.setSystemVersion(systemVersion);
    } catch (MalformedURLException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  public void load() {
    pluginManager.loadPlugins();
    pluginManager.startPlugins();
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
          .filter(p -> p.getLastRelease(systemVersion) != null).collect(Collectors.toList());
      if (plugins.size() > 0 && q != null && q.length() > 0 && !q.equals("*")) {
        plugins = plugins.stream().filter(filterPredicate(q)).collect(Collectors.toList());
      }
      if (plugins.size() > 0) {
        log.debug("Found plugins for " + q + ": " + plugins.stream().map(p -> p.id +
            "(" + p.getLastRelease(systemVersion) + ")").collect(Collectors.toList()));
        return plugins;
      }
      return Collections.emptyList();
    }
  }

  public List<PluginWrapper> listInstalled() {
    return pluginManager.getPlugins().stream().collect(Collectors.toList());
  }

  public void updateAll() {
    if (updateManager.hasUpdates()) {
      List<PluginInfo> updates = updateManager.getUpdates();
      for (PluginInfo plugin : updates) {
        PluginRelease lastRelease = plugin.getLastRelease(systemVersion);
        String lastVersion = lastRelease.version;
        String installedVersion = pluginManager.getPlugin(plugin.id).getDescriptor().getVersion().toString();
        // TODO: Inspect whether we can use the plugin or not
        log.info("Updating plugin with id " + plugin.id);
        try {
          updateManager.updatePlugin(plugin.id, new URL(lastRelease.url));
        } catch (MalformedURLException e) {
          log.warn("Failed updating plugin {}", plugin.id, e);
        }
      }
    }
  }

  public boolean install(String id) {
    Optional<PluginInfo> info = updateManager.getPlugins().stream()
        .filter(p -> id.equals(p.id) && p.getLastRelease(systemVersion) != null).findFirst();
    if (info.isPresent()) {
      String version = info.get().getLastRelease(systemVersion).version;
      log.debug("Installing plugin id {} version @{}", id, version);
      return updateManager.installPlugin(id, version);
    } else {
      log.debug("Failed to find plugin with id {}", id);
      return false;
    }
  }

  public PluginManager getPluginManager() {
    return pluginManager;
  }

  public ClassLoader getUberClassLoader(ClassLoader parent) {
    if (uberLoader == null) {
      uberLoader = new PluginBundleClassLoader(parent, pluginManager, null);
    }
    return uberLoader;
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

  /**
   * A class loader that loads classes from all plugins in manager
   */
  public static class PluginBundleClassLoader extends URLClassLoader {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private final SolrPluginManager manager;

    public PluginBundleClassLoader(ClassLoader parent, SolrPluginManager manager, URL[] urls) {
      super(urls == null ? new URL[] {} : urls, parent);
      this.manager = manager;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      // First try Solr URLs
      Class<?> clazz = super.findClass(name);
      for (ClassLoader loader : manager.getPluginClassLoaders().values()) {
        try {
          return loader.loadClass(name);
        } catch (ClassNotFoundException e) {}
      }

      throw new ClassNotFoundException("Class " + name + " not found in any plugin. Tried: " + manager.getPluginClassLoaders().keySet());
    }

    @Override
    public URL findResource(String name) {
      for (ClassLoader loader : manager.getPluginClassLoaders().values()) {
        URL url = loader.getResource(name);
        if (url != null) {
          return url;
        }
      }

      return null;
    }

    @Override
    public Enumeration<URL> findResources(String name) throws IOException {
      List<URL> resources = new ArrayList<URL>();
      for (ClassLoader loader : manager.getPluginClassLoaders().values()) {
        resources.addAll(Collections.list(loader.getResources(name)));
      }

      return Collections.enumeration(resources);
    }

  }

  /**
   * Plugin manager for Solr that changes how to read manifest mm
   */
  public static class SolrPluginManager extends DefaultPluginManager {
    private static final Logger log = LoggerFactory.getLogger(SolrPluginManager.class);

    public SolrPluginManager(Path pluginsRoot) {
      super(pluginsRoot);
    }

    @Override
    protected PluginDescriptorFinder createPluginDescriptorFinder() {
      return new PropertiesPluginDescriptorFinder();
    }

    @Override
    protected void validatePluginDescriptor(PluginDescriptor descriptor) throws PluginException {
      if (StringUtils.isEmpty(descriptor.getPluginId())) {
        throw new PluginException("id cannot be empty");
      }
      if (descriptor.getVersion() == null) {
        throw new PluginException("version cannot be empty");
      }
    }

    @Override
    protected PluginFactory createPluginFactory() {
        return new DefaultPluginFactory() {
          @Override
          public Plugin create(final PluginWrapper pluginWrapper) {
              String pluginClassName = pluginWrapper.getDescriptor().getPluginClass();
              if (pluginClassName != null) {
                log.debug("Create instance for plugin '{}'", pluginClassName);

                Class<?> pluginClass;
                try {
                  pluginClass = pluginWrapper.getPluginClassLoader().loadClass(pluginClassName);
                } catch (ClassNotFoundException e) {
                  log.error(e.getMessage(), e);
                  return null;
                }

                // once we have the class, we can do some checks on it to ensure
                // that it is a valid implementation of a plugin.
                int modifiers = pluginClass.getModifiers();
                if (Modifier.isAbstract(modifiers) || Modifier.isInterface(modifiers)
                    || (!Plugin.class.isAssignableFrom(pluginClass))) {
                  log.error("The plugin class '{}' is not valid", pluginClassName);
                  return null;
                }

                // create the plugin instance
                try {
                  Constructor<?> constructor = pluginClass.getConstructor(PluginWrapper.class);
                  return (Plugin) constructor.newInstance(pluginWrapper);
                } catch (Exception e) {
                  log.error(e.getMessage(), e);
                  throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Failed to create plugin", e);
                }
              } else {
                log.debug("Plugin " + pluginWrapper.getPluginId() + " has no PluginClass, creating NOP placeholder");
                return new Plugin(pluginWrapper) { /* NOP PLUGIN */ };
              }
          }
        };
    }

    @Override
    protected PluginLoader createPluginLoader() {
      return new DefaultPluginLoader(this, pluginClasspath) {
        @Override
        protected PluginClassLoader createPluginClassLoader(Path pluginPath, PluginDescriptor pluginDescriptor) {
          return new PluginClassLoader(pluginManager, pluginDescriptor, getClass().getClassLoader());
        }
      };
    }

    /*
     * Override super to make access public
     */
    @Override
    public Map<String, ClassLoader> getPluginClassLoaders() {
      return super.getPluginClassLoaders();
    }
  }
}
