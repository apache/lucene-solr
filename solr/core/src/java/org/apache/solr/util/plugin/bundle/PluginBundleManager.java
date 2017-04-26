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
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import com.github.zafarkhaja.semver.Version;
import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.DefaultPluginDescriptorFinder;
import ro.fortsoft.pf4j.DefaultPluginFactory;
import ro.fortsoft.pf4j.DefaultPluginLoader;
import ro.fortsoft.pf4j.DefaultPluginManager;
import ro.fortsoft.pf4j.DefaultPluginRepository;
import ro.fortsoft.pf4j.ManifestPluginDescriptorFinder;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginClassLoader;
import ro.fortsoft.pf4j.PluginClasspath;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginDescriptorFinder;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.PluginFactory;
import ro.fortsoft.pf4j.PluginLoader;
import ro.fortsoft.pf4j.PluginManager;
import ro.fortsoft.pf4j.PluginRepository;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.PropertiesPluginDescriptorFinder;
import ro.fortsoft.pf4j.update.PluginInfo;
import ro.fortsoft.pf4j.update.UpdateManager;
import ro.fortsoft.pf4j.update.UpdateRepository;
import ro.fortsoft.pf4j.util.AndFileFilter;
import ro.fortsoft.pf4j.util.DirectoryFileFilter;
import ro.fortsoft.pf4j.util.JarFileFilter;
import ro.fortsoft.pf4j.util.NotFileFilter;
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
//      ApacheMirrorsUpdateRepository apacheRepo = new ApacheMirrorsUpdateRepository("apache", "lucene/solr/" + systemVersion.toString() + "/");
      List<UpdateRepository> repos = new ArrayList<>();
      repos.add(new PluginUpdateRepository("apache", new URL("http://people.apache.org/~janhoy/dist/plugins/")));
      repos.add(new GitHubUpdateRepository("community","cominvent", "solr-plugins"));
      updateManager = new UpdateManager(pluginManager, (Path) null);
      updateManager.setRepositories(repos);
      pluginManager.setSystemVersion(systemVersion);
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
        try {
          updateManager.updatePlugin(plugin.id, null);
          log.info("Updatied plugin with id " + plugin.id);
        } catch (PluginException e) {
          log.warn("Failed to update plugin {}", plugin.id, e);
        }
      }
    }
  }

  public boolean install(String id) {
    try {
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

  public boolean update(String id) {
    try {
      return updateManager.updatePlugin(id, null);
    } catch (PluginException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to update plugin with id " + id, e);
    }
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
//      try {
//        return super.findClass(name);
//      } catch (ClassNotFoundException ignored) {}
      for (ClassLoader loader : manager.getPluginClassLoaders().values()) {
        try {
          return loader.loadClass(name);
        } catch (ClassNotFoundException ignore) {}
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
      return new AutoPluginDescriptorFinder();
    }

    @Override
    protected PluginRepository createPluginRepository() {
      return new AutoPluginRepository(getPluginsRoot(), isDevelopment());
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
          if (pluginWrapper.getDescriptor().getPluginClass() == null) {
            log.debug("Plugin " + pluginWrapper.getPluginId() + " has no PluginClass, creating NOP placeholder");
            return new Plugin(pluginWrapper) { /* NOP PLUGIN */ };
          } else {
            return super.create(pluginWrapper);
          }
        }
      };
    }

    @Override
    protected PluginLoader createPluginLoader() {
      return new AutoPluginLoader(this, pluginClasspath);
    }

    /*
     * Override super to make access public
     */
    @Override
    public Map<String, ClassLoader> getPluginClassLoaders() {
      return super.getPluginClassLoaders();
    }

    static class JarPluginDescriptorFinder extends ManifestPluginDescriptorFinder {

        @Override
        public Manifest readManifest(Path pluginPath) throws PluginException {
            try {
                return new JarFile(pluginPath.toFile()).getManifest();
            } catch (IOException e) {
                throw new PluginException(e);
            }
        }

    }

    static class JarPluginLoader extends DefaultPluginLoader {

        public JarPluginLoader(PluginManager pluginManager, PluginClasspath pluginClasspath) {
            super(pluginManager, pluginClasspath);
        }

        @Override
        public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
            PluginClassLoader pluginClassLoader = new PluginClassLoader(pluginManager, pluginDescriptor, getClass().getClassLoader());
            pluginClassLoader.addFile(pluginPath.toFile());

            return pluginClassLoader;
        }

    }

    /**
     * Descriptor finder that determines what to look for based on the given path
     */
    private class AutoPluginDescriptorFinder implements PluginDescriptorFinder {
      private final DefaultPluginDescriptorFinder manifestFinder;
      private final PropertiesPluginDescriptorFinder propertiesFinder;
      private final JarPluginDescriptorFinder jarFinder;


      public AutoPluginDescriptorFinder() {
        manifestFinder = new DefaultPluginDescriptorFinder(pluginClasspath);
        propertiesFinder = new PropertiesPluginDescriptorFinder();
        jarFinder = new JarPluginDescriptorFinder();
      }

      @Override
      public PluginDescriptor find(Path pluginPath) throws PluginException {
        if (pluginPath.toString().endsWith(".jar")) {
          return (jarFinder.find(pluginPath));
        } else if (Files.isDirectory(pluginPath)) {
          try {
            return propertiesFinder.find(pluginPath);
          } catch (Exception e) {
            return manifestFinder.find(pluginPath);
          }
        } else {
          return manifestFinder.find(pluginPath);
        }
      }
    }

    /**
     * Plugin loader that can load both jar and zip plugins
     */
    private class AutoPluginLoader implements PluginLoader {
      private final SolrPluginManager solrPluginManager;
      private final PluginClasspath pluginClasspath;
      private final JarPluginLoader jarLoader;
      private final DefaultPluginLoader defaultLoader;

      public AutoPluginLoader(SolrPluginManager solrPluginManager, PluginClasspath pluginClasspath) {
        this.solrPluginManager = solrPluginManager;
        this.pluginClasspath = pluginClasspath;
        jarLoader = new JarPluginLoader(solrPluginManager, pluginClasspath);
        defaultLoader = new DefaultPluginLoader(solrPluginManager, pluginClasspath);
      }

      @Override
      public ClassLoader loadPlugin(Path pluginPath, PluginDescriptor pluginDescriptor) {
        if (pluginPath.toString().endsWith(".jar")) {
          return jarLoader.loadPlugin(pluginPath, pluginDescriptor);
        } else {
          return defaultLoader.loadPlugin(pluginPath, pluginDescriptor);
        }
      }
    }

    /**
     * Repository that loads both zip files, unpacked zip files and jar plugins
     */
    private class AutoPluginRepository extends DefaultPluginRepository {
      public AutoPluginRepository(Path pluginsRoot, boolean development) {
        super(pluginsRoot, development);
        AndFileFilter pluginsFilter = new AndFileFilter(new DirectoryFileFilter());
        pluginsFilter.addFileFilter(new NotFileFilter(createHiddenPluginFilter(isDevelopment())));
        pluginsFilter.addFileFilter(new JarFileFilter());
        setFilter(pluginsFilter);
      }
    }
  }

}
