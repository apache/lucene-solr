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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

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
import ro.fortsoft.pf4j.util.AndFileFilter;
import ro.fortsoft.pf4j.util.DirectoryFileFilter;
import ro.fortsoft.pf4j.util.JarFileFilter;
import ro.fortsoft.pf4j.util.NotFileFilter;
import ro.fortsoft.pf4j.util.OrFileFilter;
import ro.fortsoft.pf4j.util.StringUtils;

/**
 * Plugin manager for Solr that changes how to read manifest mm
 */
public class SolrPluginManager extends DefaultPluginManager {
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
      OrFileFilter directoryOrJar = new OrFileFilter(new DirectoryFileFilter());
      directoryOrJar.addFileFilter(new JarFileFilter());
      AndFileFilter topFilter = new AndFileFilter(directoryOrJar, new NotFileFilter(createHiddenPluginFilter(isDevelopment())));
      
      setFilter(topFilter);
    }
  }
}
