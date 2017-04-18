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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Map;

import org.apache.solr.common.SolrException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ro.fortsoft.pf4j.DefaultPluginFactory;
import ro.fortsoft.pf4j.DefaultPluginManager;
import ro.fortsoft.pf4j.Plugin;
import ro.fortsoft.pf4j.PluginDescriptor;
import ro.fortsoft.pf4j.PluginDescriptorFinder;
import ro.fortsoft.pf4j.PluginException;
import ro.fortsoft.pf4j.PluginFactory;
import ro.fortsoft.pf4j.PluginLoader;
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.PropertiesPluginDescriptorFinder;
import ro.fortsoft.pf4j.util.StringUtils;

/**
 * Plugin manager for Solr that changes how to read manifest
 */
public class SolrPluginManager extends DefaultPluginManager {
  private static final Logger log = LoggerFactory.getLogger(SolrPluginManager.class);

  public SolrPluginManager(Path pluginsRoot) {
    super(pluginsRoot);
  }

  @Override
  public Path getPluginsRoot() {
    return super.getPluginsRoot();
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
      return new SolrPluginFactory();
  }

  @Override
  protected PluginLoader createPluginLoader() {
    return new SolrPluginLoader(this, pluginClasspath, getClass().getClassLoader());
  }

  public Map<String, ClassLoader> getPluginClassLoaders() {
    return super.getPluginClassLoaders();
  }

  private class SolrPluginFactory extends DefaultPluginFactory {
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
          return new SolrNopPlugin(pluginWrapper);
        }
    }

  }
}
