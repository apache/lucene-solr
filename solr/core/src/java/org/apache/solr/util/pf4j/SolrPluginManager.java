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

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

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
import ro.fortsoft.pf4j.PluginWrapper;
import ro.fortsoft.pf4j.PropertiesPluginDescriptorFinder;
import ro.fortsoft.pf4j.util.StringUtils;

/**
 * Plugin manager for Solr that changes how to read manifest
 */
public class SolrPluginManager extends DefaultPluginManager {
  private static final Logger log = LoggerFactory.getLogger(SolrPluginManager.class);

  @Override
  protected PluginDescriptorFinder createPluginDescriptorFinder() {
    return isDevelopment() ? new PropertiesPluginDescriptorFinder() :
        new PropertiesPluginDescriptorFinder() {
          // Do not require plugin.class, since we don't use pf4j resolving right now
          @Override
          protected void validatePluginDescriptor(PluginDescriptor pluginDescriptor) throws PluginException {
            if (StringUtils.isEmpty(pluginDescriptor.getPluginId())) {
              throw new PluginException("plugin.id cannot be empty");
            }
            if (pluginDescriptor.getVersion() == null) {
              throw new PluginException("plugin.version cannot be empty");
            }
          }
        };
  }

  @Override
  protected PluginFactory createPluginFactory() {
      return new SolrPluginFactory();
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
          log.info("Plugin " + pluginWrapper.getPluginId() + " has no PluginClass, creating NOP placeholder");
          return new SolrNopPlugin(pluginWrapper);
        }
    }

  }


//  @Override
//  public PluginState startPlugin(String pluginId) {
//      if (!plugins.containsKey(pluginId)) {
//          throw new IllegalArgumentException(String.format("Unknown pluginId %s", pluginId));
//      }
//
//      PluginWrapper pluginWrapper = getPlugin(pluginId);
//      PluginDescriptor pluginDescriptor = pluginWrapper.getDescriptor();
//      PluginState pluginState = pluginWrapper.getPluginState();
//      if (PluginState.STARTED == pluginState) {
//          log.debug("Already started plugin '{}:{}'", pluginDescriptor.getPluginId(), pluginDescriptor.getVersion());
//          return PluginState.STARTED;
//      }
//
//      if (PluginState.DISABLED == pluginState) {
//          // automatically enable plugin on manual plugin start
//          if (!enablePlugin(pluginId)) {
//              return pluginState;
//          }
//      }
//
//      for (PluginDependency dependency : pluginDescriptor.getDependencies()) {
//          startPlugin(dependency.getPluginId());
//      }
//
//      try {
//          log.info("Start plugin '{}:{}'", pluginDescriptor.getPluginId(), pluginDescriptor.getVersion());
//          pluginWrapper.getPlugin().start();
//          pluginWrapper.setPluginState(PluginState.STARTED);
//          startedPlugins.add(pluginWrapper);
//
//          firePluginStateEvent(new PluginStateEvent(this, pluginWrapper, pluginState));
//      } catch (PluginException e) {
//          log.error(e.getMessage(), e);
//      }
//
//      return pluginWrapper.getPluginState();
//  }
}
