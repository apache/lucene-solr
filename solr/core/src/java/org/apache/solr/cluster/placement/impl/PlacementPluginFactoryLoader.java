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

package org.apache.solr.cluster.placement.impl;

import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

/**
 * Utility class to load the configured {@link PlacementPluginFactory} plugin and
 * then keep it up to date as the plugin configuration changes.
 */
public class PlacementPluginFactoryLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void load(DelegatingPlacementPluginFactory pluginFactory, ContainerPluginsRegistry plugins) {
    ContainerPluginsRegistry.ApiInfo pluginFactoryInfo = plugins.getPlugin(PlacementPluginFactory.PLUGIN_NAME);
    if (pluginFactoryInfo != null && (pluginFactoryInfo.getInstance() instanceof PlacementPluginFactory)) {
      pluginFactory.setDelegate((PlacementPluginFactory<? extends PlacementPluginConfig>) pluginFactoryInfo.getInstance());
    }
    ContainerPluginsRegistry.PluginRegistryListener pluginListener = new ContainerPluginsRegistry.PluginRegistryListener() {
      @Override
      public void added(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof PlacementPluginFactory) {
          setDelegate(plugin.getInfo(), (PlacementPluginFactory<? extends PlacementPluginConfig>) instance);
        }
      }

      @Override
      public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
        if (plugin == null || plugin.getInstance() == null) {
          return;
        }
        Object instance = plugin.getInstance();
        if (instance instanceof PlacementPluginFactory) {
          setDelegate(plugin.getInfo(), null);
        }
      }

      @Override
      public void modified(ContainerPluginsRegistry.ApiInfo old, ContainerPluginsRegistry.ApiInfo replacement) {
        added(replacement);
      }

      private void setDelegate(PluginMeta pluginMeta, PlacementPluginFactory<? extends PlacementPluginConfig> factory) {
        if (PlacementPluginFactory.PLUGIN_NAME.equals(pluginMeta.name)) {
          pluginFactory.setDelegate(factory);
        } else {
          log.warn("Ignoring PlacementPluginFactory plugin with non-standard name: {}", pluginMeta);
        }
      }
    };
    plugins.registerListener(pluginListener);
  }
}
