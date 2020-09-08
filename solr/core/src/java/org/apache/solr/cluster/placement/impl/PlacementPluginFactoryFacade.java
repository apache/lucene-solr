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

import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;

/**
 * <p>The internal class instantiating the configured {@link PlacementPluginFactory} and creating a {@link PlacementPlugin}
 * instance by passing to the factory the appropriate configuration created from the {@code <placementPluginFactory>}
 * element in {@code solr.xml}.
 *
 * <p>A single instance of {@link PlacementPlugin} is used for all placement computations and therefore must be reentrant.
 * When configuration changes, a new instance of {@link PlacementPlugin} will be created by calling again
 * {@link PlacementPluginFactory#createPluginInstance(PlacementPluginConfig)}.
 */
public class PlacementPluginFactoryFacade {
  final PlacementPlugin placementPlugin;

  public static PlacementPluginFactoryFacade newInstance(PluginInfo info, SolrResourceLoader loader)  {
    if (info == null) {
      // placement plugins are not configured. Other placement strategy will be used. See Assign.createAssignStrategy()
      return null;
    }

    final PlacementPluginFactory placementPluginFactory;

    try {
      placementPluginFactory = loader.findClass(info.className, PlacementPluginFactory.class).getConstructor().newInstance();
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error instantiating PlacementPluginFactory class [" +
          info.className + "]: " + e.getMessage(), e);
    }

    final PlacementPluginConfig placementPluginConfig = PlacementPluginConfigImpl.createConfigFromInfo(info);

    // Ask the configured factory to create a plugin instance with the given config. We'll then reuse that plugin instance
    // for all placement calls until the config changes. Current implementation will re-create an instance of this class
    // that will recreate an instance of the PlacementPluginFactory factory (and will then
    // create a new plugin instance) but this could be changed later to reuse a single instance of PlacementPluginFactory
    // and only call its createPluginInstance() method with the new config when the class name of the configured factory
    // does not change.
    final PlacementPlugin placementPlugin = placementPluginFactory.createPluginInstance(placementPluginConfig);

    return new PlacementPluginFactoryFacade(placementPlugin);
  }

  public PlacementPlugin getPlacementPluginInstance() {
    return placementPlugin;
  }

  private PlacementPluginFactoryFacade(PlacementPlugin placementPlugin) {
    this.placementPlugin = placementPlugin;
  }
}
