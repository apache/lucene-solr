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
package org.apache.solr.managed;

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.managed.types.CacheManagerPlugin;
import org.apache.solr.managed.types.ManagedCacheComponent;

/**
 * Default implementation of {@link ResourceManagerPluginFactory}.
 */
public class DefaultResourceManagerPluginFactory implements ResourceManagerPluginFactory {

  private static final Map<String, Class<? extends ResourceManagerPlugin>> typeToPluginClass = new HashMap<>();
  private static final Map<String, Class<? extends ManagedComponent>> typeToComponentClass = new HashMap<>();

  static {
    typeToPluginClass.put(CacheManagerPlugin.TYPE, CacheManagerPlugin.class);
    typeToComponentClass.put(CacheManagerPlugin.TYPE, ManagedCacheComponent.class);
  }

  private final SolrResourceLoader loader;

  public DefaultResourceManagerPluginFactory(SolrResourceLoader loader) {
    this.loader = loader;
  }

  @Override
  public <T extends ManagedComponent> ResourceManagerPlugin<T> create(String type, Map<String, Object> params) throws Exception {
    Class<? extends ResourceManagerPlugin> pluginClazz = typeToPluginClass.get(type);
    if (pluginClazz == null) {
      throw new IllegalArgumentException("Unsupported plugin type '" + type + "'");
    }
    ResourceManagerPlugin<T> resourceManagerPlugin = loader.newInstance(pluginClazz.getName(), ResourceManagerPlugin.class);
    resourceManagerPlugin.init(params);
    return resourceManagerPlugin;
  }

  @Override
  public Class<? extends ManagedComponent> getComponentClassByType(String type) {
    return typeToComponentClass.get(type);
  }

  @Override
  public Class<? extends ResourceManagerPlugin> getPluginClassByType(String type) {
    return typeToPluginClass.get(type);
  }
}
