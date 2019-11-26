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

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.managed.types.CacheManagerPool;
import org.apache.solr.search.SolrCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of {@link ResourceManagerPoolFactory}.
 */
public class DefaultResourceManagerPoolFactory implements ResourceManagerPoolFactory {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Map<String, Class<? extends ResourceManagerPool>> typeToPoolClass = new HashMap<>();
  private static final Map<String, Class<? extends ManagedComponent>> typeToComponentClass = new HashMap<>();

  public static final String TYPE_TO_POOL = "typeToPool";
  public static final String TYPE_TO_COMPONENT = "typeToComponent";

  static {
    typeToPoolClass.put(CacheManagerPool.TYPE, CacheManagerPool.class);
    typeToPoolClass.put(NoOpResourceManager.NOOP, NoOpResourceManager.NoOpResourcePool.class);
    typeToComponentClass.put(CacheManagerPool.TYPE, SolrCache.class);
    typeToComponentClass.put(NoOpResourceManager.NOOP, NoOpResourceManager.NoOpManagedComponent.class);
  }

  private final SolrResourceLoader loader;

  public DefaultResourceManagerPoolFactory(SolrResourceLoader loader, Map<String, Object> config) {
    this.loader = loader;
    Map<String, String> typeToPoolMap = (Map<String, String>)config.getOrDefault(TYPE_TO_POOL, Collections.emptyMap());
    Map<String, String> typeToComponentMap = (Map<String, String>)config.getOrDefault(TYPE_TO_COMPONENT, Collections.emptyMap());
    Map<String, Class<? extends ResourceManagerPool>> newPlugins = new HashMap<>();
    Map<String, Class<? extends ManagedComponent>> newComponents = new HashMap<>();
    typeToPoolMap.forEach((type, className) -> {
      try {
        Class<? extends ResourceManagerPool> pluginClazz = loader.findClass(className, ResourceManagerPool.class);
        newPlugins.put(type, pluginClazz);
      } catch (Exception e) {
        log.warn("Error finding plugin class", e);
      }
    });
    typeToComponentMap.forEach((type, className) -> {
      try {
        Class<? extends ManagedComponent> componentClazz = loader.findClass(className, ManagedComponent.class);
        if (typeToPoolClass.containsKey(type) || newPlugins.containsKey(type)) {
          newComponents.put(type, componentClazz);
        }
      } catch (Exception e) {
        log.warn("Error finding plugin class", e);
        newPlugins.remove(type);
      }
    });
    newPlugins.forEach((type, pluginClass) -> {
      if (!newComponents.containsKey(type) && !typeToComponentClass.containsKey(type)) {
        return;
      }
      typeToPoolClass.put(type, pluginClass);
      if (newComponents.containsKey(type)) {
        typeToComponentClass.put(type, newComponents.get(type));
      }
    });
  }

  @Override
  public <T extends ManagedComponent> ResourceManagerPool<T> create(String name, String type, ResourceManager resourceManager,
                                                                    Map<String, Object> poolLimits, Map<String, Object> poolParams) throws Exception {
    Class<? extends ResourceManagerPool> pluginClazz = typeToPoolClass.get(type);
    if (pluginClazz == null) {
      throw new IllegalArgumentException("Unsupported plugin type '" + type + "'");
    }
    Class<? extends ManagedComponent> componentClass = typeToComponentClass.get(type);
    if (componentClass == null) {
      throw new IllegalArgumentException("Unsupported component type '" + type + "'");
    }
    ResourceManagerPool<T> resourceManagerPool = loader.newInstance(
        pluginClazz.getName(),
        ResourceManagerPool.class,
        null,
        new Class[]{String.class, String.class, ResourceManager.class, Map.class, Map.class},
        new Object[]{name, type, resourceManager, poolLimits, poolParams});
    return resourceManagerPool;
  }

  @Override
  public Class<? extends ManagedComponent> getComponentClassByType(String type) {
    return typeToComponentClass.get(type);
  }

  @Override
  public Class<? extends ResourceManagerPool> getPoolClassByType(String type) {
    return typeToPoolClass.get(type);
  }
}
