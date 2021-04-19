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
package org.apache.solr.schema;


import org.apache.solr.cloud.CloudConfigSetService;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Base class for factories for IndexSchema implementations */
public abstract class IndexSchemaFactory implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static IndexSchema buildIndexSchema(String resourceName, SolrConfig config) {
    return buildIndexSchema(resourceName, config, null);
  }
  /** Instantiates the configured schema factory, then calls create on it. */
  public static IndexSchema buildIndexSchema(String resourceName, SolrConfig config, ConfigSetService configSetService) {
    return newIndexSchemaFactory(config).create(resourceName, config, configSetService);
  }

  /** Instantiates us from {@link SolrConfig}. */
  public static IndexSchemaFactory newIndexSchemaFactory(SolrConfig config) {
    PluginInfo info = config.getPluginInfo(IndexSchemaFactory.class.getName());
    IndexSchemaFactory factory;
    if (null != info) {
      factory = config.getResourceLoader().newInstance(info.className, IndexSchemaFactory.class);
      factory.init(info.initArgs);
    } else {
      factory = config.getResourceLoader().newInstance(ManagedIndexSchemaFactory.class.getName(), IndexSchemaFactory.class);
    }
    return factory;
  }

  /**
   * Returns the resource (file) name that will be used for the schema itself.  The answer may be a guess.
   * Do not pass the result of this to {@link #create(String, SolrConfig, ConfigSetService)}.
   * The input is the name coming from the {@link org.apache.solr.core.CoreDescriptor}
   * which acts as a default or asked-for name.
   */
  public String getSchemaResourceName(String cdResourceName) {
    return cdResourceName;
  }

  /**
   * Returns an index schema created from a local resource.  The input is usually from the core descriptor.
   */
  public IndexSchema create(String resourceName, SolrConfig config, ConfigSetService configSetService) {
    SolrResourceLoader loader = config.getResourceLoader();
    InputStream schemaInputStream = null;

    if (null == resourceName) {
      resourceName = IndexSchema.DEFAULT_SCHEMA_FILE;
    }
    try {
      schemaInputStream = loader.openResource(resourceName);
      return new IndexSchema(resourceName, getConfigResource(configSetService, schemaInputStream, loader, resourceName), config.luceneMatchVersion, loader, config.getSubstituteProperties());
    } catch (RuntimeException rte) {
      throw rte;
    } catch (Exception e) {
      final String msg = "Error loading schema resource " + resourceName;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
  }

  @SuppressWarnings("unchecked")
  public static ConfigSetService.ConfigResource getConfigResource(ConfigSetService configSetService, InputStream schemaInputStream, SolrResourceLoader loader, String name) throws IOException {
    if (configSetService instanceof CloudConfigSetService && schemaInputStream instanceof ZkSolrResourceLoader.ZkByteArrayInputStream) {
      ZkSolrResourceLoader.ZkByteArrayInputStream is = (ZkSolrResourceLoader.ZkByteArrayInputStream) schemaInputStream;
      Map<String, VersionedConfig> configCache = (Map<String, VersionedConfig>) ((CloudConfigSetService) configSetService).getSolrCloudManager().getObjectCache()
              .computeIfAbsent(ConfigSetService.ConfigResource.class.getName(), s -> new ConcurrentHashMap<>());
      VersionedConfig cached = configCache.get(is.fileName);
      if (cached != null) {
        if (cached.version != is.getStat().getVersion()) {
          configCache.remove(is.fileName);// this is stale. remove from cache
        } else {
          return () -> cached.data;
        }
      }
      return () -> {
        ConfigNode data = ConfigSetService.getParsedSchema(schemaInputStream, loader, name);// either missing or stale. create a new one
        configCache.put(is.fileName, new VersionedConfig(is.getStat().getVersion(), data));
        return data;
      };
    }
    //this is not cacheable as it does not come from ZK
    return () -> ConfigSetService.getParsedSchema(schemaInputStream,loader, name);
  }

  public static class VersionedConfig {
    public final int version;
    public final ConfigNode data;

    public VersionedConfig(int version, ConfigNode data) {
      this.version = version;
      this.data = data;
    }
  }

}
