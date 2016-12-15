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
package org.apache.solr.core;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.solr.cloud.CloudConfigSetService;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service class used by the CoreContainer to load ConfigSets for use in SolrCore
 * creation.
 */
public abstract class ConfigSetService {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static ConfigSetService createConfigSetService(NodeConfig nodeConfig, SolrResourceLoader loader, ZkController zkController) {
    if (zkController != null)
      return new CloudConfigSetService(loader, zkController);

    if (nodeConfig.hasSchemaCache())
      return new SchemaCaching(loader, nodeConfig.getConfigSetBaseDirectory());

    return new Default(loader, nodeConfig.getConfigSetBaseDirectory());
  }

  protected final SolrResourceLoader parentLoader;

  /**
   * Create a new ConfigSetService
   * @param loader the CoreContainer's resource loader
   */
  public ConfigSetService(SolrResourceLoader loader) {
    this.parentLoader = loader;
  }

  /**
   * Load the ConfigSet for a core
   * @param dcore the core's CoreDescriptor
   * @return a ConfigSet
   */
  public final ConfigSet getConfig(CoreDescriptor dcore) {

    SolrResourceLoader coreLoader = createCoreResourceLoader(dcore);

    try {
      SolrConfig solrConfig = createSolrConfig(dcore, coreLoader);
      IndexSchema schema = createIndexSchema(dcore, solrConfig);
      NamedList properties = createConfigSetProperties(dcore, coreLoader);
      return new ConfigSet(configName(dcore), solrConfig, schema, properties);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load conf for core " + dcore.getName() +
              ": " + e.getMessage(), e);
    }

  }

  /**
   * Create a SolrConfig object for a core
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @return a SolrConfig object
   */
  protected SolrConfig createSolrConfig(CoreDescriptor cd, SolrResourceLoader loader) {
    return SolrConfig.readFromResourceLoader(loader, cd.getConfigName());
  }

  /**
   * Create an IndexSchema object for a core
   * @param cd the core's CoreDescriptor
   * @param solrConfig the core's SolrConfig
   * @return an IndexSchema
   */
  protected IndexSchema createIndexSchema(CoreDescriptor cd, SolrConfig solrConfig) {
    return IndexSchemaFactory.buildIndexSchema(cd.getSchemaName(), solrConfig);
  }

  /**
   * Return the ConfigSet properties
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @return the ConfigSet properties
   */
  protected NamedList createConfigSetProperties(CoreDescriptor cd, SolrResourceLoader loader) {
    return ConfigSetProperties.readFromResourceLoader(loader, cd.getConfigSetPropertiesName());
  }

  /**
   * Create a SolrResourceLoader for a core
   * @param cd the core's CoreDescriptor
   * @return a SolrResourceLoader
   */
  protected abstract SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd);

  /**
   * Return a name for the ConfigSet for a core
   * @param cd the core's CoreDescriptor
   * @return a name for the core's ConfigSet
   */
  public abstract String configName(CoreDescriptor cd);

  /**
   * The default ConfigSetService.
   *
   * Loads a ConfigSet defined by the core's configSet property,
   * looking for a directory named for the configSet property value underneath
   * a base directory.  If no configSet property is set, loads the ConfigSet
   * instead from the core's instance directory.
   */
  public static class Default extends ConfigSetService {

    private final Path configSetBase;

    /**
     * Create a new ConfigSetService.Default
     * @param loader the CoreContainer's resource loader
     * @param configSetBase the base directory under which to look for config set directories
     */
    public Default(SolrResourceLoader loader, Path configSetBase) {
      super(loader);
      this.configSetBase = configSetBase;
    }

    @Override
    public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
      Path instanceDir = locateInstanceDir(cd);
      return new SolrResourceLoader(instanceDir, parentLoader.getClassLoader(), cd.getSubstitutableProperties());
    }

    @Override
    public String configName(CoreDescriptor cd) {
      return (cd.getConfigSet() == null ? "instancedir " : "configset ") + locateInstanceDir(cd);
    }

    protected Path locateInstanceDir(CoreDescriptor cd) {
      String configSet = cd.getConfigSet();
      if (configSet == null)
        return cd.getInstanceDir();
      Path configSetDirectory = configSetBase.resolve(configSet);
      if (!Files.isDirectory(configSetDirectory))
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not load configuration from directory " + configSetDirectory);
      return configSetDirectory;
    }

  }

  /**
   * A ConfigSetService that shares schema objects between cores
   */
  public static class SchemaCaching extends Default {

    private final Cache<String, IndexSchema> schemaCache = CacheBuilder.newBuilder().build();

    public SchemaCaching(SolrResourceLoader loader, Path configSetBase) {
      super(loader, configSetBase);
    }

    public static final DateTimeFormatter cacheKeyFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    public static String cacheName(Path schemaFile) throws IOException {
      long lastModified = Files.getLastModifiedTime(schemaFile).toMillis();
      return String.format(Locale.ROOT, "%s:%s",
                            schemaFile.toString(), cacheKeyFormatter.print(lastModified));
    }

    @Override
    public IndexSchema createIndexSchema(final CoreDescriptor cd, final SolrConfig solrConfig) {
      final String resourceNameToBeUsed = IndexSchemaFactory.getResourceNameToBeUsed(cd.getSchemaName(), solrConfig);
      Path schemaFile = Paths.get(solrConfig.getResourceLoader().getConfigDir()).resolve(resourceNameToBeUsed);
      if (Files.exists(schemaFile)) {
        try {
          String cachedName = cacheName(schemaFile);
          return schemaCache.get(cachedName, () -> {
            logger.info("Creating new index schema for core {}", cd.getName());
            return IndexSchemaFactory.buildIndexSchema(cd.getSchemaName(), solrConfig);
          });
        } catch (ExecutionException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error creating index schema for core " + cd.getName(), e);
        } catch (IOException e) {
          logger.warn("Couldn't get last modified time for schema file {}: {}", schemaFile, e.getMessage());
          logger.warn("Will not use schema cache");
        }
      }
      return IndexSchemaFactory.buildIndexSchema(cd.getSchemaName(), solrConfig);
    }
  }

}
