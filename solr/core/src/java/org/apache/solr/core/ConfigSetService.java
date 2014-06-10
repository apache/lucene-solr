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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Service class used by the CoreContainer to load ConfigSets for use in SolrCore
 * creation.
 */
public abstract class ConfigSetService {

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
      return new ConfigSet(configName(dcore), solrConfig, schema);
    }
    catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load core configuration for core " + dcore.getName(), e);
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

    private final File configSetBase;

    /**
     * Create a new ConfigSetService.Default
     * @param loader the CoreContainer's resource loader
     * @param configSetBase the base directory under which to look for config set directories
     */
    public Default(SolrResourceLoader loader, String configSetBase) {
      super(loader);
      this.configSetBase = resolveBaseDirectory(loader, configSetBase);
    }

    private File resolveBaseDirectory(SolrResourceLoader loader, String configSetBase) {
      File csBase = new File(configSetBase);
      if (!csBase.isAbsolute())
        csBase = new File(loader.getInstanceDir(), configSetBase);
      return csBase;
    }

    // for testing
    File getConfigSetBase() {
      return this.configSetBase;
    }

    @Override
    public SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd) {
      String instanceDir = locateInstanceDir(cd);
      return new SolrResourceLoader(instanceDir, parentLoader.getClassLoader(), cd.getSubstitutableProperties());
    }

    @Override
    public String configName(CoreDescriptor cd) {
      return (cd.getConfigSet() == null ? "instancedir " : "configset ") + locateInstanceDir(cd);
    }

    protected String locateInstanceDir(CoreDescriptor cd) {
      String configSet = cd.getConfigSet();
      if (configSet == null)
        return cd.getInstanceDir();
      File configSetDirectory = new File(configSetBase, configSet);
      if (!configSetDirectory.exists() || !configSetDirectory.isDirectory())
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Could not load configuration from directory " + configSetDirectory.getAbsolutePath());
      return configSetDirectory.getAbsolutePath();
    }

  }

  /**
   * A ConfigSetService that shares schema objects between cores
   */
  public static class SchemaCaching extends Default {

    private static final Logger logger = LoggerFactory.getLogger(SchemaCaching.class);

    private final Cache<String, IndexSchema> schemaCache = CacheBuilder.newBuilder().build();

    public SchemaCaching(SolrResourceLoader loader, String configSetBase) {
      super(loader, configSetBase);
    }

    public static final DateTimeFormatter cacheKeyFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");

    public static String cacheName(File schemaFile) {
      return String.format(Locale.ROOT, "%s:%s",
                            schemaFile.getAbsolutePath(), cacheKeyFormatter.print(schemaFile.lastModified()));
    }

    @Override
    public IndexSchema createIndexSchema(final CoreDescriptor cd, final SolrConfig solrConfig) {
      final String resourceNameToBeUsed = IndexSchemaFactory.getResourceNameToBeUsed(cd.getSchemaName(), solrConfig);
      File schemaFile = new File(resourceNameToBeUsed);
      if (!schemaFile.isAbsolute()) {
        schemaFile = new File(solrConfig.getResourceLoader().getConfigDir(), schemaFile.getPath());
      }
      if (schemaFile.exists()) {
        try {
          return schemaCache.get(cacheName(schemaFile), new Callable<IndexSchema>() {
            @Override
            public IndexSchema call() throws Exception {
              logger.info("Creating new index schema for core {}", cd.getName());
              return IndexSchemaFactory.buildIndexSchema(cd.getSchemaName(), solrConfig);
            }
          });
        } catch (ExecutionException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Error creating index schema for core " + cd.getName(), e);
        }
      }
      return IndexSchemaFactory.buildIndexSchema(cd.getSchemaName(), solrConfig);
    }
  }

}
