package org.apache.solr.schema;
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

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/** Factory for ManagedIndexSchema */
public class ManagedIndexSchemaFactory extends IndexSchemaFactory implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(ManagedIndexSchemaFactory.class);
  private static final String UPGRADED_SCHEMA_EXTENSION = ".bak";
  private static final String SCHEMA_DOT_XML = "schema.xml";

  public static final String DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME = "managed-schema";
  public static final String MANAGED_SCHEMA_RESOURCE_NAME = "managedSchemaResourceName";

  private boolean isMutable;
  private String managedSchemaResourceName;
  public String getManagedSchemaResourceName() { return managedSchemaResourceName; }
  private SolrConfig config;
  private SolrResourceLoader loader;
  public SolrResourceLoader getResourceLoader() { return loader; }
  private String resourceName;
  private ManagedIndexSchema schema;
  private SolrCore core;
  private ZkIndexSchemaReader zkIndexSchemaReader;


  private String loadedResource;
  private boolean shouldUpgrade = false;

  @Override
  public void init(NamedList args) {
    SolrParams params = SolrParams.toSolrParams(args);
    isMutable = params.getBool("mutable", false);
    args.remove("mutable");
    managedSchemaResourceName = params.get(MANAGED_SCHEMA_RESOURCE_NAME, DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME);
    args.remove(MANAGED_SCHEMA_RESOURCE_NAME);
    if (SCHEMA_DOT_XML.equals(managedSchemaResourceName)) {
      String msg = MANAGED_SCHEMA_RESOURCE_NAME + " can't be '" + SCHEMA_DOT_XML + "'";
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
    
    if (args.size() > 0) {
      String msg = "Unexpected arg(s): " + args;
      log.error(msg);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg);
    }
  }

  /**
   * First, try to locate the managed schema file named in the managedSchemaResourceName
   * param. If the managed schema file exists and is accessible, it is used to instantiate
   * an IndexSchema.
   *
   * If the managed schema file can't be found, the resource named by the resourceName
   * parameter is used to instantiate an IndexSchema.
   *
   * Once the IndexSchema is instantiated, if the managed schema file does not exist,
   * the instantiated IndexSchema is persisted to the managed schema file named in the
   * managedSchemaResourceName param, in the directory given by 
   * {@link org.apache.solr.core.SolrResourceLoader#getConfigDir()}, or if configs are
   * in ZooKeeper, under {@link org.apache.solr.cloud.ZkSolrResourceLoader#collectionZkPath}.
   *
   * After the managed schema file is persisted, the original schema file is
   * renamed by appending the extension named in {@link #UPGRADED_SCHEMA_EXTENSION}.
   */
  @Override
  public ManagedIndexSchema create(String resourceName, SolrConfig config) {
    this.resourceName = resourceName;
    this.config = config;
    this.loader = config.getResourceLoader();
    InputStream schemaInputStream = null;

    if (null == resourceName) {
      resourceName = IndexSchema.DEFAULT_SCHEMA_FILE;
    }

    int schemaZkVersion = -1;
    if ( ! (loader instanceof ZkSolrResourceLoader)) {
      schemaInputStream = readSchemaLocally();
    } else { // ZooKeeper
      final ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
      final SolrZkClient zkClient = zkLoader.getZkController().getZkClient();
      final String managedSchemaPath = zkLoader.getCollectionZkPath() + "/" + managedSchemaResourceName;
      Stat stat = new Stat();
      try {
        // Attempt to load the managed schema
        byte[] data = zkClient.getData(managedSchemaPath, null, stat, true);
        schemaZkVersion = stat.getVersion();
        schemaInputStream = new ByteArrayInputStream(data);
        loadedResource = managedSchemaResourceName;
        warnIfNonManagedSchemaExists();
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      } catch (KeeperException.NoNodeException e) {
        log.info("The schema is configured as managed, but managed schema resource " + managedSchemaResourceName
                + " not found - loading non-managed schema " + resourceName + " instead");
      } catch (KeeperException e) {
        String msg = "Error attempting to access " + managedSchemaPath;
        log.error(msg, e);
        throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
      }
      if (null == schemaInputStream) {
        // The managed schema file could not be found - load the non-managed schema
        try {
          schemaInputStream = loader.openSchema(resourceName);
          loadedResource = resourceName;
          shouldUpgrade = true;
        } catch (Exception e) {
          try {
            // Retry to load the managed schema, in case it was created since the first attempt
            byte[] data = zkClient.getData(managedSchemaPath, null, stat, true);
            schemaZkVersion = stat.getVersion();
            schemaInputStream = new ByteArrayInputStream(data);
            warnIfNonManagedSchemaExists();
          } catch (Exception e1) {
            if (e1 instanceof InterruptedException) {
              Thread.currentThread().interrupt(); // Restore the interrupted status
            }
            final String msg = "Error loading both non-managed schema '" + resourceName + "' and managed schema '"
                             + managedSchemaResourceName + "'";
            log.error(msg, e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
          }
        }
      }
    }
    InputSource inputSource = new InputSource(schemaInputStream);
    inputSource.setSystemId(SystemIdResolver.createSystemIdFromResourceName(loadedResource));
    try {
      schema = new ManagedIndexSchema(config, loadedResource, inputSource, isMutable, 
                                      managedSchemaResourceName, schemaZkVersion, getSchemaUpdateLock());
    } catch (KeeperException e) {
      final String msg = "Error instantiating ManagedIndexSchema";
      log.error(msg, e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.warn("", e);
    }

    if (shouldUpgrade) {
      // Persist the managed schema if it doesn't already exist
      upgradeToManagedSchema();
    }

    return schema;
  }

  private InputStream readSchemaLocally() {
    InputStream schemaInputStream = null;
    try {
      // Attempt to load the managed schema
      schemaInputStream = loader.openSchema(managedSchemaResourceName);
      loadedResource = managedSchemaResourceName;
      warnIfNonManagedSchemaExists();
    } catch (IOException e) {
      log.info("The schema is configured as managed, but managed schema resource " + managedSchemaResourceName
              + " not found - loading non-managed schema " + resourceName + " instead");
    }
    if (null == schemaInputStream) {
      // The managed schema file could not be found - load the non-managed schema
      try {
        schemaInputStream = loader.openSchema(resourceName);
        loadedResource = resourceName;
        shouldUpgrade = true;
      } catch (Exception e) {
        final String msg = "Error loading both non-managed schema '" + resourceName + "' and managed schema '"
                         + managedSchemaResourceName + "'";
        log.error(msg, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
      }
    }
    return schemaInputStream; 
  }

  /**
   * Return whether a non-managed schema exists, either in local storage or on ZooKeeper. 
   */
  private void warnIfNonManagedSchemaExists() {
    if ( ! resourceName.equals(managedSchemaResourceName)) {
      boolean exists = false;
      SolrResourceLoader loader = config.getResourceLoader();
      if (loader instanceof ZkSolrResourceLoader) {
        ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
        String nonManagedSchemaPath = zkLoader.getCollectionZkPath() + "/" + resourceName;
        try {
          exists = zkLoader.getZkController().pathExists(nonManagedSchemaPath);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
          log.warn("", e); // Log as warning and suppress the exception 
        } catch (KeeperException e) {
          // log as warning and suppress the exception
          log.warn("Error checking for the existence of the non-managed schema " + resourceName, e);
        }
      } else { // Config is not in ZooKeeper
        InputStream nonManagedSchemaInputStream = null;
        try {
          nonManagedSchemaInputStream = loader.openSchema(resourceName);
          if (null != nonManagedSchemaInputStream) {
            exists = true;
          }
        } catch (IOException e) {
          // This is expected when the non-managed schema does not exist
        } finally {
          IOUtils.closeQuietly(nonManagedSchemaInputStream);
        }
      }
      if (exists) {
        log.warn("The schema has been upgraded to managed, but the non-managed schema " + resourceName
                + " is still loadable.  PLEASE REMOVE THIS FILE.");
      }
    }
  }
  
  /**
   * Persist the managed schema and rename the non-managed schema 
   * by appending {@link #UPGRADED_SCHEMA_EXTENSION}.
   *
   * Failure to rename the non-managed schema will be logged as a warning,
   * and no exception will be thrown.
   */
  private void upgradeToManagedSchema() {
    SolrResourceLoader loader = config.getResourceLoader();
    if (loader instanceof ZkSolrResourceLoader) {
      zkUgradeToManagedSchema();
    } else {
      // Configs are not on ZooKeeper
      schema.persistManagedSchema(true);  // Only create it - don't update it if it already exists

      // After successfully persisting the managed schema, rename the non-managed
      // schema file by appending UPGRADED_SCHEMA_EXTENSION to its name.

      if (resourceName.equals(managedSchemaResourceName)) {
        log.info("On upgrading to managed schema, did not rename non-managed schema '"
            + resourceName + "' because it's the same as the managed schema's name.");
      } else {
        final File nonManagedSchemaFile = locateConfigFile(resourceName);
        if (null == nonManagedSchemaFile) {
          // Don't throw an exception for failure to rename the non-managed schema
          log.warn("On upgrading to managed schema, did not rename non-managed schema "
              + resourceName + " because it's neither an absolute file "
              + "nor under SolrConfig.getConfigDir() or the current directory."
              + "  PLEASE REMOVE THIS FILE.");
        } else {
          File upgradedSchemaFile = new File(nonManagedSchemaFile.getPath() + UPGRADED_SCHEMA_EXTENSION);
          if (nonManagedSchemaFile.renameTo(upgradedSchemaFile)) {
            // Set the resource name to the managed schema so that the CoreAdminHandler returns a findable filename 
            schema.setResourceName(managedSchemaResourceName);

            log.info("After upgrading to managed schema, renamed the non-managed schema "
                + nonManagedSchemaFile + " to " + upgradedSchemaFile);
          } else {
            // Don't throw an exception for failure to rename the non-managed schema
            log.warn("Can't rename " + nonManagedSchemaFile.toString() + " to "
                + upgradedSchemaFile.toString() + " - PLEASE REMOVE THIS FILE.");
          }
        }
      }
    }
  }

  /**
   * Finds any resource by its name on the filesystem.  The classpath is not consulted.
   *
   * If the resource is not absolute, the resource is sought in $configDir and then in the current directory.
   *
   *@return the File for the named resource, or null if it can't be found
   */
  private File locateConfigFile(String resource) {
    File located = null;
    File file = new File(resource);
    if (file.isAbsolute()) {
      if (file.isFile() && file.canRead()) {
        located = file;
      }
    } else {
      // try $configDir/$resource
      File fileUnderConfigDir = new File(config.getResourceLoader().getConfigDir() + resource);
      if (fileUnderConfigDir.isFile() && fileUnderConfigDir.canRead()) {
        located = fileUnderConfigDir;
      } else {
        // no success with $configDir/$resource - try $CWD/$resource
        if (file.isFile() && file.canRead()) {
          located = file;
        }
      }
    }
    return located;
  }

  /**
   * Persist the managed schema to ZooKeeper and rename the non-managed schema 
   * by appending {@link #UPGRADED_SCHEMA_EXTENSION}.
   *
   * Failure to rename the non-managed schema will be logged as a warning,
   * and no exception will be thrown.
   */
  private void zkUgradeToManagedSchema() {
    schema.persistManagedSchemaToZooKeeper(true); // Only create, don't update it if it already exists

    // After successfully persisting the managed schema, rename the non-managed
    // schema znode by appending UPGRADED_SCHEMA_EXTENSION to its name.

    if (resourceName.equals(managedSchemaResourceName)) {
      log.info("On upgrading to managed schema, did not rename non-managed schema "
          + resourceName + " because it's the same as the managed schema's name.");
    } else {
      // Rename the non-managed schema znode in ZooKeeper
      ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
      final String nonManagedSchemaPath = zkLoader.getCollectionZkPath() + "/" + resourceName;
      try {
        ZkController zkController = zkLoader.getZkController();
        ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(zkController.getClientTimeout());
        if (zkController.pathExists(nonManagedSchemaPath)) {
          // First, copy the non-managed schema znode content to the upgraded schema znode
          byte[] bytes = zkController.getZkClient().getData(nonManagedSchemaPath, null, null, true);
          final String upgradedSchemaPath = nonManagedSchemaPath + UPGRADED_SCHEMA_EXTENSION;
          zkCmdExecutor.ensureExists(upgradedSchemaPath, zkController.getZkClient());
          zkController.getZkClient().setData(upgradedSchemaPath, bytes, true);
          // Then delete the non-managed schema znode
          zkController.getZkClient().delete(nonManagedSchemaPath, -1, true);

          // Set the resource name to the managed schema so that the CoreAdminHandler returns a findable filename 
          schema.setResourceName(managedSchemaResourceName);

          log.info("After upgrading to managed schema in ZooKeeper, renamed the non-managed schema "
                  + nonManagedSchemaPath + " to " + upgradedSchemaPath);
        } else {
          log.info("After upgrading to managed schema in ZooKeeper, the non-managed schema "
                  + nonManagedSchemaPath + " no longer exists.");
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
        }
        final String msg = "Error persisting managed schema resource " + managedSchemaResourceName;
        log.warn(msg, e); // Log as warning and suppress the exception
      }
    }
  }

  private Object schemaUpdateLock = new Object();
  public Object getSchemaUpdateLock() { return schemaUpdateLock; }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    if (loader instanceof ZkSolrResourceLoader) {
      this.zkIndexSchemaReader = new ZkIndexSchemaReader(this);
      ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
      zkLoader.setZkIndexSchemaReader(this.zkIndexSchemaReader);
    } else {
      this.zkIndexSchemaReader = null;
    }
  }

  public ManagedIndexSchema getSchema() {
    return schema;
  }

  public void setSchema(ManagedIndexSchema schema) {
    this.schema = schema;
    core.setLatestSchema(schema);
  }
}
