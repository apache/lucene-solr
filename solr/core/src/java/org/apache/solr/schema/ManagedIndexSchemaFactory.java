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
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;

import org.apache.commons.io.IOUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

/** Factory for ManagedIndexSchema */
public class ManagedIndexSchemaFactory extends IndexSchemaFactory implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String UPGRADED_SCHEMA_EXTENSION = ".bak";
  private static final String SCHEMA_DOT_XML = "schema.xml";

  public static final String DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME = "managed-schema";
  public static final String MANAGED_SCHEMA_RESOURCE_NAME = "managedSchemaResourceName";

  private boolean isMutable = true;
  private String managedSchemaResourceName = DEFAULT_MANAGED_SCHEMA_RESOURCE_NAME;
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
  public void init(@SuppressWarnings({"rawtypes"})NamedList args) {
    SolrParams params = args.toSolrParams();
    isMutable = params.getBool("mutable", true);
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

  @Override
  public String getSchemaResourceName(String cdResourceName) {
    return managedSchemaResourceName; // actually a guess; reality depends on the actual files in the config set :-(
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
   * in ZooKeeper, under {@link org.apache.solr.cloud.ZkSolrResourceLoader#getConfigSetZkPath()}.
   *
   * After the managed schema file is persisted, the original schema file is
   * renamed by appending the extension named in {@link #UPGRADED_SCHEMA_EXTENSION}.
   */
  @Override
  public ManagedIndexSchema create(String resourceName, SolrConfig config, ConfigSetService configSetService) {
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
      final String managedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + managedSchemaResourceName;
      Stat stat = new Stat();
      try {
        // Attempt to load the managed schema
        byte[] data = zkClient.getData(managedSchemaPath, null, stat, true);
        schemaZkVersion = stat.getVersion();
        schemaInputStream = new ZkSolrResourceLoader.ZkByteArrayInputStream(data, managedSchemaPath, stat);
        loadedResource = managedSchemaResourceName;
        warnIfNonManagedSchemaExists();
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      } catch (KeeperException.NoNodeException e) {
        log.info("The schema is configured as managed, but managed schema resource {} not found - loading non-managed schema {} instead"
            , managedSchemaResourceName, resourceName);
      } catch (KeeperException e) {
        String msg = "Error attempting to access " + managedSchemaPath;
        log.error(msg, e);
        throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
      }
      if (null == schemaInputStream) {
        // The managed schema file could not be found - load the non-managed schema
        try {
          schemaInputStream = loader.openResource(resourceName);
          loadedResource = resourceName;
          shouldUpgrade = true;
        } catch (Exception e) {
          try {
            // Retry to load the managed schema, in case it was created since the first attempt
            byte[] data = zkClient.getData(managedSchemaPath, null, stat, true);
            schemaZkVersion = stat.getVersion();
            schemaInputStream = new ByteArrayInputStream(data);
            loadedResource = managedSchemaPath;
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
      schema = new ManagedIndexSchema(config, loadedResource,IndexSchemaFactory.getConfigResource(configSetService, schemaInputStream, loader, managedSchemaResourceName) , isMutable,
              managedSchemaResourceName, schemaZkVersion, getSchemaUpdateLock());
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error loading parsing schema", e);
    }
    if (shouldUpgrade) {
      // Persist the managed schema if it doesn't already exist
      synchronized (schema.getSchemaUpdateLock()) {
        upgradeToManagedSchema();
      }
    }

    return schema;
  }

  private InputStream readSchemaLocally() {
    InputStream schemaInputStream = null;
    try {
      // Attempt to load the managed schema
      schemaInputStream = loader.openResource(managedSchemaResourceName);
      loadedResource = managedSchemaResourceName;
      warnIfNonManagedSchemaExists();
    } catch (IOException e) {
      log.info("The schema is configured as managed, but managed schema resource {}  not found - loading non-managed schema {} instead"
          , managedSchemaResourceName, resourceName);
    }
    if (null == schemaInputStream) {
      // The managed schema file could not be found - load the non-managed schema
      try {
        schemaInputStream = loader.openResource(resourceName);
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
        String nonManagedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + resourceName;
        try {
          exists = zkLoader.getZkController().pathExists(nonManagedSchemaPath);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
          log.warn("", e); // Log as warning and suppress the exception 
        } catch (KeeperException e) {
          // log as warning and suppress the exception
          log.warn("Error checking for the existence of the non-managed schema {}", resourceName, e);
        }
      } else { // Config is not in ZooKeeper
        InputStream nonManagedSchemaInputStream = null;
        try {
          nonManagedSchemaInputStream = loader.openResource(resourceName);
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
        log.warn("The schema has been upgraded to managed, but the non-managed schema {} is still loadable.  PLEASE REMOVE THIS FILE."
            , resourceName);
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
        log.info("On upgrading to managed schema, did not rename non-managed schema '{}' because it's the same as the managed schema's name."
            , resourceName);
      } else {
        final File nonManagedSchemaFile = locateConfigFile(resourceName);
        if (null == nonManagedSchemaFile) {
          // Don't throw an exception for failure to rename the non-managed schema
          log.warn("On upgrading to managed schema, did not rename non-managed schema {} {}{}{}"
              , resourceName
              , "because it's neither an absolute file "
              , "nor under SolrConfig.getConfigDir() or the current directory. "
              , "PLEASE REMOVE THIS FILE.");
        } else {
          File upgradedSchemaFile = new File(nonManagedSchemaFile + UPGRADED_SCHEMA_EXTENSION);
          if (nonManagedSchemaFile.renameTo(upgradedSchemaFile)) {
            // Set the resource name to the managed schema so that the CoreAdminHandler returns a findable filename 
            schema.setResourceName(managedSchemaResourceName);

            log.info("After upgrading to managed schema, renamed the non-managed schema {} to {}"
                , nonManagedSchemaFile, upgradedSchemaFile);
          } else {
            // Don't throw an exception for failure to rename the non-managed schema
            log.warn("Can't rename {} to {} - PLEASE REMOVE THIS FILE."
                , nonManagedSchemaFile, upgradedSchemaFile);
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
    String location = config.getResourceLoader().resourceLocation(resource);
    if (location == null || location.equals(resource) || location.startsWith("classpath:"))
      return null;
    return new File(location);
  }

  /**
   * Persist the managed schema to ZooKeeper and rename the non-managed schema 
   * by appending {@link #UPGRADED_SCHEMA_EXTENSION}.
   *
   * Failure to rename the non-managed schema will be logged as a warning,
   * and no exception will be thrown.
   */
  private void zkUgradeToManagedSchema() {
    if (resourceName.equals(managedSchemaResourceName)) {
      log.info("On upgrading to managed schema, did not rename non-managed schema {} because it's the same as the managed schema's name."
          , resourceName);
      return;
    }
    final ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
    final ZkController zkController = zkLoader.getZkController();
    final SolrZkClient zkClient = zkController.getZkClient();
    final String lockPath = zkLoader.getConfigSetZkPath() + "/schemaUpgrade.lock";
    boolean locked = false;
    try {
      try {
        zkClient.makePath(lockPath, null, CreateMode.EPHEMERAL, null, true, true);
        locked = true;
      } catch (Exception e) {
        // some other node already started the upgrade, or an error occurred - bail out
        return;
      }
      schema.persistManagedSchemaToZooKeeper(true); // Only create, don't update it if it already exists

      // After successfully persisting the managed schema, rename the non-managed
      // schema znode by appending UPGRADED_SCHEMA_EXTENSION to its name.

      // Rename the non-managed schema znode in ZooKeeper
      final String nonManagedSchemaPath = zkLoader.getConfigSetZkPath() + "/" + resourceName;
      try {
        ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(zkController.getClientTimeout());
        if (zkController.pathExists(nonManagedSchemaPath)) {
          // First, copy the non-managed schema znode content to the upgraded schema znode
          byte[] bytes = zkController.getZkClient().getData(nonManagedSchemaPath, null, null, true);
          final String upgradedSchemaPath = nonManagedSchemaPath + UPGRADED_SCHEMA_EXTENSION;
          zkCmdExecutor.ensureExists(upgradedSchemaPath, zkController.getZkClient());
          zkController.getZkClient().setData(upgradedSchemaPath, bytes, true);
          // Then delete the non-managed schema znode
          if (zkController.getZkClient().exists(nonManagedSchemaPath, true)) {
            try {
              zkController.getZkClient().delete(nonManagedSchemaPath, -1, true);
            } catch (KeeperException.NoNodeException ex) {
              // ignore - someone beat us to it
            }
          }

          // Set the resource name to the managed schema so that the CoreAdminHandler returns a findable filename
          schema.setResourceName(managedSchemaResourceName);

          log.info("After upgrading to managed schema in ZooKeeper, renamed the non-managed schema {} to {}"
              , nonManagedSchemaPath, upgradedSchemaPath);
        } else {
          log.info("After upgrading to managed schema in ZooKeeper, the non-managed schema {} no longer exists."
              , nonManagedSchemaPath);
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt(); // Restore the interrupted status
        }
        final String msg = "Error persisting managed schema resource " + managedSchemaResourceName;
        log.warn(msg, e); // Log as warning and suppress the exception
      }
    } finally {
      if (locked) {
        // unlock
        try {
          zkClient.delete(lockPath, -1, true);
        } catch (KeeperException.NoNodeException nne) {
          // ignore - someone else deleted it
        } catch (Exception e) {
          log.warn("Unable to delete schema upgrade lock file {}", lockPath, e);
        }
      }
    }
  }

  private Object schemaUpdateLock = new Object();
  public Object getSchemaUpdateLock() { return schemaUpdateLock; }

  @Override
  public void inform(SolrCore core) {
    this.core = core;
    if (loader instanceof ZkSolrResourceLoader) {
      this.zkIndexSchemaReader = new ZkIndexSchemaReader(this, core);
      ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)loader;
      zkLoader.setZkIndexSchemaReader(this.zkIndexSchemaReader);
      try {
        zkIndexSchemaReader.refreshSchemaFromZk(-1); // update immediately if newer is available
        core.setLatestSchema(getSchema());
      } catch (KeeperException e) {
        String msg = "Error attempting to access " + zkLoader.getConfigSetZkPath() + "/" + managedSchemaResourceName;
        log.error(msg, e);
        throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.warn("", e);
      }
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
  
  public boolean isMutable() {
    return isMutable;
  }

  public SolrConfig getConfig() {
    return config;
  }
}
