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
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;

public class ManagedIndexSchemaFactory extends IndexSchemaFactory {
  private static final Logger log = LoggerFactory.getLogger(ManagedIndexSchemaFactory.class);
  private static final String UPGRADED_SCHEMA_EXTENSION = ".bak";

  private boolean isMutable;
  private String managedSchemaResourceName;
  private SolrConfig config;
  private SolrResourceLoader loader;
  private String resourceName;
  private IndexSchema schema;

  @Override
  public void init(NamedList args) {
    SolrParams params = SolrParams.toSolrParams(args);
    isMutable = params.getBool("mutable", false);
    args.remove("mutable");
    managedSchemaResourceName = params.get("managedSchemaResourceName", "managed-schema");
    args.remove("managedSchemaResourceName");
    if ("schema.xml".equals(managedSchemaResourceName)) {
      String msg = "managedSchemaResourceName can't be 'schema.xml'";
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
  public IndexSchema create(String resourceName, SolrConfig config) {
    this.resourceName = resourceName;
    this.config = config;
    SolrResourceLoader loader = config.getResourceLoader();
    this.loader = loader;
    InputStream schemaInputStream = null;
    boolean shouldUpgrade = false;
    String loadedResource = null;

    if (null == resourceName) {
      resourceName = IndexSchema.DEFAULT_SCHEMA_FILE;
    }

    try {
      // Attempt to load the managed schema
      schemaInputStream = loader.openSchema(managedSchemaResourceName);
      loadedResource = managedSchemaResourceName;

      // Check if the non-managed schema is also present
      if ( ! resourceName.equals(managedSchemaResourceName)) {
        if (nonManagedSchemaExists()) {
          // Warn if the non-managed schema is present
          log.warn("The schema has been upgraded to managed, but the non-managed schema " + resourceName
              + " is still loadable.  PLEASE REMOVE THIS FILE.");
        }
      }
    } catch (IOException e) {
      log.info("SolrConfig.isManagedSchema = true, but managed schema resource " + managedSchemaResourceName
          + " not found - loading non-managed schema " + resourceName + " instead");
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
          schemaInputStream = loader.openSchema(managedSchemaResourceName);
          loadedResource = managedSchemaResourceName;
        } catch (IOException e1) {
          final String msg = "Error loading both non-managed schema '" + resourceName + "' and managed schema '"
                           + managedSchemaResourceName + "'";
          log.error(msg, e);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
        }
      }
    }
    InputSource inputSource = new InputSource(schemaInputStream);
    inputSource.setSystemId(SystemIdResolver.createSystemIdFromResourceName(loadedResource));
    schema = new ManagedIndexSchema(config, loadedResource, inputSource, isMutable);

    if (shouldUpgrade) {
      // Persist the managed schema if it doesn't already exist
      upgradeToManagedSchema();
    }
    return schema;
  }

  /**
   * Return whether a non-managed schema exists, either in local storage or on ZooKeeper. 
   */
  private boolean nonManagedSchemaExists() {
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
    return exists;
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
      File managedSchemaFile = new File(loader.getConfigDir(), managedSchemaResourceName);
      OutputStreamWriter writer = null;
      try {
        File parentDir = managedSchemaFile.getParentFile();
        if (!parentDir.isDirectory()) {
          if (!parentDir.mkdirs()) {
            final String msg = "Can't create managed schema directory " + parentDir.getAbsolutePath();
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg);
          }
        }
        final FileOutputStream out = new FileOutputStream(managedSchemaFile);
        writer = new OutputStreamWriter(out, "UTF-8");
        schema.persist(writer);
        log.info("Upgraded to managed schema at " + managedSchemaFile.getPath());
      } catch (IOException e) {
        final String msg = "Error persisting managed schema " + managedSchemaFile;
        log.error(msg, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
      } finally {
        IOUtils.closeQuietly(writer);
        try {
          FileUtils.sync(managedSchemaFile);
        } catch (IOException e) {
          final String msg = "Error syncing the managed schema file " + managedSchemaFile;
          log.error(msg, e);
        }
      }

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
    ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader)config.getResourceLoader();
    ZkCmdExecutor zkCmdExecutor = new ZkCmdExecutor(30);
    ZkController zkController = zkLoader.getZkController();
    final String managedSchemaPath = zkLoader.getCollectionZkPath() + "/" + managedSchemaResourceName;
    try {
      // Create the managed schema znode
      zkCmdExecutor.ensureExists(managedSchemaPath, zkController.getZkClient());
      // Persist the managed schema
      StringWriter writer = new StringWriter();
      schema.persist(writer);
      zkController.getZkClient().setData(managedSchemaPath, writer.toString().getBytes("UTF-8"), true);
      log.info("Upgraded to managed schema at " + managedSchemaPath + "");
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt(); // Restore the interrupted status
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
      } else {
        final String msg = "Error persisting managed schema resource " + managedSchemaResourceName;
        log.error(msg, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, msg, e);
      }
    }

    // After successfully persisting the managed schema, rename the non-managed
    // schema znode by appending UPGRADED_SCHEMA_EXTENSION to its name.

    if (resourceName.equals(managedSchemaResourceName)) {
      log.info("On upgrading to managed schema, did not rename non-managed schema "
          + resourceName + " because it's the same as the managed schema's name.");
    } else {
      // Rename the non-managed schema znode in ZooKeeper
      final String nonManagedSchemaPath = zkLoader.getCollectionZkPath() + "/" + resourceName;
      try {
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
          log.warn("", e); // Log as warning and suppress the exception 
        } else {
          final String msg = "Error persisting managed schema resource " + managedSchemaResourceName;
          log.warn(msg, e); // Log as warning and suppress the exception
        }
      }
    }
  }
}
