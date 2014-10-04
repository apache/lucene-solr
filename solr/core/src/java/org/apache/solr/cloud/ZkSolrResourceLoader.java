package org.apache.solr.cloud;

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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.zookeeper.KeeperException;

/**
 * ResourceLoader that works with ZooKeeper.
 *
 */
public class ZkSolrResourceLoader extends SolrResourceLoader {

  private final String collectionZkPath;
  private ZkController zkController;
  private ZkIndexSchemaReader zkIndexSchemaReader;

  public ZkSolrResourceLoader(String instanceDir, String collection,
      ZkController zooKeeperController) {
    super(instanceDir);
    this.zkController = zooKeeperController;
    collectionZkPath = ZkController.CONFIGS_ZKNODE + "/" + collection;
  }

  /**
   * <p>
   * This loader will first attempt to load resources from ZooKeeper, but if not found
   * will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files found in
   * the "lib/" directory in the specified instance directory.
   * <p>
   */
  public ZkSolrResourceLoader(String instanceDir, String collection, ClassLoader parent,
      Properties coreProperties, ZkController zooKeeperController) {
    super(instanceDir, parent, coreProperties);
    this.zkController = zooKeeperController;
    collectionZkPath = ZkController.CONFIGS_ZKNODE + "/" + collection;
  }

  /**
   * Opens any resource by its name. By default, this will look in multiple
   * locations to load the resource: $configDir/$resource from ZooKeeper.
   * It will look for it in any jar
   * accessible through the class loader if it cannot be found in ZooKeeper. 
   * Override this method to customize loading resources.
   * 
   * @return the stream for the named resource
   */
  @Override
  public InputStream openResource(String resource) throws IOException {
    InputStream is = null;
    String file = collectionZkPath + "/" + resource;
    try {
      if (zkController.pathExists(file)) {
        byte[] bytes = zkController.getZkClient().getData(file, null, null, true);
        return new ByteArrayInputStream(bytes);
      }
    } catch (Exception e) {
      throw new IOException("Error opening " + file, e);
    }
    try {
      // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
      is = classLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));
    } catch (Exception e) {
      throw new IOException("Error opening " + resource, e);
    }
    if (is == null) {
      throw new IOException("Can't find resource '" + resource
          + "' in classpath or '" + collectionZkPath + "', cwd="
          + System.getProperty("user.dir"));
    }
    return is;
  }

  @Override
  public String getConfigDir() {
    throw new ZooKeeperException(
        ErrorCode.SERVER_ERROR,
        "ZkSolrResourceLoader does not support getConfigDir() - likely, what you are trying to do is not supported in ZooKeeper mode");
  }
  
  @Override
  public String[] listConfigDir() {
    List<String> list;
    try {
      list = zkController.getZkClient().getChildren(collectionZkPath, null, true);
    } catch (InterruptedException e) {
      // Restore the interrupted status
      Thread.currentThread().interrupt();
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    } catch (KeeperException e) {
      log.error("", e);
      throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
          "", e);
    }
    return list.toArray(new String[0]);
  }

  public String getCollectionZkPath() {
    return collectionZkPath;
  }
  
  public ZkController getZkController() {
    return zkController;
  }

  public void setZkIndexSchemaReader(ZkIndexSchemaReader zkIndexSchemaReader) {
    this.zkIndexSchemaReader = zkIndexSchemaReader;
  }

  public ZkIndexSchemaReader getZkIndexSchemaReader() { return zkIndexSchemaReader; }
}
