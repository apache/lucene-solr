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
package org.apache.solr.cloud;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrResourceNotFoundException;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ResourceLoader that works with ZooKeeper.
 *
 */
public class ZkSolrResourceLoader extends SolrResourceLoader {

  private final String configSetZkPath;
  private ZkController zkController;
  private ZkIndexSchemaReader zkIndexSchemaReader;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkSolrResourceLoader(Path instanceDir, String configSet, ZkController zooKeeperController) {
    super(instanceDir);
    this.zkController = zooKeeperController;
    configSetZkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet;
  }

  /**
   * <p>
   * This loader will first attempt to load resources from ZooKeeper, but if not found
   * will delegate to the context classloader when possible,
   * otherwise it will attempt to resolve resources using any jar files found in
   * the "lib/" directory in the specified instance directory.
   */
  public ZkSolrResourceLoader(Path instanceDir, String configSet, ClassLoader parent,
      Properties coreProperties, ZkController zooKeeperController) {
    super(instanceDir, parent, coreProperties);
    this.zkController = zooKeeperController;
    configSetZkPath = ZkConfigManager.CONFIGS_ZKNODE + "/" + configSet;
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
    InputStream is;
    String file = configSetZkPath + "/" + resource;
    int maxTries = 10;
    Exception exception = null;
    while (maxTries -- > 0) {
      try {
        if (zkController.pathExists(file)) {
          Stat stat = new Stat();
          byte[] bytes = zkController.getZkClient().getData(file, null, stat, true);
          return new ZkByteArrayInputStream(bytes, stat);
        } else {
          //Path does not exists. We only retry for session expired exceptions.
          break;
        }
      } catch (KeeperException.SessionExpiredException e) {
        exception = e;
        if (!zkController.getCoreContainer().isShutDown()) {
          // Retry in case of session expiry
          try {
            Thread.sleep(1000);
            log.debug("Sleeping for 1s before retrying fetching resource=" + resource);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Could not load resource=" + resource, ie);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Error opening " + file, e);
      } catch (Exception e) {
        throw new IOException("Error opening " + file, e);
      }
    }

    if (exception != null) {
      throw new IOException("We re-tried 10 times but was still unable to fetch resource=" + resource + " from ZK", exception);
    }

    try {
      // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
      is = classLoader.getResourceAsStream(resource.replace(File.separatorChar, '/'));
    } catch (Exception e) {
      throw new IOException("Error opening " + resource, e);
    }
    if (is == null) {
      throw new SolrResourceNotFoundException("Can't find resource '" + resource
          + "' in classpath or '" + configSetZkPath + "', cwd="
          + System.getProperty("user.dir"));
    }
    return is;
  }

  public static class ZkByteArrayInputStream extends ByteArrayInputStream{

    private final Stat stat;
    public ZkByteArrayInputStream(byte[] buf, Stat stat) {
      super(buf);
      this.stat = stat;

    }

    public Stat getStat(){
      return stat;
    }
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
      list = zkController.getZkClient().getChildren(configSetZkPath, null, true);
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

  public String getConfigSetZkPath() {
    return configSetZkPath;
  }
  
  public ZkController getZkController() {
    return zkController;
  }

  public void setZkIndexSchemaReader(ZkIndexSchemaReader zkIndexSchemaReader) {
    this.zkIndexSchemaReader = zkIndexSchemaReader;
  }

  public ZkIndexSchemaReader getZkIndexSchemaReader() { return zkIndexSchemaReader; }
}
