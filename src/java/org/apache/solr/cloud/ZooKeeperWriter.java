package org.apache.solr.cloud;

/**
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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ZooKeeperWriter {
  private static Logger log = LoggerFactory.getLogger(ZooKeeperWriter.class);

  private ZooKeeperConnection keeperConnection;

  private boolean closeKeeper;

  /**
   * For testing. For regular use see {@link #ZooKeeperWriter(ZooKeeper)}.
   * 
   * @param zooKeeperHost
   * @param zkClientTimeout
   * @throws IOException
   * @throws InterruptedException
   * @throws TimeoutException
   */
  ZooKeeperWriter(String zooKeeperHost, int zkClientTimeout)
      throws IOException, InterruptedException, TimeoutException {
    closeKeeper = true;
    
    keeperConnection = new ZooKeeperConnection(zooKeeperHost, zkClientTimeout);
    keeperConnection.connect();
  }


  /**
   * @param keeperConnection
   */
  ZooKeeperWriter(ZooKeeperConnection keeperConnection) {
    this.keeperConnection = keeperConnection;
  }

  /**
   * Close underling ZooKeeper client if this owns it.
   * 
   * Only for tests.
   * 
   * @throws InterruptedException
   */
  public void close() throws InterruptedException {
    if (closeKeeper) {
      keeperConnection.close();
    }
  }

  /**
   * 
   * @param path
   * @param data
   * @param watcher
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public String makeEphemeralSeqPath(String path, byte[] data,
      Watcher watcher) throws KeeperException, InterruptedException {

    String zkPath = keeperConnection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.EPHEMERAL_SEQUENTIAL);
    
    keeperConnection.exists(zkPath, watcher);
    
    return zkPath;
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path) throws KeeperException,
      InterruptedException {
    makePath(path, null, CreateMode.PERSISTENT);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data) throws KeeperException,
      InterruptedException {
    makePath(path, data, CreateMode.PERSISTENT);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @param createMode
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data, CreateMode createMode)
      throws KeeperException, InterruptedException {
    makePath(path, data, createMode, null);
  }

  /**
   * Creates the path in ZooKeeper, creating each node as necessary.
   * 
   * e.g. If <code>path=/solr/group/node</code> and none of the nodes, solr,
   * group, node exist, each will be created.
   * 
   * @param path
   * @param data to set on the last zkNode
   * @param createMode
   * @param watcher
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String path, byte[] data, CreateMode createMode,
      Watcher watcher) throws KeeperException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info("makePath: " + path);
    }

    if (path.startsWith("/")) {
      path = path.substring(1, path.length());
    }
    String[] paths = path.split("/");
    StringBuilder sbPath = new StringBuilder();
    for (int i = 0; i < paths.length; i++) {
      byte[] bytes = null;
      String pathPiece = paths[i];
      sbPath.append("/" + pathPiece);
      String currentPath = sbPath.toString();
      Object exists = keeperConnection.exists(currentPath, watcher);
      if (exists == null) {
        CreateMode mode = CreateMode.PERSISTENT;
        if (i == paths.length - 1) {
          mode = createMode;
          bytes = data;
        }
        keeperConnection.create(currentPath, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
        // set new watch
        keeperConnection.exists(currentPath, watcher);
      } else if (i == paths.length - 1) {
        // nocommit: version ?
        keeperConnection.setData(currentPath, data, -1);
        // set new watch
        keeperConnection.exists(currentPath, watcher);
      }
    }
  }

  /**
   * @param zkPath
   * @param createMode
   * @param watcher
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void makePath(String zkPath, CreateMode createMode, Watcher watcher)
      throws KeeperException, InterruptedException {
    makePath(zkPath, null, createMode, watcher);
  }

  /**
   * Write data to ZooKeeper.
   * 
   * @param path
   * @param data
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void write(String path, byte[] data) throws KeeperException,
      InterruptedException {

    makePath(path);

    Object exists = keeperConnection.exists(path, null);
    if (exists != null) {
      keeperConnection.setData(path, data, -1);
    } else {
      keeperConnection.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT);
    }
  }

  /**
   * Write file to ZooKeeper - default system encoding used.
   * 
   * @param path path to upload file to e.g. /solr/conf/solrconfig.xml
   * @param file path to file to be uploaded
   * @throws IOException
   * @throws KeeperException
   * @throws InterruptedException
   */
  public void write(String path, File file) throws IOException,
      KeeperException, InterruptedException {
    if(log.isInfoEnabled()) {
      log.info("Write to ZooKeepeer " + file.getAbsolutePath() + " to " + path);
    }

    String data = FileUtils.readFileToString(file);
    write(path, data.getBytes());
  }
  
}
