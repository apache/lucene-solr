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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * A distributed map.
 * This supports basic map functions e.g. get, put, contains for interaction with zk which
 * don't have to be ordered i.e. DistributedQueue.
 */
public class DistributedMap {
  protected final String dir;

  protected SolrZkClient zookeeper;

  protected final String prefix = "mn-";

  public DistributedMap(SolrZkClient zookeeper, String dir) {
    this.dir = dir;

    ZkCmdExecutor cmdExecutor = new ZkCmdExecutor(zookeeper.getZkClientTimeout());
    try {
      cmdExecutor.ensureExists(dir, zookeeper);
    } catch (KeeperException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }

    this.zookeeper = zookeeper;
  }


  public void put(String trackingId, byte[] data) throws KeeperException, InterruptedException {
    zookeeper.makePath(dir + "/" + prefix + trackingId, data, CreateMode.PERSISTENT, null, false, true);
  }

  public byte[] get(String trackingId) throws KeeperException, InterruptedException {
    return zookeeper.getData(dir + "/" + prefix + trackingId, null, null, true);
  }

  public boolean contains(String trackingId) throws KeeperException, InterruptedException {
    return zookeeper.exists(dir + "/" + prefix + trackingId, true);
  }

  public int size() throws KeeperException, InterruptedException {
    Stat stat = new Stat();
    zookeeper.getData(dir, null, stat, true);
    return stat.getNumChildren();
  }

  /**
   * return true if the znode was successfully deleted
   *        false if the node didn't exist and therefore not deleted
   *        exception an exception occurred while deleting
   */
  public boolean remove(String trackingId) throws KeeperException, InterruptedException {
    try {
      zookeeper.delete(dir + "/" + prefix + trackingId, -1, true);
    } catch (KeeperException.NoNodeException e) {
      return false;
    }
    return true;
  }

  /**
   * Helper method to clear all child nodes for a parent node.
   */
  public void clear() throws KeeperException, InterruptedException {
    List<String> childNames = zookeeper.getChildren(dir, null, true);
    for(String childName: childNames) {
      zookeeper.delete(dir + "/" + childName, -1, true);
    }

  }

}
