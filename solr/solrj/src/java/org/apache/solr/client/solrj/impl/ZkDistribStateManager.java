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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.solr.client.solrj.cloud.autoscaling.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.BadVersionException;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.autoscaling.NotEmptyException;
import org.apache.solr.client.solrj.cloud.autoscaling.VersionedData;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import static org.apache.solr.common.util.Utils.fromJSON;

/**
 * Implementation of {@link DistribStateManager} that uses Zookeeper.
 */
public class ZkDistribStateManager implements DistribStateManager {

  private final SolrZkClient zkClient;

  public ZkDistribStateManager(SolrZkClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public boolean hasData(String path) throws IOException, KeeperException, InterruptedException {
    try {
      return zkClient.exists(path, true);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public List<String> listData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    try {
      return zkClient.getChildren(path, watcher, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public List<String> listData(String path) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    return listData(path, null);
  }

  @Override
  public VersionedData getData(String path, Watcher watcher) throws NoSuchElementException, IOException, KeeperException, InterruptedException {
    Stat stat = new Stat();
    try {
      byte[] bytes = zkClient.getData(path, watcher, stat, true);
      return new VersionedData(stat.getVersion(), bytes,
          stat.getEphemeralOwner() != 0 ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT,
          String.valueOf(stat.getEphemeralOwner()));
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public void makePath(String path) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    try {
      zkClient.makePath(path, true);
    } catch (KeeperException.NodeExistsException e) {
      throw new AlreadyExistsException(path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public void makePath(String path, byte[] data, CreateMode createMode, boolean failOnExists) throws AlreadyExistsException, IOException, KeeperException, InterruptedException {
    try {
      zkClient.makePath(path, data, createMode, null, failOnExists, true);
    } catch (KeeperException.NodeExistsException e) {
      throw new AlreadyExistsException(path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public String createData(String path, byte[] data, CreateMode mode) throws NoSuchElementException, AlreadyExistsException, IOException, KeeperException, InterruptedException {
    try {
      return zkClient.create(path, data, mode, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException.NodeExistsException e) {
      throw new AlreadyExistsException(path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public void removeData(String path, int version) throws NoSuchElementException, BadVersionException, NotEmptyException, IOException, KeeperException, InterruptedException {
    try {
      zkClient.delete(path, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException.NotEmptyException e) {
      throw new NotEmptyException(path);
    } catch (KeeperException.BadVersionException e) {
      throw new BadVersionException(version, path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public void setData(String path, byte[] data, int version) throws BadVersionException, NoSuchElementException, IOException, KeeperException, InterruptedException {
    try {
      zkClient.setData(path, data, version, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(path);
    } catch (KeeperException.BadVersionException e) {
      throw new BadVersionException(version, path);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  public List<OpResult> multi(Iterable<Op> ops) throws BadVersionException, AlreadyExistsException, NoSuchElementException, IOException, KeeperException, InterruptedException {
    try {
      return zkClient.multi(ops, true);
    } catch (KeeperException.NoNodeException e) {
      throw new NoSuchElementException(ops.toString());
    } catch (KeeperException.NodeExistsException e) {
      throw new AlreadyExistsException(ops.toString());
    } catch (KeeperException.BadVersionException e) {
      throw new BadVersionException(-1, ops.toString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AlreadyClosedException();
    }
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public AutoScalingConfig getAutoScalingConfig(Watcher watcher) throws InterruptedException, IOException {
    Map<String, Object> map = new HashMap<>();
    Stat stat = new Stat();
    try {
      byte[] bytes = zkClient.getData(ZkStateReader.SOLR_AUTOSCALING_CONF_PATH, watcher, stat, true);
      if (bytes != null && bytes.length > 0) {
        map = (Map<String, Object>) fromJSON(bytes);
      }
    } catch (KeeperException.NoNodeException e) {
      // ignore
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    map.put(AutoScalingParams.ZK_VERSION, stat.getVersion());
    return new AutoScalingConfig(map);
  }

  @Override
  public void close() throws IOException {

  }

  public SolrZkClient getZkClient() {
    return zkClient;
  }

  @Override
  public PerReplicaStates getReplicaStates(String path) throws KeeperException, InterruptedException {
    return PerReplicaStates.fetch(path, zkClient, null);
  }
}
