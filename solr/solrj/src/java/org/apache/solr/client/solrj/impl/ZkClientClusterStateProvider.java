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
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkClientClusterStateProvider implements ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  volatile ZkStateReader zkStateReader;
  private boolean closeZkStateReader = true;
  String zkHost;
  int zkConnectTimeout = 15000;
  int zkClientTimeout = 45000;


  private volatile boolean isClosed = false;

  public ZkClientClusterStateProvider(ZkStateReader zkStateReader) {
    this.zkStateReader = zkStateReader;
    this.closeZkStateReader =  false;
  }

  public ZkClientClusterStateProvider(Collection<String> zkHosts, String chroot) {
    zkHost = buildZkHostString(zkHosts,chroot);
  }

  public ZkClientClusterStateProvider(String zkHost){
    this.zkHost = zkHost;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    ClusterState clusterState = zkStateReader.getClusterState();
    if (clusterState != null) {
      return clusterState.getCollectionRef(collection);
    } else {
      return null;
    }
  }
  public ZkStateReader getZkStateReader(){
    return zkStateReader;
  }

  @Override
  public Set<String> getLiveNodes() {
    if (isClosed) throw new AlreadyClosedException();
    ClusterState clusterState = zkStateReader.getClusterState();
    if (clusterState != null) {
      return clusterState.getLiveNodes();
    } else {
      return Collections.emptySet();
    }
  }


  @Override
  public List<String> resolveAlias(String alias) {
    return zkStateReader.getAliases().resolveAliases(alias); // if not an alias, returns itself
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    Map<String, Object> props = zkStateReader.getClusterProperties();
    return props.get(propertyName);
  }

  @Override
  public <T> T getClusterProperty(String propertyName, T def) {
    Map<String, Object> props = zkStateReader.getClusterProperties();
    if (props.containsKey(propertyName)) {
      return (T)props.get(propertyName);
    }
    return def;
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return zkStateReader.getClusterState();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return zkStateReader.getClusterProperties();
  }

  @Override
  public String getPolicyNameByCollection(String coll) {
    ClusterState.CollectionRef state = getState(coll);
    return state == null || state.get() == null ? null : (String) state.get().getProperties().get("policy");
  }

  /**
   * Download a named config from Zookeeper to a location on the filesystem
   * @param configName    the name of the config
   * @param downloadPath  the path to write config files to
   * @throws IOException  if an I/O exception occurs
   */
  public void downloadConfig(String configName, Path downloadPath) throws IOException {
    connect();
    zkStateReader.getConfigManager().downloadConfigDir(configName, downloadPath);
  }

  /**
   * Upload a set of config files to Zookeeper and give it a name
   *
   * NOTE: You should only allow trusted users to upload configs.  If you
   * are allowing client access to zookeeper, you should protect the
   * /configs node against unauthorised write access.
   *
   * @param configPath {@link java.nio.file.Path} to the config files
   * @param configName the name of the config
   * @throws IOException if an IO error occurs
   */
  public void uploadConfig(Path configPath, String configName) throws IOException {
    connect();
    zkStateReader.getConfigManager().uploadConfigDir(configPath, configName);
  }

  @Override
  public void connect() {
    if (zkStateReader == null) {
      synchronized (this) {
        if (zkStateReader == null) {
          ZkStateReader zk = null;
          try {
            zk = new ZkStateReader(zkHost, zkClientTimeout, zkConnectTimeout);
            zk.createClusterStateWatchersAndUpdate();
            zkStateReader = zk;
            log.info("Cluster at {} ready", zkHost);
          } catch (InterruptedException e) {
            zk.close();
            Thread.currentThread().interrupt();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
          } catch (KeeperException e) {
            zk.close();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "", e);
          } catch (Exception e) {
            if (zk != null) zk.close();
            // do not wrap because clients may be relying on the underlying exception being thrown
            throw e;
          }
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    isClosed  = true;
    if (zkStateReader != null && closeZkStateReader) {
      synchronized (this) {
        if (zkStateReader != null)
          zkStateReader.close();
        zkStateReader = null;
      }
    }
  }


  static String buildZkHostString(Collection<String> zkHosts, String chroot) {
    if (zkHosts == null || zkHosts.isEmpty()) {
      throw new IllegalArgumentException("Cannot create CloudSearchClient without valid ZooKeeper host; none specified!");
    }

    StringBuilder zkBuilder = new StringBuilder();
    int lastIndexValue = zkHosts.size() - 1;
    int i = 0;
    for (String zkHost : zkHosts) {
      zkBuilder.append(zkHost);
      if (i < lastIndexValue) {
        zkBuilder.append(",");
      }
      i++;
    }
    if (chroot != null) {
      if (chroot.startsWith("/")) {
        zkBuilder.append(chroot);
      } else {
        throw new IllegalArgumentException(
            "The chroot must start with a forward slash.");
      }
    }

    /* Log the constructed connection string and then initialize. */
    final String zkHostString = zkBuilder.toString();
    log.debug("Final constructed zkHost string: " + zkHostString);
    return zkHostString;
  }

  @Override
  public String toString() {
    return zkHost;
  }
}
