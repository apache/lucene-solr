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

import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class ZkClientClusterStateProvider implements ClusterStateProvider, Replica.NodeNameToBaseUrl {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  volatile ZkStateReader zkStateReader;
  private boolean closeZkStateReader = false;
  final String zkHost;
  int zkConnectTimeout = 15000;
  int zkClientTimeout = 45000;

  private volatile boolean isClosed = false;

  ZkClientClusterStateProvider(String zkServerAddress, int zkClientTimeout, int zkClientConnectTimeout) {
    this.zkHost = zkServerAddress;
    this.zkClientTimeout = zkClientTimeout;
    this.zkConnectTimeout = zkClientConnectTimeout;
    this.closeZkStateReader = true;
  }

  public ZkClientClusterStateProvider(ZkStateReader zkStateReader) {
    this(zkStateReader, false);
  }

  public ZkClientClusterStateProvider(ZkStateReader zkStateReader, boolean closeReader) {
    this.zkStateReader = zkStateReader;
    this.zkHost = zkStateReader.getZkClient().getZkServerAddress();
    this.closeZkStateReader = closeReader;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    ClusterState clusterState = getZkStateReader().getClusterState();
    if (clusterState != null) {
      return clusterState.getCollectionRef(collection);
    } else {
      return null;
    }
  }
  
  @Override
  public Set<String> getLiveNodes() {
    return getZkStateReader().getLiveNodes();
  }


  @Override
  public List<String> resolveAlias(String alias) {
    return getZkStateReader().getAliases().resolveAliases(alias); // if not an alias, returns itself
  }

  @Override
  public Map<String, String> getAliasProperties(String alias) {
    return getZkStateReader().getAliases().getCollectionAliasProperties(alias);
  }

  @Override
  public String resolveSimpleAlias(String alias) throws IllegalArgumentException {
    return getZkStateReader().getAliases().resolveSimpleAlias(alias);
  }

  @Override
  public Object getClusterProperty(String propertyName) {
    Map<String, Object> props = getZkStateReader().getClusterProperties();
    return props.get(propertyName);
  }

  @Override
  public <T> T getClusterProperty(String propertyName, T def) {
    Map<String, Object> props = getZkStateReader().getClusterProperties();
    if (props.containsKey(propertyName)) {
      return (T)props.get(propertyName);
    }
    return def;
  }

  @Override
  public ClusterState getClusterState() throws IOException {
    return getZkStateReader().getClusterState();
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return getZkStateReader().getClusterProperties();
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
    getZkStateReader().getConfigManager().downloadConfigDir(configName, downloadPath);
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
  public void uploadConfig(Path configPath, String configName) throws IOException, KeeperException {
    getZkStateReader().getConfigManager().uploadConfigDir(configPath, configName);
  }

  @Override
  public void connect() {
    if (isClosed) {
      throw new AlreadyClosedException();
    }
    if (this.zkStateReader == null) {
      boolean createWatchers = false;
      synchronized (this) {
        if (this.zkStateReader == null) {
          this.zkStateReader = new ZkStateReader(zkHost, zkClientTimeout, zkConnectTimeout);
          createWatchers = true;
        }
      }
      if (createWatchers)  {
        this.zkStateReader.createClusterStateWatchersAndUpdate();
      }
    }
  }

  public ZkStateReader getZkStateReader() {
    if (isClosed) {
      throw new AlreadyClosedException();
    }

    if (zkStateReader == null) {
      synchronized (this) {
        if (zkStateReader == null) {
          connect();
        }
      }
    }
    return zkStateReader;
  }
  
  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }

    final ZkStateReader zkToClose = zkStateReader;
    if (zkToClose != null && closeZkStateReader) {
      IOUtils.closeQuietly(zkToClose);
    }
    isClosed = true;
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
    log.debug("Final constructed zkHost string: {}", zkHostString);
    return zkHostString;
  }

  @Override
  public String toString() {
    return zkHost;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public String getBaseUrlForNodeName(final String nodeName) {
    if (nodeName == null) throw new NullPointerException("The nodeName cannot be null");
    return Utils.getBaseUrlForNodeName(nodeName,
        getClusterProperty(ZkStateReader. URL_SCHEME, "http"));
  }
}
