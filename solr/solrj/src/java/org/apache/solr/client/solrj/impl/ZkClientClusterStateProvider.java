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
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZkClientClusterStateProvider implements CloudSolrClient.ClusterStateProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  ZkStateReader zkStateReader;
  String zkHost;
  int zkConnectTimeout = 10000;
  int zkClientTimeout = 10000;

  public ZkClientClusterStateProvider(Collection<String> zkHosts, String chroot) {
    zkHost = buildZkHostString(zkHosts,chroot);
  }

  public ZkClientClusterStateProvider(String zkHost){
    this.zkHost = zkHost;
  }

  @Override
  public ClusterState.CollectionRef getState(String collection) {
    return zkStateReader.getClusterState().getCollectionRef(collection);
  }

  @Override
  public Set<String> liveNodes() {
    return zkStateReader.getClusterState().getLiveNodes();
  }


  @Override
  public String getAlias(String collection) {
    Aliases aliases = zkStateReader.getAliases();
    return aliases.getCollectionAlias(collection);
  }

  @Override
  public Map<String, Object> getClusterProperties() {
    return zkStateReader.getClusterProperties();
  }

  @Override
  public String getCollectionName(String name) {
    Aliases aliases = zkStateReader.getAliases();
    if (aliases != null) {
      Map<String, String> collectionAliases = aliases.getCollectionAliasMap();
      if (collectionAliases != null && collectionAliases.containsKey(name)) {
        name = collectionAliases.get(name);
      }
    }
    return name;
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
    if (zkStateReader != null) {
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
