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

package org.apache.solr.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

/**
 * Extracts SolrCloud information from ZooKeeper.
 */
final class ZooKeeperInspector {
  
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperInspector.class);
  
  public List<List<String>> extractShardUrls(String zkHost, String collection) {

    DocCollection docCollection = extractDocCollection(zkHost, collection);
    List<Slice> slices = getSortedSlices(docCollection.getSlices());
    List<List<String>> solrUrls = new ArrayList<List<String>>(slices.size());
    for (Slice slice : slices) {
      if (slice.getLeader() == null) {
        throw new IllegalArgumentException("Cannot find SolrCloud slice leader. " +
            "It looks like not all of your shards are registered in ZooKeeper yet");
      }
      Collection<Replica> replicas = slice.getReplicas();
      List<String> urls = new ArrayList<String>(replicas.size());
      for (Replica replica : replicas) {
        ZkCoreNodeProps props = new ZkCoreNodeProps(replica);
        urls.add(props.getCoreUrl());
      }
      solrUrls.add(urls);
    }
    return solrUrls;
  }
  
  public DocCollection extractDocCollection(String zkHost, String collection) {
    if (collection == null) {
      throw new IllegalArgumentException("collection must not be null");
    }
    SolrZkClient zkClient = getZkClient(zkHost);
    
    try {
      ZkStateReader zkStateReader = new ZkStateReader(zkClient);
      try {
        // first check for alias
        collection = checkForAlias(zkClient, collection);
        zkStateReader.createClusterStateWatchersAndUpdate();
      } catch (Exception e) {
        throw new IllegalArgumentException("Cannot find expected information for SolrCloud in ZooKeeper: " + zkHost, e);
      }
      
      try {
        return zkStateReader.getClusterState().getCollection(collection);
      } catch (SolrException e) {
        throw new IllegalArgumentException("Cannot find collection '" + collection + "' in ZooKeeper: " + zkHost, e);
      }
    } finally {
      zkClient.close();
    }    
  }

  public SolrZkClient getZkClient(String zkHost) {
    if (zkHost == null) {
      throw new IllegalArgumentException("zkHost must not be null");
    }

    SolrZkClient zkClient;
    try {
      zkClient = new SolrZkClient(zkHost, 30000);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot connect to ZooKeeper: " + zkHost, e);
    }
    return zkClient;
  }
  
  public List<Slice> getSortedSlices(Collection<Slice> slices) {
    List<Slice> sorted = new ArrayList(slices);
    Collections.sort(sorted, new Comparator<Slice>() {
      @Override
      public int compare(Slice slice1, Slice slice2) {
        Comparator c = new AlphaNumericComparator();
        return c.compare(slice1.getName(), slice2.getName());
      }      
    });
    LOG.trace("Sorted slices: {}", sorted);
    return sorted;
  }

  /**
   * Returns config value given collection name
   * Borrowed heavily from Solr's ZKController.
   */
  public String readConfigName(SolrZkClient zkClient, String collection)
  throws KeeperException, InterruptedException {
    if (collection == null) {
      throw new IllegalArgumentException("collection must not be null");
    }
    String configName = null;

    // first check for alias
    collection = checkForAlias(zkClient, collection);
    
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection;
    if (LOG.isInfoEnabled()) {
      LOG.info("Load collection config from:" + path);
    }
    byte[] data = zkClient.getData(path, null, null, true);
    
    if(data != null) {
      ZkNodeProps props = ZkNodeProps.load(data);
      configName = props.getStr(ZkController.CONFIGNAME_PROP);
    }
    
    if (configName != null && !zkClient.exists(ZkController.CONFIGS_ZKNODE + "/" + configName, true)) {
      LOG.error("Specified config does not exist in ZooKeeper:" + configName);
      throw new IllegalArgumentException("Specified config does not exist in ZooKeeper:"
        + configName);
    }

    return configName;
  }

  private String checkForAlias(SolrZkClient zkClient, String collection)
      throws KeeperException, InterruptedException {
    byte[] aliasData = zkClient.getData(ZkStateReader.ALIASES, null, null, true);
    Aliases aliases = ClusterState.load(aliasData);
    String alias = aliases.getCollectionAlias(collection);
    if (alias != null) {
      List<String> aliasList = StrUtils.splitSmart(alias, ",", true);
      if (aliasList.size() > 1) {
        throw new IllegalArgumentException("collection cannot be an alias that maps to multiple collections");
      }
      collection = aliasList.get(0);
    }
    return collection;
  }

  /**
   * Download and return the config directory from ZK
   */
  public File downloadConfigDir(SolrZkClient zkClient, String configName)
  throws IOException, InterruptedException, KeeperException {
    File dir = Files.createTempDir();
    dir.deleteOnExit();
    ZkController.downloadConfigDir(zkClient, configName, dir);
    File confDir = new File(dir, "conf");
    if (!confDir.isDirectory()) {
      // create a temporary directory with "conf" subdir and mv the config in there.  This is
      // necessary because of CDH-11188; solrctl does not generate nor accept directories with e.g.
      // conf/solrconfig.xml which is necessary for proper solr operation.  This should work
      // even if solrctl changes.
      confDir = new File(Files.createTempDir().getAbsolutePath(), "conf");
      confDir.getParentFile().deleteOnExit();
      Files.move(dir, confDir);
      dir = confDir.getParentFile();
    }
    FileUtils.writeStringToFile(new File(dir, "solr.xml"), "<solr><cores><core name=\"collection1\" instanceDir=\".\" /></cores></solr>", "UTF-8");
    return dir;
  }

}
