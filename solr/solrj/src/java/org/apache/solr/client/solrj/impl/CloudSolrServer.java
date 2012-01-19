package org.apache.solr.client.solrj.impl;

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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.CloudState;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.zookeeper.KeeperException;

public class CloudSolrServer extends SolrServer {
  private volatile ZkStateReader zkStateReader;
  private String zkHost; // the zk server address
  private int zkConnectTimeout = 10000;
  private int zkClientTimeout = 10000;
  private String defaultCollection;
  private LBHttpSolrServer lbServer;
  Random rand = new Random();
  private MultiThreadedHttpConnectionManager connManager;
  /**
   * @param zkHost The address of the zookeeper quorum containing the cloud state
   */
  public CloudSolrServer(String zkHost) throws MalformedURLException {
      connManager = new MultiThreadedHttpConnectionManager();
      this.zkHost = zkHost;
      this.lbServer = new LBHttpSolrServer(new HttpClient(connManager));
  }

  /**
   * @param zkHost The address of the zookeeper quorum containing the cloud state
   */
  public CloudSolrServer(String zkHost, LBHttpSolrServer lbServer) {
    this.zkHost = zkHost;
    this.lbServer = lbServer;
  }

  /** Sets the default collection for request */
  public void setDefaultCollection(String collection) {
    this.defaultCollection = collection;
  }

  /** Set the connect timeout to the zookeeper ensemble in ms */
  public void setZkConnectTimeout(int zkConnectTimeout) {
    this.zkConnectTimeout = zkConnectTimeout;
  }

  /** Set the timeout to the zookeeper ensemble in ms */
  public void setZkClientTimeout(int zkClientTimeout) {
    this.zkClientTimeout = zkClientTimeout;
  }

  /**
   * Connect to the zookeeper ensemble.
   * This is an optional method that may be used to force a connect before any other requests are sent.
   *
   * @throws IOException
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public void connect() {
    if (zkStateReader == null) {
      synchronized (this) {
        if (zkStateReader == null) {
          try {
            ZkStateReader zk = new ZkStateReader(zkHost, zkConnectTimeout,
                zkClientTimeout);
            zk.createClusterStateWatchersAndUpdate();
            zkStateReader = zk;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (KeeperException e) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (IOException e) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          } catch (TimeoutException e) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          }
        }
      }
    }
  }

  @Override
  public NamedList<Object> request(SolrRequest request) throws SolrServerException, IOException {
    connect();

    // TODO: if you can hash here, you could favor the shard leader
    
    CloudState cloudState = zkStateReader.getCloudState();

    SolrParams reqParams = request.getParams();
    if (reqParams == null) {
      reqParams = new ModifiableSolrParams();
    }
    String collection = reqParams.get("collection", defaultCollection);
    
    // Extract each comma separated collection name and store in a List.
    List<String> collectionList = StrUtils.splitSmart(collection, ",", true);
    
    // Retrieve slices from the cloud state and, for each collection specified,
    // add it to the Map of slices.
    Map<String,Slice> slices = new HashMap<String,Slice>();
    for (int i = 0; i < collectionList.size(); i++) {
      String coll= collectionList.get(i);
      ClientUtils.appendMap(coll, slices, cloudState.getSlices(coll));
    }

    Set<String> liveNodes = cloudState.getLiveNodes();

    // IDEA: have versions on various things... like a global cloudState version
    // or shardAddressVersion (which only changes when the shards change)
    // to allow caching.

    // build a map of unique nodes
    // TODO: allow filtering by group, role, etc
    Map<String,ZkNodeProps> nodes = new HashMap<String,ZkNodeProps>();
    List<String> urlList = new ArrayList<String>();
    for (Slice slice : slices.values()) {
      for (ZkNodeProps nodeProps : slice.getShards().values()) {
        ZkCoreNodeProps coreNodeProps = new ZkCoreNodeProps(nodeProps);
        String node = coreNodeProps.getNodeName();
        if (!liveNodes.contains(coreNodeProps.getNodeName())
            || !coreNodeProps.getState().equals(
                ZkStateReader.ACTIVE)) continue;
        if (nodes.put(node, nodeProps) == null) {
          String url = coreNodeProps.getCoreUrl();
          urlList.add(url);
        }
      }
    }

    Collections.shuffle(urlList, rand);
    //System.out.println("########################## MAKING REQUEST TO " + urlList);
 
    LBHttpSolrServer.Req req = new LBHttpSolrServer.Req(request, urlList);
    LBHttpSolrServer.Rsp rsp = lbServer.request(req);
    return rsp.getResponse();
  }

  public void close() {
    if (zkStateReader != null) {
      synchronized(this) {
        if (zkStateReader!= null)
          zkStateReader.close();
        zkStateReader = null;
      }
    }
    if (connManager != null) {
      connManager.shutdown();
    }
  }

  public LBHttpSolrServer getLbServer() {
    return lbServer;
  }
}
