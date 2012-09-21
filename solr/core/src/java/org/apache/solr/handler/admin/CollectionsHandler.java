package org.apache.solr.handler.admin;

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

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestSyncShard;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerCollectionProcessor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CollectionsHandler extends RequestHandlerBase {
  protected static Logger log = LoggerFactory.getLogger(CollectionsHandler.class);
  protected final CoreContainer coreContainer;

  public CollectionsHandler() {
    super();
    // Unlike most request handlers, CoreContainer initialization 
    // should happen in the constructor...  
    this.coreContainer = null;
  }


  /**
   * Overloaded ctor to inject CoreContainer into the handler.
   *
   * @param coreContainer Core Container of the solr webapp installed.
   */
  public CollectionsHandler(final CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }


  @Override
  final public void init(NamedList args) {

  }

  /**
   * The instance of CoreContainer this handler handles. This should be the CoreContainer instance that created this
   * handler.
   *
   * @return a CoreContainer instance
   */
  public CoreContainer getCoreContainer() {
    return this.coreContainer;
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    // Make sure the cores is enabled
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Core container instance missing");
    }

    // Pick the action
    SolrParams params = req.getParams();
    CollectionAction action = null;
    String a = params.get(CoreAdminParams.ACTION);
    if (a != null) {
      action = CollectionAction.get(a);
    }
    if (action != null) {
      switch (action) {
        case CREATE: {
          this.handleCreateAction(req, rsp);
          break;
        }
        case DELETE: {
          this.handleDeleteAction(req, rsp);
          break;
        }
        case RELOAD: {
          this.handleReloadAction(req, rsp);
          break;
        }
        case SYNCSHARD: {
          this.handleSyncShardAction(req, rsp);
          break;
        }
        
        default: {
          throw new RuntimeException("Unknown action: " + action);
        }
      }
    }

    rsp.setHttpCaching(false);
  }

  private void handleReloadAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Reloading Collection : " + req.getParamString());
    String name = req.getParams().required().get("name");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        OverseerCollectionProcessor.RELOADCOLLECTION, "name", name);

    // TODO: what if you want to block until the collection is available?
    coreContainer.getZkController().getOverseerCollectionQueue().offer(ZkStateReader.toJSON(m));
  }
  
  private void handleSyncShardAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException, SolrServerException, IOException {
    log.info("Syncing shard : " + req.getParamString());
    String collection = req.getParams().required().get("collection");
    String shard = req.getParams().required().get("shard");
    
    ClusterState clusterState = coreContainer.getZkController().getClusterState();
    
    ZkNodeProps leaderProps = clusterState.getLeader(collection, shard);
    ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);
    
    HttpSolrServer server = new HttpSolrServer(nodeProps.getBaseUrl());
    RequestSyncShard reqSyncShard = new CoreAdminRequest.RequestSyncShard();
    reqSyncShard.setCollection(collection);
    reqSyncShard.setShard(shard);
    reqSyncShard.setCoreName(nodeProps.getCoreName());
    server.request(reqSyncShard);
  }


  private void handleDeleteAction(SolrQueryRequest req, SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    log.info("Deleting Collection : " + req.getParamString());
    
    String name = req.getParams().required().get("name");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        OverseerCollectionProcessor.DELETECOLLECTION, "name", name);

    // TODO: what if you want to block until the collection is available?
    coreContainer.getZkController().getOverseerCollectionQueue().offer(ZkStateReader.toJSON(m));
  }


  // very simple currently, you can pass a template collection, and the new collection is created on
  // every node the template collection is on
  // there is a lot more to add - you should also be able to create with an explicit server list
  // we might also want to think about error handling (add the request to a zk queue and involve overseer?)
  // as well as specific replicas= options
  private void handleCreateAction(SolrQueryRequest req,
      SolrQueryResponse rsp) throws InterruptedException, KeeperException {
    log.info("Creating Collection : " + req.getParamString());
    Integer numReplicas = req.getParams().getInt(OverseerCollectionProcessor.REPLICATION_FACTOR, 0);
    String name = req.getParams().required().get("name");
    String configName = req.getParams().get("collection.configName");
    String numShards = req.getParams().get("numShards");
    
    ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION,
        OverseerCollectionProcessor.CREATECOLLECTION, OverseerCollectionProcessor.REPLICATION_FACTOR, numReplicas.toString(), "name", name,
        "collection.configName", configName, "numShards", numShards);

    // TODO: what if you want to block until the collection is available?
    coreContainer.getZkController().getOverseerCollectionQueue().offer(ZkStateReader.toJSON(m));
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i=0; i<params.length; i+=2) {
      msp.add(params[i], params[i+1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage SolrCloud Collections";
  }

  @Override
  public String getSource() {
    return "$URL: https://svn.apache.org/repos/asf/lucene/dev/trunk/solr/core/src/java/org/apache/solr/handler/admin/CollectionHandler.java $";
  }
}
