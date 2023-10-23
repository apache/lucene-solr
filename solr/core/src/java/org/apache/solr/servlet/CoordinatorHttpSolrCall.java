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

package org.apache.solr.servlet;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.solr.api.CoordinatorV2HttpSolrCall;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.DelegatingSolrQueryRequest;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A coordinator node can serve requests as if it hosts all collections in the cluster. it does so
 * by hosting a synthetic replica for each configset used in the cluster.
 *
 * <p>This class is responsible for forwarding the requests to the right core when the node is
 * acting as a Coordinator The responsibilities also involve creating a synthetic collection or
 * replica if they do not exist. It also sets the right threadlocal variables which reflects the
 * current collection being served.
 */
public class CoordinatorHttpSolrCall extends HttpSolrCall {
  public static final String SYNTHETIC_COLL_PREFIX =
      ".sys." + "COORDINATOR-COLL-";
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String collectionName;
  private final Factory factory;

  public CoordinatorHttpSolrCall(
      Factory factory,
      SolrDispatchFilter solrDispatchFilter,
      CoreContainer cores,
      HttpServletRequest request,
      HttpServletResponse response,
      boolean retry) {
    super(solrDispatchFilter, cores, request, response, retry);
    this.factory = factory;
  }

  @Override
  protected SolrCore getCoreByCollection(String collectionName, boolean isPreferLeader) {
    log.info("getCoreByCollection(String collectionName({})", collectionName);
    this.collectionName = collectionName;
    SolrCore core = super.getCoreByCollection(collectionName, isPreferLeader);
    if (core != null) return core;
    if (!path.endsWith("/select")) return null;
    return getCore(factory, this, collectionName, isPreferLeader);
  }

  public static SolrCore getCore(
      Factory factory, HttpSolrCall solrCall, String collectionName, boolean isPreferLeader) {
    log.info("CoordinatorHttpSolrCall#getCore({})", collectionName);
    String syntheticCoreName = factory.collectionVsCoreNameMapping.get(collectionName);
    if (syntheticCoreName != null) {
      SolrCore syntheticCore = solrCall.cores.getCore(syntheticCoreName);
      setMDCLoggingContext(collectionName);
      return syntheticCore;
    } else {
      ZkStateReader zkStateReader = solrCall.cores.getZkController().getZkStateReader();
      ClusterState clusterState = zkStateReader.getClusterState();
      DocCollection coll = clusterState.getCollectionOrNull(collectionName, true);
      SolrCore core = null;
      if (coll != null) {
        String confName = null;
        try {
          confName = zkStateReader.readConfigName(collectionName);
        } catch (KeeperException e) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Could not locate config for collection ["
                  + collectionName
                  + "] ", e);
        }
        String syntheticCollectionName = getSyntheticCollectionName(confName);

        DocCollection syntheticColl = clusterState.getCollectionOrNull(syntheticCollectionName);
        synchronized (CoordinatorHttpSolrCall.class) {
          if (syntheticColl == null) {
            // no synthetic collection for this config, let's create one
            if (log.isInfoEnabled()) {
              log.info(
                  "synthetic collection: {} does not exist, creating.. ", syntheticCollectionName);
            }

            SolrException createException = null;
            try {
              createColl(syntheticCollectionName, solrCall.cores, confName);
            } catch (SolrException exception) {
              // concurrent requests could have created the collection hence causing collection
              // exists
              // exception
              createException = exception;
            } finally {
              syntheticColl =
                  zkStateReader.getClusterState().getCollectionOrNull(syntheticCollectionName);
            }

            // then indeed the collection was not created properly, either by this or other
            // concurrent
            // requests
            if (syntheticColl == null) {
              if (createException != null) {
                throw createException; // rethrow the exception since such collection was not
                // created
              } else {
                throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Could not locate synthetic collection ["
                        + syntheticCollectionName
                        + "] after creation!");
              }
            }
          }

          // get docCollection again to ensure we get the fresh state
          syntheticColl =
              zkStateReader.getClusterState().getCollectionOrNull(syntheticCollectionName);
          List<Replica> nodeNameSyntheticReplicas =
              syntheticColl.getReplicas(solrCall.cores.getZkController().getNodeName());
          if (nodeNameSyntheticReplicas == null || nodeNameSyntheticReplicas.isEmpty()) {
            // this node does not have a replica. add one
            if (log.isInfoEnabled()) {
              log.info(
                  "this node does not have a replica of the synthetic collection: {} , adding replica ",
                  syntheticCollectionName);
            }

            addReplica(syntheticCollectionName, solrCall.cores);
          }

          // still have to ensure that it's active, otherwise super.getCoreByCollection
          // will return null and then CoordinatorHttpSolrCall will call getCore again
          // hence creating a calling loop
          try {
            zkStateReader.waitForState(
                syntheticCollectionName,
                10,
                TimeUnit.SECONDS,
                docCollection -> {
                  for (Replica nodeNameSyntheticReplica :
                      docCollection.getReplicas(solrCall.cores.getZkController().getNodeName())) {
                    if (nodeNameSyntheticReplica.getState() == Replica.State.ACTIVE) {
                      return true;
                    }
                  }
                  return false;
                });
          } catch (Exception e) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Failed to wait for active replica for synthetic collection ["
                    + syntheticCollectionName
                    + "]",
                e);
          }
        }

        core = solrCall.getCoreByCollection(syntheticCollectionName, isPreferLeader);
        if (core != null) {
          factory.collectionVsCoreNameMapping.put(collectionName, core.getName());
          // for the watcher, only remove on collection deletion (ie collection == null), since
          // watch from coordinator is collection specific
          solrCall
              .cores
              .getZkController()
              .getZkStateReader()
              .registerDocCollectionWatcher(
                  collectionName,
                  collection -> {
                    if (collection == null) {
                      factory.collectionVsCoreNameMapping.remove(collectionName);
                      return true;
                    } else {
                      return false;
                    }
                  });
          if (log.isDebugEnabled()) {
            log.debug("coordinator node, returns synthetic core: {}", core.getName());
          }
        }
        setMDCLoggingContext(collectionName);
        return core;
      }
      return null;
    }
  }

  public static String getSyntheticCollectionName(String configName) {
    return SYNTHETIC_COLL_PREFIX + configName;
  }

  /**
   * Overrides the MDC context as the core set was synthetic core, which does not reflect the
   * collection being operated on
   */
  private static void setMDCLoggingContext(String collectionName) {
    MDCLoggingContext.setCollection(collectionName);

    // below is irrelevant for call to coordinator
    MDCLoggingContext.setCoreName(null);
    MDCLoggingContext.setShard(null);
    MDCLoggingContext.setCoreName(null);
  }

  private static void addReplica(String syntheticCollectionName, CoreContainer cores) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      CollectionAdminRequest.AddReplica addReplicaRequest =
          CollectionAdminRequest.addReplicaToShard(syntheticCollectionName, "shard1")
              // we are fixing the name, so that no two replicas are created in the same node
              .setNode(cores.getZkController().getNodeName());
      addReplicaRequest.setWaitForFinalState(true);
      cores
          .getCollectionsHandler()
          .handleRequestBody(new LocalSolrQueryRequest(null, addReplicaRequest.getParams()), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not auto-create collection: " + Utils.toJSONString(rsp.getValues()));
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private static void createColl(
      String syntheticCollectionName, CoreContainer cores, String confName) {
    SolrQueryResponse rsp = new SolrQueryResponse();
    try {
      CollectionAdminRequest.Create collCreationRequest =
          CollectionAdminRequest.createCollection(syntheticCollectionName, confName, 1, 1)
              .setCreateNodeSet(cores.getZkController().getNodeName());
      collCreationRequest.setWaitForFinalState(true);
      SolrParams params = collCreationRequest.getParams();
      if (log.isInfoEnabled()) {
        log.info("sending collection admin command : {}", Utils.toJSONString(params));
      }
      cores.getCollectionsHandler().handleRequestBody(new LocalSolrQueryRequest(null, params), rsp);
      if (rsp.getValues().get("success") == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Could not create :"
                + syntheticCollectionName
                + " collection: "
                + Utils.toJSONString(rsp.getValues()));
      }
    } catch (SolrException e) {
      throw e;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  @Override
  protected void init() throws Exception {
    super.init();
    if (action == SolrDispatchFilter.Action.PROCESS && core != null) {
      solrReq = wrappedReq(solrReq, collectionName, this);
    }
  }

  @Override
  protected String getCoreOrColName() {
    return collectionName;
  }

  public static SolrQueryRequest wrappedReq(
      SolrQueryRequest delegate, String collectionName, HttpSolrCall httpSolrCall) {
    if(collectionName== null) return delegate;
    Properties p = new Properties();
    log.info("CoordinatorHttpSolrCall#wrappedReq({})",collectionName);
    p.put(CoreDescriptor.CORE_COLLECTION, collectionName);
    p.put(CloudDescriptor.REPLICA_TYPE, Replica.Type.PULL.toString());
    p.put(CoreDescriptor.CORE_SHARD, "_");

    CloudDescriptor cloudDescriptor =
        new CloudDescriptor(
            delegate.getCore().getCoreDescriptor(), delegate.getCore().getName(), p);
    return new DelegatingSolrQueryRequest(delegate) {
      @Override
      public HttpSolrCall getHttpSolrCall() {
        return httpSolrCall;
      }

      @Override
      public CloudDescriptor getCloudDescriptor() {
        return cloudDescriptor;
      }
    };
  }

  // The factory that creates an instance of HttpSolrCall
  public static class Factory implements SolrDispatchFilter.HttpSolrCallFactory {
    private final Map<String, String> collectionVsCoreNameMapping = new ConcurrentHashMap<>();

    @Override
    public HttpSolrCall createInstance(
        SolrDispatchFilter filter,
        String path,
        CoreContainer cores,
        HttpServletRequest request,
        HttpServletResponse response,
        boolean retry) {
      if ((path.startsWith("/____v2/") || path.equals("/____v2"))) {
        return new CoordinatorV2HttpSolrCall(this, filter, cores, request, response, retry);
      } else if (path.startsWith("/" + SYNTHETIC_COLL_PREFIX)) {
        return SolrDispatchFilter.HttpSolrCallFactory.super.createInstance(
            filter, path, cores, request, response, retry);
      } else {
        return new CoordinatorHttpSolrCall(this, filter, cores, request, response, retry);
      }
    }
  }
}
