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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient.Builder;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestSyncShard;
import org.apache.solr.client.solrj.response.RequestStatusState;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.OverseerTaskQueue.QueueEvent;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.rule.ReplicaAssigner;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.CoreAdminParams.CoreAdminAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.snapshots.CollectionSnapshotMetaData;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.client.solrj.response.RequestStatusState.COMPLETED;
import static org.apache.solr.client.solrj.response.RequestStatusState.FAILED;
import static org.apache.solr.client.solrj.response.RequestStatusState.NOT_FOUND;
import static org.apache.solr.client.solrj.response.RequestStatusState.RUNNING;
import static org.apache.solr.client.solrj.response.RequestStatusState.SUBMITTED;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_CONF;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_PROP_PREFIX;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.ONLY_IF_DOWN;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.REQUESTID;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.SHARDS_PROP;
import static org.apache.solr.cloud.OverseerCollectionMessageHandler.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.cloud.DocCollection.RULE;
import static org.apache.solr.common.cloud.DocCollection.SNITCH;
import static org.apache.solr.common.cloud.DocCollection.STATE_FORMAT;
import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.VALUE_LONG;
import static org.apache.solr.common.params.CoreAdminParams.DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.INSTANCE_DIR;
import static org.apache.solr.common.params.ShardParams._ROUTE_;
import static org.apache.solr.common.util.StrUtils.formatString;

public class CollectionsHandler extends RequestHandlerBase implements PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    String action = ctx.getParams().get("action");
    if (action == null) return PermissionNameProvider.Name.COLL_READ_PERM;
    CollectionParams.CollectionAction collectionAction = CollectionParams.CollectionAction.get(action);
    if (collectionAction == null) return null;
    return collectionAction.isWrite ?
        PermissionNameProvider.Name.COLL_EDIT_PERM :
        PermissionNameProvider.Name.COLL_READ_PERM;
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

    // Make sure that the core is ZKAware
    if(!cores.isZooKeeperAware()) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "Solr instance is not running in SolrCloud mode.");
    }

    // Pick the action
    SolrParams params = req.getParams();
    String a = params.get(CoreAdminParams.ACTION);
    if (a != null) {
      CollectionAction action = CollectionAction.get(a);
      if (action == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      }
      CollectionOperation operation = CollectionOperation.get(action);
      log.info("Invoked Collection Action :{} with params {} and sendToOCPQueue={}", action.toLower(), req.getParamString(), operation.sendToOCPQueue);

      SolrResponse response = null;
      Map<String, Object> props = operation.execute(req, rsp, this);
      String asyncId = req.getParams().get(ASYNC);
      if (props != null) {
        if (asyncId != null) {
          props.put(ASYNC, asyncId);
        }
        props.put(QUEUE_OPERATION, operation.action.toLower());
        ZkNodeProps zkProps = new ZkNodeProps(props);
        if (operation.sendToOCPQueue) {
          response = handleResponse(operation.action.toLower(), zkProps, rsp, operation.timeOut);
        }
        else Overseer.getStateUpdateQueue(coreContainer.getZkController().getZkClient()).offer(Utils.toJSON(props));
        final String collectionName = zkProps.getStr(NAME);
        if (action.equals(CollectionAction.CREATE) && asyncId == null) {
          if (rsp.getException() == null) {
            waitForActiveCollection(collectionName, zkProps, cores, response);
          }
        }
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "action is a required param");
    }
    rsp.setHttpCaching(false);
  }




  static final Set<String> KNOWN_ROLES = ImmutableSet.of("overseer");

  public static long DEFAULT_COLLECTION_OP_TIMEOUT = 180*1000;

  void handleResponse(String operation, ZkNodeProps m,
                              SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    handleResponse(operation, m, rsp, DEFAULT_COLLECTION_OP_TIMEOUT);
  }

  private SolrResponse handleResponse(String operation, ZkNodeProps m,
      SolrQueryResponse rsp, long timeout) throws KeeperException, InterruptedException {
    long time = System.nanoTime();

    if (m.containsKey(ASYNC) && m.get(ASYNC) != null) {

       String asyncId = m.getStr(ASYNC);

       if(asyncId.equals("-1")) {
         throw new SolrException(ErrorCode.BAD_REQUEST, "requestid can not be -1. It is reserved for cleanup purposes.");
       }

       NamedList<String> r = new NamedList<>();

       if (coreContainer.getZkController().getOverseerCompletedMap().contains(asyncId) ||
           coreContainer.getZkController().getOverseerFailureMap().contains(asyncId) ||
           coreContainer.getZkController().getOverseerRunningMap().contains(asyncId) ||
           overseerCollectionQueueContains(asyncId)) {
         r.add("error", "Task with the same requestid already exists.");

       } else {
         coreContainer.getZkController().getOverseerCollectionQueue()
             .offer(Utils.toJSON(m));
       }
       r.add(CoreAdminParams.REQUESTID, (String) m.get(ASYNC));
       SolrResponse response = new OverseerSolrResponse(r);

       rsp.getValues().addAll(response.getResponse());

       return response;
     }

    QueueEvent event = coreContainer.getZkController()
        .getOverseerCollectionQueue()
        .offer(Utils.toJSON(m), timeout);
    if (event.getBytes() != null) {
      SolrResponse response = SolrResponse.deserialize(event.getBytes());
      rsp.getValues().addAll(response.getResponse());
      SimpleOrderedMap exp = (SimpleOrderedMap) response.getResponse().get("exception");
      if (exp != null) {
        Integer code = (Integer) exp.get("rspCode");
        rsp.setException(new SolrException(code != null && code != -1 ? ErrorCode.getErrorCode(code) : ErrorCode.SERVER_ERROR, (String)exp.get("msg")));
      }
      return response;
    } else {
      if (System.nanoTime() - time >= TimeUnit.NANOSECONDS.convert(timeout, TimeUnit.MILLISECONDS)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection time out:" + timeout / 1000 + "s");
      } else if (event.getWatchedEvent() != null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection error [Watcher fired on path: "
            + event.getWatchedEvent().getPath() + " state: "
            + event.getWatchedEvent().getState() + " type "
            + event.getWatchedEvent().getType() + "]");
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, operation
            + " the collection unknown case");
      }
    }
  }

  private boolean overseerCollectionQueueContains(String asyncId) throws KeeperException, InterruptedException {
    OverseerTaskQueue collectionQueue = coreContainer.getZkController().getOverseerCollectionQueue();
    return collectionQueue.containsTaskWithRequestId(ASYNC, asyncId);
  }

  private static Map<String, Object> copyPropertiesWithPrefix(SolrParams params, Map<String, Object> props, String prefix) {
    Iterator<String> iter =  params.getParameterNamesIterator();
    while (iter.hasNext()) {
      String param = iter.next();
      if (param.startsWith(prefix)) {
        props.put(param, params.get(param));
      }
    }
    return props;
  }

  public static ModifiableSolrParams params(String... params) {
    ModifiableSolrParams msp = new ModifiableSolrParams();
    for (int i = 0; i < params.length; i += 2) {
      msp.add(params[i], params[i + 1]);
    }
    return msp;
  }

  //////////////////////// SolrInfoMBeans methods //////////////////////

  @Override
  public String getDescription() {
    return "Manage SolrCloud Collections";
  }

  public static final String SYSTEM_COLL = ".system";

  private static void createSysConfigSet(CoreContainer coreContainer) throws KeeperException, InterruptedException {
    SolrZkClient zk = coreContainer.getZkController().getZkStateReader().getZkClient();
    ZkCmdExecutor cmdExecutor = new ZkCmdExecutor(zk.getZkClientTimeout());
    cmdExecutor.ensureExists(ZkStateReader.CONFIGS_ZKNODE, zk);
    cmdExecutor.ensureExists(ZkStateReader.CONFIGS_ZKNODE + "/" + SYSTEM_COLL, zk);

    try {
      String path = ZkStateReader.CONFIGS_ZKNODE + "/" + SYSTEM_COLL + "/schema.xml";
      byte[] data = IOUtils.toByteArray(Thread.currentThread().getContextClassLoader().getResourceAsStream("SystemCollectionSchema.xml"));
      assert data != null && data.length > 0;
      cmdExecutor.ensureExists(path, data, CreateMode.PERSISTENT, zk);
      path = ZkStateReader.CONFIGS_ZKNODE + "/" + SYSTEM_COLL + "/solrconfig.xml";
      data = IOUtils.toByteArray(Thread.currentThread().getContextClassLoader().getResourceAsStream("SystemCollectionSolrConfig.xml"));
      assert data != null && data.length > 0;
      cmdExecutor.ensureExists(path, data, CreateMode.PERSISTENT, zk);
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }


  }

  private static void addStatusToResponse(NamedList<Object> results, RequestStatusState state, String msg) {
    SimpleOrderedMap<String> status = new SimpleOrderedMap<>();
    status.add("state", state.getKey());
    status.add("msg", msg);
    results.add("status", status);
  }

  enum CollectionOperation implements CollectionOp {
    /**
     * very simple currently, you can pass a template collection, and the new collection is created on
     * every node the template collection is on
     * there is a lot more to add - you should also be able to create with an explicit server list
     * we might also want to think about error handling (add the request to a zk queue and involve overseer?)
     * as well as specific replicas= options
     */
    CREATE_OP(CREATE, (req, rsp, h) -> {
      Map<String, Object> props = req.getParams().required().getAll(null, NAME);
      props.put("fromApi", "true");
      req.getParams().getAll(props,
          REPLICATION_FACTOR,
          COLL_CONF,
          NUM_SLICES,
          MAX_SHARDS_PER_NODE,
          CREATE_NODE_SET, CREATE_NODE_SET_SHUFFLE,
          SHARDS_PROP,
          STATE_FORMAT,
          AUTO_ADD_REPLICAS,
          RULE,
          SNITCH);

      if (props.get(STATE_FORMAT) == null) {
        props.put(STATE_FORMAT, "2");
      }
      addMapObject(props, RULE);
      addMapObject(props, SNITCH);
      verifyRuleParams(h.coreContainer, props);
      final String collectionName = SolrIdentifierValidator.validateCollectionName((String) props.get(NAME));
      final String shardsParam = (String) props.get(SHARDS_PROP);
      if (StringUtils.isNotEmpty(shardsParam)) {
        verifyShardsParam(shardsParam);
      }
      if (SYSTEM_COLL.equals(collectionName)) {
        //We must always create a .system collection with only a single shard
        props.put(NUM_SLICES, 1);
        props.remove(SHARDS_PROP);
        createSysConfigSet(h.coreContainer);

      }
      copyPropertiesWithPrefix(req.getParams(), props, COLL_PROP_PREFIX);
      return copyPropertiesWithPrefix(req.getParams(), props, "router.");

    }),
    DELETE_OP(DELETE, (req, rsp, h) -> req.getParams().required().getAll(null, NAME)),

    RELOAD_OP(RELOAD, (req, rsp, h) -> req.getParams().required().getAll(null, NAME)),

    SYNCSHARD_OP(SYNCSHARD, (req, rsp, h) -> {
      String collection = req.getParams().required().get("collection");
      String shard = req.getParams().required().get("shard");

      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();

      DocCollection docCollection = clusterState.getCollection(collection);
      ZkNodeProps leaderProps = docCollection.getLeader(shard);
      ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);

      try (HttpSolrClient client = new Builder(nodeProps.getBaseUrl()).build()) {
        client.setConnectionTimeout(15000);
        client.setSoTimeout(60000);
        RequestSyncShard reqSyncShard = new RequestSyncShard();
        reqSyncShard.setCollection(collection);
        reqSyncShard.setShard(shard);
        reqSyncShard.setCoreName(nodeProps.getCoreName());
        client.request(reqSyncShard);
      }
      return null;
    }),
    CREATEALIAS_OP(CREATEALIAS, (req, rsp, h) -> {
      SolrIdentifierValidator.validateAliasName(req.getParams().get(NAME));
      return req.getParams().required().getAll(null, NAME, "collections");
    }),
    DELETEALIAS_OP(DELETEALIAS, (req, rsp, h) -> req.getParams().required().getAll(null, NAME)),
    SPLITSHARD_OP(SPLITSHARD, DEFAULT_COLLECTION_OP_TIMEOUT * 5, true, (req, rsp, h) -> {
      String name = req.getParams().required().get(COLLECTION_PROP);
      // TODO : add support for multiple shards
      String shard = req.getParams().get(SHARD_ID_PROP);
      String rangesStr = req.getParams().get(CoreAdminParams.RANGES);
      String splitKey = req.getParams().get("split.key");

      if (splitKey == null && shard == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "At least one of shard, or split.key should be specified.");
      }
      if (splitKey != null && shard != null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Only one of 'shard' or 'split.key' should be specified");
      }
      if (splitKey != null && rangesStr != null) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Only one of 'ranges' or 'split.key' should be specified");
      }

      Map<String, Object> map = req.getParams().getAll(null,
          COLLECTION_PROP,
          SHARD_ID_PROP,
          "split.key",
          CoreAdminParams.RANGES);
      return copyPropertiesWithPrefix(req.getParams(), map, COLL_PROP_PREFIX);
    }),
    DELETESHARD_OP(DELETESHARD, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP,
          SHARD_ID_PROP);
      req.getParams().getAll(map,
          DELETE_INDEX,
          DELETE_DATA_DIR,
          DELETE_INSTANCE_DIR);
      return map;
    }),
    FORCELEADER_OP(FORCELEADER, (req, rsp, h) -> {
      forceLeaderElection(req, h);
      return null;
    }),
    CREATESHARD_OP(CREATESHARD, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP,
          SHARD_ID_PROP);
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      final String newShardName = SolrIdentifierValidator.validateShardName(req.getParams().get(SHARD_ID_PROP));
      if (!ImplicitDocRouter.NAME.equals(((Map) clusterState.getCollection(req.getParams().get(COLLECTION_PROP)).get(DOC_ROUTER)).get(NAME)))
        throw new SolrException(ErrorCode.BAD_REQUEST, "shards can be added only to 'implicit' collections");
      req.getParams().getAll(map,
          REPLICATION_FACTOR,
          CREATE_NODE_SET);
      return copyPropertiesWithPrefix(req.getParams(), map, COLL_PROP_PREFIX);
    }),
    DELETEREPLICA_OP(DELETEREPLICA, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP);

      return req.getParams().getAll(map,
          DELETE_INDEX,
          DELETE_DATA_DIR,
          DELETE_INSTANCE_DIR,
              COUNT_PROP, REPLICA_PROP,
              SHARD_ID_PROP,
          ONLY_IF_DOWN);
    }),
    MIGRATE_OP(MIGRATE, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null, COLLECTION_PROP, "split.key", "target.collection");
      return req.getParams().getAll(map, "forward.timeout");
    }),
    ADDROLE_OP(ADDROLE, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null, "role", "node");
      if (!KNOWN_ROLES.contains(map.get("role")))
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
      return map;
    }),
    REMOVEROLE_OP(REMOVEROLE, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null, "role", "node");
      if (!KNOWN_ROLES.contains(map.get("role")))
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
      return map;
    }),
    CLUSTERPROP_OP(CLUSTERPROP, (req, rsp, h) -> {
      String name = req.getParams().required().get(NAME);
      String val = req.getParams().get(VALUE_LONG);
      ClusterProperties cp = new ClusterProperties(h.coreContainer.getZkController().getZkClient());
      cp.setClusterProperty(name, val);
      return null;
    }),
    REQUESTSTATUS_OP(REQUESTSTATUS, (req, rsp, h) -> {
      req.getParams().required().check(REQUESTID);

      final CoreContainer coreContainer1 = h.coreContainer;
      final String requestId = req.getParams().get(REQUESTID);
      final ZkController zkController = coreContainer1.getZkController();

      final NamedList<Object> results = new NamedList<>();
      if (zkController.getOverseerCompletedMap().contains(requestId)) {
        final byte[] mapEntry = zkController.getOverseerCompletedMap().get(requestId);
        rsp.getValues().addAll(SolrResponse.deserialize(mapEntry).getResponse());
        addStatusToResponse(results, COMPLETED, "found [" + requestId + "] in completed tasks");
      } else if (zkController.getOverseerFailureMap().contains(requestId)) {
        final byte[] mapEntry = zkController.getOverseerFailureMap().get(requestId);
        rsp.getValues().addAll(SolrResponse.deserialize(mapEntry).getResponse());
        addStatusToResponse(results, FAILED, "found [" + requestId + "] in failed tasks");
      } else if (zkController.getOverseerRunningMap().contains(requestId)) {
        addStatusToResponse(results, RUNNING, "found [" + requestId + "] in running tasks");
      } else if (h.overseerCollectionQueueContains(requestId)) {
        addStatusToResponse(results, SUBMITTED, "found [" + requestId + "] in submitted tasks");
      } else {
        addStatusToResponse(results, NOT_FOUND, "Did not find [" + requestId + "] in any tasks queue");
      }

      final SolrResponse response = new OverseerSolrResponse(results);
      rsp.getValues().addAll(response.getResponse());
      return null;
    }),
    DELETESTATUS_OP(DELETESTATUS, new CollectionOp() {
      @SuppressWarnings("unchecked")
      @Override
      public Map<String, Object> execute(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        final CoreContainer coreContainer = h.coreContainer;
        final String requestId = req.getParams().get(REQUESTID);
        final ZkController zkController = coreContainer.getZkController();
        Boolean flush = req.getParams().getBool(CollectionAdminParams.FLUSH, false);

        if (requestId == null && !flush) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Either requestid or flush parameter must be specified.");
        }

        if (requestId != null && flush) {
          throw new SolrException(ErrorCode.BAD_REQUEST,
              "Both requestid and flush parameters can not be specified together.");
        }

        if (flush) {
          zkController.getOverseerCompletedMap().clear();
          zkController.getOverseerFailureMap().clear();
          rsp.getValues().add("status", "successfully cleared stored collection api responses");
          return null;
        } else {
          // Request to cleanup
          if (zkController.getOverseerCompletedMap().remove(requestId)) {
            rsp.getValues().add("status", "successfully removed stored response for [" + requestId + "]");
          } else if (zkController.getOverseerFailureMap().remove(requestId)) {
            rsp.getValues().add("status", "successfully removed stored response for [" + requestId + "]");
          } else {
            rsp.getValues().add("status", "[" + requestId + "] not found in stored responses");
          }
        }
        return null;
      }
    }),
    ADDREPLICA_OP(ADDREPLICA, (req, rsp, h) -> {
      Map<String, Object> props = req.getParams().getAll(null,
          COLLECTION_PROP,
          "node",
          SHARD_ID_PROP,
          _ROUTE_,
          CoreAdminParams.NAME,
          INSTANCE_DIR,
          DATA_DIR);
      return copyPropertiesWithPrefix(req.getParams(), props, COLL_PROP_PREFIX);
    }),
    OVERSEERSTATUS_OP(OVERSEERSTATUS, (req, rsp, h) -> (Map) new LinkedHashMap<>()),

    /**
     * Handle list collection request.
     * Do list collection request to zk host
     */
    LIST_OP(LIST, (req, rsp, h) -> {
      NamedList<Object> results = new NamedList<>();
      Map<String, DocCollection> collections = h.coreContainer.getZkController().getZkStateReader().getClusterState().getCollectionsMap();
      List<String> collectionList = new ArrayList<>(collections.keySet());
      results.add("collections", collectionList);
      SolrResponse response = new OverseerSolrResponse(results);
      rsp.getValues().addAll(response.getResponse());
      return null;
    }),
    /**
     * Handle cluster status request.
     * Can return status per specific collection/shard or per all collections.
     */
    CLUSTERSTATUS_OP(CLUSTERSTATUS, (req, rsp, h) -> {
      Map<String, Object> all = req.getParams().getAll(null,
          COLLECTION_PROP,
          SHARD_ID_PROP,
          _ROUTE_);
      new ClusterStatus(h.coreContainer.getZkController().getZkStateReader(),
          new ZkNodeProps(all)).getClusterStatus(rsp.getValues());
      return null;
    }),
    ADDREPLICAPROP_OP(ADDREPLICAPROP, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP,
          PROPERTY_PROP,
          SHARD_ID_PROP,
          REPLICA_PROP,
          PROPERTY_VALUE_PROP);
      req.getParams().getAll(map, SHARD_UNIQUE);
      String property = (String) map.get(PROPERTY_PROP);
      if (!property.startsWith(COLL_PROP_PREFIX)) {
        property = COLL_PROP_PREFIX + property;
      }

      boolean uniquePerSlice = Boolean.parseBoolean((String) map.get(SHARD_UNIQUE));

      // Check if we're trying to set a property with parameters that allow us to set the property on multiple replicas
      // in a slice on properties that are known to only be one-per-slice and error out if so.
      if (StringUtils.isNotBlank((String) map.get(SHARD_UNIQUE)) &&
          SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(property.toLowerCase(Locale.ROOT)) &&
          uniquePerSlice == false) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Overseer replica property command received for property " + property +
                " with the " + SHARD_UNIQUE +
                " parameter set to something other than 'true'. No action taken.");
      }
      return map;
    }),
    DELETEREPLICAPROP_OP(DELETEREPLICAPROP, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP,
          PROPERTY_PROP,
          SHARD_ID_PROP,
          REPLICA_PROP);
      return req.getParams().getAll(map, PROPERTY_PROP);
    }),
    BALANCESHARDUNIQUE_OP(BALANCESHARDUNIQUE, (req, rsp, h) -> {
      Map<String, Object> map = req.getParams().required().getAll(null,
          COLLECTION_PROP,
          PROPERTY_PROP);
      Boolean shardUnique = Boolean.parseBoolean(req.getParams().get(SHARD_UNIQUE));
      String prop = req.getParams().get(PROPERTY_PROP).toLowerCase(Locale.ROOT);
      if (!StringUtils.startsWith(prop, COLL_PROP_PREFIX)) {
        prop = COLL_PROP_PREFIX + prop;
      }

      if (!shardUnique && !SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(prop)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Balancing properties amongst replicas in a slice requires that"
            + " the property be pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'. " +
            " Property: " + prop + " shardUnique: " + Boolean.toString(shardUnique));
      }

      return req.getParams().getAll(map, ONLY_ACTIVE_NODES, SHARD_UNIQUE);
    }),
    REBALANCELEADERS_OP(REBALANCELEADERS, (req, rsp, h) -> {
      new RebalanceLeaders(req, rsp, h).execute();
      return null;
    }),
    MODIFYCOLLECTION_OP(MODIFYCOLLECTION, (req, rsp, h) -> {
      Map<String, Object> m = req.getParams().getAll(null, MODIFIABLE_COLL_PROPS);
      if (m.isEmpty()) throw new SolrException(ErrorCode.BAD_REQUEST,
          formatString("no supported values provided rule, snitch, maxShardsPerNode, replicationFactor, collection.configName"));
      req.getParams().required().getAll(m, COLLECTION_PROP);
      addMapObject(m, RULE);
      addMapObject(m, SNITCH);
      for (String prop : MODIFIABLE_COLL_PROPS) DocCollection.verifyProp(m, prop);
      verifyRuleParams(h.coreContainer, m);
      return m;
    }),
    MIGRATESTATEFORMAT_OP(MIGRATESTATEFORMAT, (req, rsp, h) -> req.getParams().required().getAll(null, COLLECTION_PROP)),

    BACKUP_OP(BACKUP, (req, rsp, h) -> {
      req.getParams().required().check(NAME, COLLECTION_PROP);

      String collectionName = req.getParams().get(COLLECTION_PROP);
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      if (!clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
      }

      CoreContainer cc = h.coreContainer;
      String repo = req.getParams().get(CoreAdminParams.BACKUP_REPOSITORY);
      BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo));

      String location = repository.getBackupLocation(req.getParams().get(CoreAdminParams.BACKUP_LOCATION));
      if (location == null) {
        //Refresh the cluster property file to make sure the value set for location is the latest
        // Check if the location is specified in the cluster property.
        location = new ClusterProperties(h.coreContainer.getZkController().getZkClient()).getClusterProperty(CoreAdminParams.BACKUP_LOCATION, null);
        if (location == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "'location' is not specified as a query"
              + " parameter or as a default repository property or as a cluster property.");
        }
      }

      // Check if the specified location is valid for this repository.
      URI uri = repository.createURI(location);
      try {
        if (!repository.exists(uri)) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "specified location " + uri + " does not exist.");
        }
      } catch (IOException ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to check the existance of " + uri + ". Is it valid?", ex);
      }

      String strategy = req.getParams().get(CollectionAdminParams.INDEX_BACKUP_STRATEGY, CollectionAdminParams.COPY_FILES_STRATEGY);
      if (!CollectionAdminParams.INDEX_BACKUP_STRATEGIES.contains(strategy)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown index backup strategy " + strategy);
      }

      Map<String, Object> params = req.getParams().getAll(null, NAME, COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);
      params.put(CoreAdminParams.BACKUP_LOCATION, location);
      params.put(CollectionAdminParams.INDEX_BACKUP_STRATEGY, strategy);
      return params;
    }),
    RESTORE_OP(RESTORE, (req, rsp, h) -> {
      req.getParams().required().check(NAME, COLLECTION_PROP);

      String collectionName = SolrIdentifierValidator.validateCollectionName(req.getParams().get(COLLECTION_PROP));
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      //We always want to restore into an collection name which doesn't  exist yet.
      if (clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' exists, no action taken.");
      }

      CoreContainer cc = h.coreContainer;
      String repo = req.getParams().get(CoreAdminParams.BACKUP_REPOSITORY);
      BackupRepository repository = cc.newBackupRepository(Optional.ofNullable(repo));

      String location = repository.getBackupLocation(req.getParams().get(CoreAdminParams.BACKUP_LOCATION));
      if (location == null) {
        //Refresh the cluster property file to make sure the value set for location is the latest
        // Check if the location is specified in the cluster property.
        location = new ClusterProperties(h.coreContainer.getZkController().getZkClient()).getClusterProperty("location", null);
        if (location == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "'location' is not specified as a query"
              + " parameter or as a default repository property or as a cluster property.");
        }
      }

      // Check if the specified location is valid for this repository.
      URI uri = repository.createURI(location);
      try {
        if (!repository.exists(uri)) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "specified location " + uri + " does not exist.");
        }
      } catch (IOException ex) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Failed to check the existance of " + uri + ". Is it valid?", ex);
      }

      Map<String, Object> params = req.getParams().getAll(null, NAME, COLLECTION_PROP);
      params.put(CoreAdminParams.BACKUP_LOCATION, location);
      // from CREATE_OP:
      req.getParams().getAll(params, COLL_CONF, REPLICATION_FACTOR, MAX_SHARDS_PER_NODE, STATE_FORMAT, AUTO_ADD_REPLICAS);
      copyPropertiesWithPrefix(req.getParams(), params, COLL_PROP_PREFIX);
      return params;
    }),
    CREATESNAPSHOT_OP(CREATESNAPSHOT, (req, rsp, h) -> {
      req.getParams().required().check(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);

      String collectionName = req.getParams().get(COLLECTION_PROP);
      String commitName = req.getParams().get(CoreAdminParams.COMMIT_NAME);
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      if (!clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
      }

      SolrZkClient client = h.coreContainer.getZkController().getZkClient();
      if (SolrSnapshotManager.snapshotExists(client, collectionName, commitName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST,
            "Snapshot with name '" + commitName + "' already exists for collection '"
                + collectionName + "', no action taken.");
      }

      Map<String, Object> params = req.getParams().getAll(null, COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);
      return params;
    }),
    DELETESNAPSHOT_OP(DELETESNAPSHOT, (req, rsp, h) -> {
      req.getParams().required().check(COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);

      String collectionName = req.getParams().get(COLLECTION_PROP);
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      if (!clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
      }

      Map<String, Object> params = req.getParams().getAll(null, COLLECTION_PROP, CoreAdminParams.COMMIT_NAME);
      return params;
    }),
    LISTSNAPSHOTS_OP(LISTSNAPSHOTS, (req, rsp, h) -> {
      req.getParams().required().check(COLLECTION_PROP);

      String collectionName = req.getParams().get(COLLECTION_PROP);
      ClusterState clusterState = h.coreContainer.getZkController().getClusterState();
      if (!clusterState.hasCollection(collectionName)) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' does not exist, no action taken.");
      }

      NamedList<Object> snapshots = new NamedList<Object>();
      SolrZkClient client = h.coreContainer.getZkController().getZkClient();
      Collection<CollectionSnapshotMetaData> m = SolrSnapshotManager.listSnapshots(client, collectionName);
      for (CollectionSnapshotMetaData meta : m) {
        snapshots.add(meta.getName(), meta.toNamedList());
      }

      rsp.add(SolrSnapshotManager.SNAPSHOTS_INFO, snapshots);
      return null;
    }),
    REPLACENODE_OP(REPLACENODE, (req, rsp, h) -> req.getParams().required().getAll(req.getParams().getAll(null, "parallel"), "source", "target")),
    DELETENODE_OP(DELETENODE, (req, rsp, h) -> req.getParams().required().getAll(null, "node"));
    public final CollectionOp fun;
    CollectionAction action;
    long timeOut;
    boolean sendToOCPQueue;

    CollectionOperation(CollectionAction action, CollectionOp fun) {
      this(action, DEFAULT_COLLECTION_OP_TIMEOUT, true, fun);
    }

    CollectionOperation(CollectionAction action, long timeOut, boolean sendToOCPQueue, CollectionOp fun) {
      this.action = action;
      this.timeOut = timeOut;
      this.sendToOCPQueue = sendToOCPQueue;
      this.fun = fun;

    }


    public static CollectionOperation get(CollectionAction action) {
      for (CollectionOperation op : values()) {
        if (op.action == action) return op;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "No such action" + action);
    }

    @Override
    public Map<String, Object> execute(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
        throws Exception {
      return fun.execute(req, rsp, h);
    }
  }

  private static void forceLeaderElection(SolrQueryRequest req, CollectionsHandler handler) {
    ClusterState clusterState = handler.coreContainer.getZkController().getClusterState();
    String collectionName = req.getParams().required().get(COLLECTION_PROP);
    String sliceId = req.getParams().required().get(SHARD_ID_PROP);

    log.info("Force leader invoked, state: {}", clusterState);
    DocCollection collection = clusterState.getCollection(collectionName);
    Slice slice = collection.getSlice(sliceId);
    if (slice == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST,
          "No shard with name " + sliceId + " exists for collection " + collectionName);
    }

    try {
      // if an active replica is the leader, then all is fine already
      Replica leader = slice.getLeader();
      if (leader != null && leader.getState() == State.ACTIVE) {
        throw new SolrException(ErrorCode.SERVER_ERROR,
            "The shard already has an active leader. Force leader is not applicable. State: " + slice);
      }

      // Clear out any LIR state
      String lirPath = handler.coreContainer.getZkController().getLeaderInitiatedRecoveryZnodePath(collectionName, sliceId);
      if (handler.coreContainer.getZkController().getZkClient().exists(lirPath, true)) {
        StringBuilder sb = new StringBuilder();
        handler.coreContainer.getZkController().getZkClient().printLayout(lirPath, 4, sb);
        log.info("Cleaning out LIR data, which was: {}", sb);
        handler.coreContainer.getZkController().getZkClient().clean(lirPath);
      }

      // Call all live replicas to prepare themselves for leadership, e.g. set last published
      // state to active.
      for (Replica rep : slice.getReplicas()) {
        if (clusterState.getLiveNodes().contains(rep.getNodeName())) {
          ShardHandler shardHandler = handler.coreContainer.getShardHandlerFactory().getShardHandler();

          ModifiableSolrParams params = new ModifiableSolrParams();
          params.set(CoreAdminParams.ACTION, CoreAdminAction.FORCEPREPAREFORLEADERSHIP.toString());
          params.set(CoreAdminParams.CORE, rep.getStr("core"));
          String nodeName = rep.getNodeName();

          OverseerCollectionMessageHandler.sendShardRequest(nodeName, params, shardHandler, null, null,
              CommonParams.CORES_HANDLER_PATH, handler.coreContainer.getZkController().getZkStateReader()); // synchronous request
        }
      }

      // Wait till we have an active leader
      boolean success = false;
      for (int i = 0; i < 9; i++) {
        Thread.sleep(5000);
        clusterState = handler.coreContainer.getZkController().getClusterState();
        collection = clusterState.getCollection(collectionName);
        slice = collection.getSlice(sliceId);
        if (slice.getLeader() != null && slice.getLeader().getState() == State.ACTIVE) {
          success = true;
          break;
        }
        log.warn("Force leader attempt {}. Waiting 5 secs for an active leader. State of the slice: {}", (i + 1), slice);
      }

      if (success) {
        log.info("Successfully issued FORCELEADER command for collection: {}, shard: {}", collectionName, sliceId);
      } else {
        log.info("Couldn't successfully force leader, collection: {}, shard: {}. Cluster state: {}", collectionName, sliceId, clusterState);
      }
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR,
          "Error executing FORCELEADER operation for collection: " + collectionName + " shard: " + sliceId, e);
    }
  }

  private static void waitForActiveCollection(String collectionName, ZkNodeProps message, CoreContainer cc, SolrResponse response)
      throws KeeperException, InterruptedException {

    if (response.getResponse().get("exception") != null) {
      // the main called failed, don't wait
      log.info("Not waiting for active collection due to exception: " + response.getResponse().get("exception"));
      return;
    }
    
    if (response.getResponse().get("failure") != null) {
      // TODO: we should not wait for Replicas we know failed
    }
    
    String replicaNotAlive = null;
    String replicaState = null;
    String nodeNotLive = null;

    CloudConfig ccfg = cc.getConfig().getCloudConfig();
    Integer numRetries = ccfg.getCreateCollectionWaitTimeTillActive();
    Boolean checkLeaderOnly = ccfg.isCreateCollectionCheckLeaderActive();
    log.info("Wait for new collection to be active for at most " + numRetries + " seconds. Check all shard "
        + (checkLeaderOnly ? "leaders" : "replicas"));
    ZkStateReader zkStateReader = cc.getZkController().getZkStateReader();
    for (int i = 0; i < numRetries; i++) {
      ClusterState clusterState = zkStateReader.getClusterState();

      Collection<Slice> shards = clusterState.getSlices(collectionName);
      if (shards != null) {
        replicaNotAlive = null;
        for (Slice shard : shards) {
          Collection<Replica> replicas;
          if (!checkLeaderOnly) replicas = shard.getReplicas();
          else {
            replicas = new ArrayList<Replica>();
            replicas.add(shard.getLeader());
          }
          for (Replica replica : replicas) {
            String state = replica.getStr(ZkStateReader.STATE_PROP);
            log.debug("Checking replica status, collection={} replica={} state={}", collectionName,
                replica.getCoreUrl(), state);
            if (!clusterState.liveNodesContain(replica.getNodeName())
                || !state.equals(Replica.State.ACTIVE.toString())) {
              replicaNotAlive = replica.getCoreUrl();
              nodeNotLive = replica.getNodeName();
              replicaState = state;
              break;
            }
          }
          if (replicaNotAlive != null) break;
        }

        if (replicaNotAlive == null) return;
      }
      Thread.sleep(1000);
    }
    if (nodeNotLive != null && replicaState != null) {
      log.error("Timed out waiting for new collection's replicas to become ACTIVE "
              + (replicaState.equals(Replica.State.ACTIVE.toString()) ? "node " + nodeNotLive + " is not live"
                  : "replica " + replicaNotAlive + " is in state of " + replicaState.toString()) + " with timeout=" + numRetries);
    } else {
      log.error("Timed out waiting for new collection's replicas to become ACTIVE with timeout=" + numRetries);
    }
  }
  
  public static void verifyRuleParams(CoreContainer cc, Map<String, Object> m) {
    List l = (List) m.get(RULE);
    if (l != null) {
      for (Object o : l) {
        Map map = (Map) o;
        try {
          new Rule(map);
        } catch (Exception e) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error in rule " + m, e);
        }
      }
    }
    ReplicaAssigner.verifySnitchConf(cc, (List) m.get(SNITCH));
  }

  /**
   * Converts a String of the form a:b,c:d to a Map
   */
  private static Map<String, Object> addMapObject(Map<String, Object> props, String key) {
    Object v = props.get(key);
    if (v == null) return props;
    List<String> val = new ArrayList<>();
    if (v instanceof String[]) {
      val.addAll(Arrays.asList((String[]) v));
    } else {
      val.add(v.toString());
    }
    if (val.size() > 0) {
      ArrayList<Map> l = new ArrayList<>();
      for (String rule : val) l.add(Rule.parseRule(rule));
      props.put(key, l);
    }
    return props;
  }
  
  private static void verifyShardsParam(String shardsParam) {
    for (String shard : shardsParam.split(",")) {
      SolrIdentifierValidator.validateShardName(shard);
    }
  }

  interface CollectionOp {
    Map<String, Object> execute(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception;
    
  }

  public static final List<String> MODIFIABLE_COLL_PROPS = Arrays.asList(
      RULE,
      SNITCH,
      REPLICATION_FACTOR,
      MAX_SHARDS_PER_NODE,
      AUTO_ADD_REPLICAS,
      COLL_CONF);
}
