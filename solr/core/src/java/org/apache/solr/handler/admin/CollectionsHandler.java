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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang.StringUtils;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestSyncShard;
import org.apache.solr.cloud.DistributedQueue;
import org.apache.solr.cloud.DistributedQueue.QueueEvent;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.rule.Rule;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.BlobHandler;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ASYNC;
import static org.apache.solr.cloud.OverseerCollectionProcessor.COLL_CONF;
import static org.apache.solr.cloud.OverseerCollectionProcessor.COLL_PROP_PREFIX;
import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET;
import static org.apache.solr.cloud.OverseerCollectionProcessor.CREATE_NODE_SET_SHUFFLE;
import static org.apache.solr.cloud.OverseerCollectionProcessor.NUM_SLICES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.OverseerCollectionProcessor.ONLY_IF_DOWN;
import static org.apache.solr.cloud.OverseerCollectionProcessor.REQUESTID;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARDS_PROP;
import static org.apache.solr.cloud.OverseerCollectionProcessor.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.DocCollection.DOC_ROUTER;
import static org.apache.solr.common.cloud.DocCollection.STATE_FORMAT;
import static org.apache.solr.common.cloud.ZkStateReader.AUTO_ADD_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CONFIGS_ZKNODE;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_SHARDS_PER_NODE;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.VALUE_LONG;
import static org.apache.solr.common.params.CoreAdminParams.DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.INSTANCE_DIR;
import static org.apache.solr.common.params.ShardParams._ROUTE_;

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
      if (action == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unknown action: " + a);
      CollectionOperation operation = CollectionOperation.get(action);
      log.info("Invoked Collection Action :{} with params{} ", action.toLower(), req.getParamString());
      Map<String, Object> result = operation.call(req, rsp, this);
      if (result != null) {
        result.put(QUEUE_OPERATION, operation.action.toLower());
        ZkNodeProps props = new ZkNodeProps(result);
        handleResponse(operation.action.toLower(), props, rsp, operation.timeOut);
      }
    } else {
      throw new SolrException(ErrorCode.BAD_REQUEST, "action is a required param");

    }

    rsp.setHttpCaching(false);
  }




  static final Set<String> KNOWN_ROLES = ImmutableSet.of("overseer");

  public static long DEFAULT_ZK_TIMEOUT = 180*1000;

  void handleResponse(String operation, ZkNodeProps m,
                              SolrQueryResponse rsp) throws KeeperException, InterruptedException {
    handleResponse(operation, m, rsp, DEFAULT_ZK_TIMEOUT);
  }

  private void handleResponse(String operation, ZkNodeProps m,
      SolrQueryResponse rsp, long timeout) throws KeeperException, InterruptedException {
    long time = System.nanoTime();

     if(m.containsKey(ASYNC) && m.get(ASYNC) != null) {

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
             .offer(ZkStateReader.toJSON(m));
       }
       r.add(CoreAdminParams.REQUESTID, (String) m.get(ASYNC));
       SolrResponse response = new OverseerSolrResponse(r);

       rsp.getValues().addAll(response.getResponse());

       return;
     }

    QueueEvent event = coreContainer.getZkController()
        .getOverseerCollectionQueue()
        .offer(ZkStateReader.toJSON(m), timeout);
    if (event.getBytes() != null) {
      SolrResponse response = SolrResponse.deserialize(event.getBytes());
      rsp.getValues().addAll(response.getResponse());
      SimpleOrderedMap exp = (SimpleOrderedMap) response.getResponse().get("exception");
      if (exp != null) {
        Integer code = (Integer) exp.get("rspCode");
        rsp.setException(new SolrException(code != null && code != -1 ? ErrorCode.getErrorCode(code) : ErrorCode.SERVER_ERROR, (String)exp.get("msg")));
      }
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
            + " the collection unkown case");
      }
    }
  }

  private boolean overseerCollectionQueueContains(String asyncId) throws KeeperException, InterruptedException {
    DistributedQueue collectionQueue = coreContainer.getZkController().getOverseerCollectionQueue();
    return collectionQueue.containsTaskWithRequestId(asyncId);
  }

  public static void createNodeIfNotExists(SolrZkClient zk, String path, byte[] data) throws KeeperException, InterruptedException {
    if(!zk.exists(path, true)){
      //create the config znode
      try {
        zk.create(path,data, CreateMode.PERSISTENT,true);
      } catch (KeeperException.NodeExistsException e) {
        //no problem . race condition. carry on the good work
      }
    }
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

  enum CollectionOperation {
    /**
     * very simple currently, you can pass a template collection, and the new collection is created on
     * every node the template collection is on
     * there is a lot more to add - you should also be able to create with an explicit server list
     * we might also want to think about error handling (add the request to a zk queue and involve overseer?)
     * as well as specific replicas= options
     */
    CREATE_OP(CREATE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
          throws KeeperException, InterruptedException {
        Map<String, Object> props = req.getParams().required().getAll(null, NAME);
        props.put("fromApi", "true");
        req.getParams().getAll(props,
            NAME,
            REPLICATION_FACTOR,
            COLL_CONF,
            NUM_SLICES,
            MAX_SHARDS_PER_NODE,
            CREATE_NODE_SET, CREATE_NODE_SET_SHUFFLE,
            SHARDS_PROP,
            ASYNC,
            STATE_FORMAT,
            AUTO_ADD_REPLICAS);

        if (props.get(STATE_FORMAT) == null) {
          props.put(STATE_FORMAT, "2");
        }
        addRuleMap(req.getParams(), props, "rule");
        addRuleMap(req.getParams(), props, "snitch");

        if (SYSTEM_COLL.equals(props.get(NAME))) {
          //We must always create asystem collection with only a single shard
          props.put(NUM_SLICES, 1);
          props.remove(SHARDS_PROP);
          createSysConfigSet(h.coreContainer);

        }
        copyPropertiesWithPrefix(req.getParams(), props, COLL_PROP_PREFIX);
        return copyPropertiesWithPrefix(req.getParams(), props, "router.");

      }

      private void addRuleMap(SolrParams params, Map<String, Object> props, String key) {
        String[] rules = params.getParams(key);
        if (rules != null && rules.length > 0) {
          ArrayList<Map> l = new ArrayList<>();
          for (String rule : rules) l.add(Rule.parseRule(rule));
          props.put(key, l);
        }
      }

      private void createSysConfigSet(CoreContainer coreContainer) throws KeeperException, InterruptedException {
        SolrZkClient zk = coreContainer.getZkController().getZkStateReader().getZkClient();
        createNodeIfNotExists(zk, CONFIGS_ZKNODE, null);
        createNodeIfNotExists(zk, CONFIGS_ZKNODE + "/" + SYSTEM_COLL, null);
        createNodeIfNotExists(zk, CONFIGS_ZKNODE + "/" + SYSTEM_COLL + "/schema.xml",
            BlobHandler.SCHEMA.replaceAll("'", "\"").getBytes(UTF_8));
        createNodeIfNotExists(zk, CONFIGS_ZKNODE + "/" + SYSTEM_COLL + "/solrconfig.xml",
            BlobHandler.CONF.replaceAll("'", "\"").getBytes(UTF_8));
      }
    },
    DELETE_OP(DELETE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler)
          throws Exception {
        return req.getParams().required().getAll(null, NAME);
      }
    },
    RELOAD_OP(RELOAD) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler)
          throws Exception {
        return req.getParams().required().getAll(null, NAME);

      }
    },
    SYNCSHARD_OP(SYNCSHARD) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
          throws Exception {
        String collection = req.getParams().required().get("collection");
        String shard = req.getParams().required().get("shard");

        ClusterState clusterState = h.coreContainer.getZkController().getClusterState();

        ZkNodeProps leaderProps = clusterState.getLeader(collection, shard);
        ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);

        ;
        try (HttpSolrClient client = new HttpSolrClient(nodeProps.getBaseUrl())) {
          client.setConnectionTimeout(15000);
          client.setSoTimeout(60000);
          RequestSyncShard reqSyncShard = new CoreAdminRequest.RequestSyncShard();
          reqSyncShard.setCollection(collection);
          reqSyncShard.setShard(shard);
          reqSyncShard.setCoreName(nodeProps.getCoreName());
          client.request(reqSyncShard);
        }
        return null;
      }

    },
    CREATEALIAS_OP(CREATEALIAS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler)
          throws Exception {
        return req.getParams().required().getAll(null, NAME, "collections");
      }
    },
    DELETEALIAS_OP(DELETEALIAS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler)
          throws Exception {
        return req.getParams().required().getAll(null, NAME);
      }

    },
    SPLITSHARD_OP(SPLITSHARD, DEFAULT_ZK_TIMEOUT * 5) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
          throws Exception {
        String name = req.getParams().required().get(COLLECTION_PROP);
        // TODO : add support for multiple shards
        String shard = req.getParams().get(SHARD_ID_PROP);
        String rangesStr = req.getParams().get(CoreAdminParams.RANGES);
        String splitKey = req.getParams().get("split.key");

        if (splitKey == null && shard == null) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Missing required parameter: shard");
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
            CoreAdminParams.RANGES,
            ASYNC);
        return copyPropertiesWithPrefix(req.getParams(), map, COLL_PROP_PREFIX);
      }
    },
    DELETESHARD_OP(DELETESHARD) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler) throws Exception {
        return req.getParams().required().getAll(null,
            COLLECTION_PROP,
            SHARD_ID_PROP);
      }
    },
    CREATESHARD_OP(CREATESHARD) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null,
            COLLECTION_PROP,
            SHARD_ID_PROP);
        ClusterState clusterState = handler.coreContainer.getZkController().getClusterState();
        if (!ImplicitDocRouter.NAME.equals(((Map) clusterState.getCollection(req.getParams().get(COLLECTION_PROP)).get(DOC_ROUTER)).get(NAME)))
          throw new SolrException(ErrorCode.BAD_REQUEST, "shards can be added only to 'implicit' collections");
        req.getParams().getAll(map,
            REPLICATION_FACTOR,
            CREATE_NODE_SET, ASYNC);
        return copyPropertiesWithPrefix(req.getParams(), map, COLL_PROP_PREFIX);
      }
    },
    DELETEREPLICA_OP(DELETEREPLICA) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null,
            COLLECTION_PROP,
            SHARD_ID_PROP,
            REPLICA_PROP);
        return req.getParams().getAll(map, ASYNC, ONLY_IF_DOWN);
      }
    },
    MIGRATE_OP(MIGRATE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null, COLLECTION_PROP, "split.key", "target.collection");
        return req.getParams().getAll(map, "forward.timeout", ASYNC);
      }
    },
    ADDROLE_OP(ADDROLE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null, "role", "node");
        if (!KNOWN_ROLES.contains(map.get("role")))
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
        return map;
      }
    },
    REMOVEROLE_OP(REMOVEROLE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null, "role", "node");
        if (!KNOWN_ROLES.contains(map.get("role")))
          throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown role. Supported roles are ," + KNOWN_ROLES);
        return map;
      }
    },
    CLUSTERPROP_OP(CLUSTERPROP) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        String name = req.getParams().required().get(NAME);
        String val = req.getParams().get(VALUE_LONG);
        h.coreContainer.getZkController().getZkStateReader().setClusterProperty(name, val);
        return null;
      }
    },
    REQUESTSTATUS_OP(REQUESTSTATUS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        CoreContainer coreContainer = h.coreContainer;
        req.getParams().required().check(REQUESTID);

        String requestId = req.getParams().get(REQUESTID);

        if (requestId.equals("-1")) {
          // Special taskId (-1), clears up the request state maps.
          if (requestId.equals("-1")) {
            coreContainer.getZkController().getOverseerCompletedMap().clear();
            coreContainer.getZkController().getOverseerFailureMap().clear();
            return null;
          }
        } else {
          NamedList<Object> results = new NamedList<>();
          if (coreContainer.getZkController().getOverseerCompletedMap().contains(requestId)) {
            SimpleOrderedMap success = new SimpleOrderedMap();
            success.add("state", "completed");
            success.add("msg", "found " + requestId + " in completed tasks");
            results.add("status", success);
          } else if (coreContainer.getZkController().getOverseerFailureMap().contains(requestId)) {
            SimpleOrderedMap success = new SimpleOrderedMap();
            success.add("state", "failed");
            success.add("msg", "found " + requestId + " in failed tasks");
            results.add("status", success);
          } else if (coreContainer.getZkController().getOverseerRunningMap().contains(requestId)) {
            SimpleOrderedMap success = new SimpleOrderedMap();
            success.add("state", "running");
            success.add("msg", "found " + requestId + " in running tasks");
            results.add("status", success);
          } else if (h.overseerCollectionQueueContains(requestId)) {
            SimpleOrderedMap success = new SimpleOrderedMap();
            success.add("state", "submitted");
            success.add("msg", "found " + requestId + " in submitted tasks");
            results.add("status", success);
          } else {
            SimpleOrderedMap failure = new SimpleOrderedMap();
            failure.add("state", "notfound");
            failure.add("msg", "Did not find taskid [" + requestId + "] in any tasks queue");
            results.add("status", failure);
          }
          SolrResponse response = new OverseerSolrResponse(results);

          rsp.getValues().addAll(response.getResponse());
        }
        return null;
      }

    },
    ADDREPLICA_OP(ADDREPLICA) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h)
          throws Exception {
        Map<String, Object> props = req.getParams().getAll(null,
            COLLECTION_PROP,
            "node",
            SHARD_ID_PROP,
            _ROUTE_,
            CoreAdminParams.NAME,
            INSTANCE_DIR,
            DATA_DIR,
            ASYNC);
        return copyPropertiesWithPrefix(req.getParams(), props, COLL_PROP_PREFIX);
      }
    },
    OVERSEERSTATUS_OP(OVERSEERSTATUS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        return new LinkedHashMap<>();
      }
    },

    /**
     * Handle list collection request.
     * Do list collection request to zk host
     */
    LIST_OP(LIST) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler) throws Exception {
        NamedList<Object> results = new NamedList<>();
        Set<String> collections = handler.coreContainer.getZkController().getZkStateReader().getClusterState().getCollections();
        List<String> collectionList = new ArrayList<>();
        for (String collection : collections) {
          collectionList.add(collection);
        }
        results.add("collections", collectionList);
        SolrResponse response = new OverseerSolrResponse(results);
        rsp.getValues().addAll(response.getResponse());
        return null;
      }
    },
    /**
     * Handle cluster status request.
     * Can return status per specific collection/shard or per all collections.
     */
    CLUSTERSTATUS_OP(CLUSTERSTATUS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler handler)
          throws KeeperException, InterruptedException {
        return req.getParams().getAll(null,
            COLLECTION_PROP,
            SHARD_ID_PROP,
            _ROUTE_);
      }
    },
    ADDREPLICAPROP_OP(ADDREPLICAPROP) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
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
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Overseer replica property command received for property " + property +
                  " with the " + SHARD_UNIQUE +
                  " parameter set to something other than 'true'. No action taken.");
        }
        return map;
      }
    },
    DELETEREPLICAPROP_OP(DELETEREPLICAPROP) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null,
            COLLECTION_PROP,
            PROPERTY_PROP,
            SHARD_ID_PROP,
            REPLICA_PROP);
        return req.getParams().getAll(map, PROPERTY_PROP);
      }
    },
    BALANCESHARDUNIQUE_OP(BALANCESHARDUNIQUE) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        Map<String, Object> map = req.getParams().required().getAll(null,
            COLLECTION_PROP,
            PROPERTY_PROP);
        Boolean shardUnique = Boolean.parseBoolean(req.getParams().get(SHARD_UNIQUE));
        String prop = req.getParams().get(PROPERTY_PROP).toLowerCase(Locale.ROOT);
        if (!StringUtils.startsWith(prop, COLL_PROP_PREFIX)) {
          prop = COLL_PROP_PREFIX + prop;
        }

        if (!shardUnique &&
            !SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(prop)) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Balancing properties amongst replicas in a slice requires that"
              + " the property be pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'. " +
              " Property: " + prop + " shardUnique: " + Boolean.toString(shardUnique));
        }

        return req.getParams().getAll(map, ONLY_ACTIVE_NODES, SHARD_UNIQUE);
      }
    },
    REBALANCELEADERS_OP(REBALANCELEADERS) {
      @Override
      Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception {
        new RebalanceLeaders(req,rsp,h).execute();
        return null;
      }
    };
    CollectionAction action;
    long timeOut;

    CollectionOperation(CollectionAction action) {
      this(action, DEFAULT_ZK_TIMEOUT);
    }

    CollectionOperation(CollectionAction action, long timeOut) {
      this.action = action;
      this.timeOut = timeOut;
    }

    /**
     * All actions must implement this method. If a non null map is returned , the action name is added to
     * the map and sent to overseer for processing. If it returns a null, the call returns immediately
     */
    abstract Map<String, Object> call(SolrQueryRequest req, SolrQueryResponse rsp, CollectionsHandler h) throws Exception;

    public static CollectionOperation get(CollectionAction action) {
      for (CollectionOperation op : values()) {
        if (op.action == action) return op;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "No such action" + action);
    }
  }

}
