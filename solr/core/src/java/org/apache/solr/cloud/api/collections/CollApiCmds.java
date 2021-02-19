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

package org.apache.solr.cloud.api.collections;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.UrlScheme;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.ELECTION_NODE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NODE_NAME_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REJOIN_AT_HEAD_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * This class contains "smaller" Collection API commands implementation as well as the interface implemented by all commands.
 * Previously these implementations in {@link OverseerCollectionMessageHandler} were relying on methods implementing the
 * functional interface.
 */
public class CollApiCmds {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Interface implemented by all Collection API commands. Collection API commands are defined in classes whose names ends in {@code Cmd}.
   */
  protected interface CollectionApiCommand {
    void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception;
  }

  static public class MockOperationCmd implements CollectionApiCommand {
    @SuppressForbidden(reason = "Needs currentTimeMillis for mock requests")
    @SuppressWarnings({"unchecked"})
    public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws InterruptedException {
      //only for test purposes
      Thread.sleep(message.getInt("sleep", 1));
      if (log.isInfoEnabled()) {
        log.info("MOCK_TASK_EXECUTED time {} data {}", System.currentTimeMillis(), Utils.toJSONString(message));
      }
      results.add("MOCK_FINISHED", System.currentTimeMillis());
    }
  }

  static public class ReloadCollectionCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public ReloadCollectionCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings({"unchecked"})
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.RELOAD.toString());

      String asyncId = message.getStr(ASYNC);
      CollectionHandlingUtils.collectionCmd(message, params, results, Replica.State.ACTIVE, asyncId, Collections.emptySet(), ccc, clusterState);
    }
  }

  static public class RebalanceLeadersCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public RebalanceLeadersCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings("unchecked")
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results)
        throws Exception {
      CollectionHandlingUtils.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, CORE_NAME_PROP, ELECTION_NODE_PROP,
          CORE_NODE_NAME_PROP, NODE_NAME_PROP, REJOIN_AT_HEAD_PROP);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(COLLECTION_PROP, message.getStr(COLLECTION_PROP));
      params.set(SHARD_ID_PROP, message.getStr(SHARD_ID_PROP));
      params.set(REJOIN_AT_HEAD_PROP, message.getStr(REJOIN_AT_HEAD_PROP));
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REJOINLEADERELECTION.toString());
      params.set(CORE_NAME_PROP, message.getStr(CORE_NAME_PROP));
      params.set(CORE_NODE_NAME_PROP, message.getStr(CORE_NODE_NAME_PROP));
      params.set(ELECTION_NODE_PROP, message.getStr(ELECTION_NODE_PROP));
      params.set(NODE_NAME_PROP, message.getStr(NODE_NAME_PROP));

      String baseUrl = UrlScheme.INSTANCE.getBaseUrlForNodeName(message.getStr(NODE_NAME_PROP));
      ShardRequest sreq = new ShardRequest();
      sreq.nodeName = message.getStr(ZkStateReader.CORE_NAME_PROP);
      // yes, they must use same admin handler path everywhere...
      params.set("qt", ccc.getAdminPath());
      sreq.purpose = ShardRequest.PURPOSE_PRIVATE;
      sreq.shards = new String[]{baseUrl};
      sreq.actualShards = sreq.shards;
      sreq.params = params;
      ShardHandler shardHandler = ccc.getShardHandler();
      shardHandler.submit(sreq, baseUrl, sreq.params);
    }
  }

  static public class AddReplicaPropCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public AddReplicaPropCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    @SuppressWarnings("unchecked")
    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
        throws Exception {
      CollectionHandlingUtils.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP, PROPERTY_VALUE_PROP);
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, ADDREPLICAPROP.toLower());
      propMap.putAll(message.getProperties());
      ZkNodeProps m = new ZkNodeProps(propMap);
      if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
        ccc.getDistributedClusterStateUpdater().doSingleStateUpdate(DistributedClusterStateUpdater.MutatingCommand.ReplicaAddReplicaProperty, m,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }
  }

  static public class DeleteReplicaPropCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public DeleteReplicaPropCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results)
        throws Exception {
      CollectionHandlingUtils.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP, PROPERTY_PROP);
      Map<String, Object> propMap = new HashMap<>();
      propMap.put(Overseer.QUEUE_OPERATION, DELETEREPLICAPROP.toLower());
      propMap.putAll(message.getProperties());
      ZkNodeProps m = new ZkNodeProps(propMap);
      if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
        ccc.getDistributedClusterStateUpdater().doSingleStateUpdate(DistributedClusterStateUpdater.MutatingCommand.ReplicaDeleteReplicaProperty, m,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }
  }

  static public class BalanceShardsUniqueCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public BalanceShardsUniqueCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {
      if (StringUtils.isBlank(message.getStr(COLLECTION_PROP)) || StringUtils.isBlank(message.getStr(PROPERTY_PROP))) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "The '" + COLLECTION_PROP + "' and '" + PROPERTY_PROP +
                "' parameters are required for the BALANCESHARDUNIQUE operation, no action taken");
      }
      Map<String, Object> m = new HashMap<>();
      m.put(Overseer.QUEUE_OPERATION, BALANCESHARDUNIQUE.toLower());
      m.putAll(message.getProperties());
      if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
        ccc.getDistributedClusterStateUpdater().doSingleStateUpdate(DistributedClusterStateUpdater.MutatingCommand.BalanceShardsUnique, new ZkNodeProps(m),
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(m));
      }
    }
  }

  static public class ModifyCollectionCmd implements CollectionApiCommand {
    private final CollectionCommandContext ccc;

    public ModifyCollectionCmd(CollectionCommandContext ccc) {
      this.ccc = ccc;
    }

    public void call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results) throws Exception {

      final String collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);
      //the rest of the processing is based on writing cluster state properties
      //remove the property here to avoid any errors down the pipeline due to this property appearing
      String configName = (String) message.getProperties().remove(CollectionAdminParams.COLL_CONF);

      if (configName != null) {
        CollectionHandlingUtils.validateConfigOrThrowSolrException(ccc.getSolrCloudManager(), configName);

        CollectionHandlingUtils.createConfNode(ccc.getSolrCloudManager().getDistribStateManager(), configName, collectionName);
        new ReloadCollectionCmd(ccc).call(clusterState, new ZkNodeProps(NAME, collectionName), results);
      }

      if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
        // Apply the state update right away. The wait will still be useful for the change to be visible in the local cluster state (watchers have fired).
        ccc.getDistributedClusterStateUpdater().doSingleStateUpdate(DistributedClusterStateUpdater.MutatingCommand.CollectionModifyCollection, message,
            ccc.getSolrCloudManager(), ccc.getZkStateReader());
      } else {
        ccc.offerStateUpdate(Utils.toJSON(message));
      }

      try {
        ccc.getZkStateReader().waitForState(collectionName, 30, TimeUnit.SECONDS, c -> {
          if (c == null) return false;

          for (Map.Entry<String, Object> updateEntry : message.getProperties().entrySet()) {
            String updateKey = updateEntry.getKey();

            if (!updateKey.equals(ZkStateReader.COLLECTION_PROP)
                && !updateKey.equals(Overseer.QUEUE_OPERATION)
                && updateEntry.getValue() != null // handled below in a separate conditional
                && !updateEntry.getValue().equals(c.get(updateKey))) {
              return false;
            }
            if (updateEntry.getValue() == null && c.containsKey(updateKey)) {
              return false;
            }
          }

          return true;
        });
      } catch (TimeoutException | InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        log.debug("modifyCollection(ClusterState={}, ZkNodeProps={}, NamedList={})", clusterState, message, results, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to modify collection", e);
      }

      // if switching to/from read-only mode reload the collection
      if (message.keySet().contains(ZkStateReader.READ_ONLY)) {
        new ReloadCollectionCmd(ccc).call(clusterState, new ZkNodeProps(NAME, collectionName), results);
      }
    }
  }
}
