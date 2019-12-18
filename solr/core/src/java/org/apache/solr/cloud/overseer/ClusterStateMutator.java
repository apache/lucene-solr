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
package org.apache.solr.cloud.overseer;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

public class ClusterStateMutator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  protected final SolrCloudManager dataProvider;
  protected final DistribStateManager stateManager;

  public ClusterStateMutator(SolrCloudManager dataProvider) {
    this.dataProvider = dataProvider;
    this.stateManager = dataProvider.getDistribStateManager();
  }

  public ZkWriteCommand createCollection(ClusterState clusterState, ZkNodeProps message) {
    String cName = message.getStr(NAME);
    log.debug("building a new cName: " + cName);
    if (clusterState.hasCollection(cName)) {
      log.warn("Collection {} already exists. exit", cName);
      return ZkStateWriter.NO_OP;
    }

    Map<String, Object> routerSpec = DocRouter.getRouterSpec(message);
    String routerName = routerSpec.get(NAME) == null ? DocRouter.DEFAULT_NAME : (String) routerSpec.get(NAME);
    DocRouter router = DocRouter.getDocRouter(routerName);

    Object messageShardsObj = message.get("shards");

    Map<String, Slice> slices;
    if (messageShardsObj instanceof Map) { // we are being explicitly told the slice data (e.g. coll restore)
      slices = Slice.loadAllFromMap(cName, (Map<String, Object>)messageShardsObj);
    } else {
      List<String> shardNames = new ArrayList<>();

      if (router instanceof ImplicitDocRouter) {
        getShardNames(shardNames, message.getStr("shards", DocRouter.DEFAULT_NAME));
      } else {
        int numShards = message.getInt(ZkStateReader.NUM_SHARDS_PROP, -1);
        if (numShards < 1)
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "numShards is a required parameter for 'compositeId' router");
        getShardNames(numShards, shardNames);
      }
      List<DocRouter.Range> ranges = router.partitionRange(shardNames.size(), router.fullRange());//maybe null

      slices = new LinkedHashMap<>();
      for (int i = 0; i < shardNames.size(); i++) {
        String sliceName = shardNames.get(i);

        Map<String, Object> sliceProps = new LinkedHashMap<>(1);
        sliceProps.put(Slice.RANGE, ranges == null ? null : ranges.get(i));

        slices.put(sliceName, new Slice(sliceName, null, sliceProps,cName));
      }
    }

    Map<String, Object> collectionProps = new HashMap<>();

    for (Map.Entry<String, Object> e : OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.entrySet()) {
      Object val = message.get(e.getKey());
      if (val == null) {
        val = OverseerCollectionMessageHandler.COLLECTION_PROPS_AND_DEFAULTS.get(e.getKey());
      }
      if (val != null) collectionProps.put(e.getKey(), val);
    }
    collectionProps.put(DocCollection.DOC_ROUTER, routerSpec);

    if (message.getStr("fromApi") == null) {
      collectionProps.put("autoCreated", "true");
    }

    //TODO default to 2; but need to debug why BasicDistributedZk2Test fails early on
    String znode = message.getInt(DocCollection.STATE_FORMAT, 1) == 1 ? null
        : ZkStateReader.getCollectionPath(cName);

    DocCollection newCollection = new DocCollection(cName,
        slices, collectionProps, router, -1, znode);

    return new ZkWriteCommand(cName, newCollection);
  }

  public ZkWriteCommand deleteCollection(ClusterState clusterState, ZkNodeProps message) {
    final String collection = message.getStr(NAME);
    if (!CollectionMutator.checkKeyExistence(message, NAME)) return ZkStateWriter.NO_OP;
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null) return ZkStateWriter.NO_OP;

    return new ZkWriteCommand(coll.getName(), null);
  }

  public static ClusterState newState(ClusterState state, String name, DocCollection collection) {
    ClusterState newClusterState = null;
    if (collection == null) {
      newClusterState = state.copyWith(name, null);
    } else {
      newClusterState = state.copyWith(name, collection);
    }
    return newClusterState;
  }

  public static void getShardNames(Integer numShards, List<String> shardNames) {
    if (numShards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "numShards" + " is a required param");
    for (int i = 0; i < numShards; i++) {
      final String sliceName = "shard" + (i + 1);
      shardNames.add(sliceName);
    }

  }

  public static void getShardNames(List<String> shardNames, String shards) {
    if (shards == null)
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
    for (String s : shards.split(",")) {
      if (s == null || s.trim().isEmpty()) continue;
      shardNames.add(s.trim());
    }
    if (shardNames.isEmpty())
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "shards" + " is a required param");
  }

  /*
       * Return an already assigned id or null if not assigned
       */
  public static String getAssignedId(final DocCollection collection, final String nodeName) {
    Collection<Slice> slices = collection != null ? collection.getSlices() : null;
    if (slices != null) {
      for (Slice slice : slices) {
        if (slice.getReplicasMap().get(nodeName) != null) {
          return slice.getName();
        }
      }
    }
    return null;
  }

  public static String getAssignedCoreNodeName(DocCollection collection, String forNodeName, String forCoreName) {
    Collection<Slice> slices = collection != null ? collection.getSlices() : null;
    if (slices != null) {
      for (Slice slice : slices) {
        for (Replica replica : slice.getReplicas()) {
          String nodeName = replica.getStr(ZkStateReader.NODE_NAME_PROP);
          String core = replica.getStr(ZkStateReader.CORE_NAME_PROP);

          if (nodeName.equals(forNodeName) && core.equals(forCoreName)) {
            return replica.getName();
          }
        }
      }
    }
    return null;
  }

  public ZkWriteCommand migrateStateFormat(ClusterState clusterState, ZkNodeProps message) {
    final String collection = message.getStr(ZkStateReader.COLLECTION_PROP);
    if (!CollectionMutator.checkKeyExistence(message, ZkStateReader.COLLECTION_PROP)) return ZkStateWriter.NO_OP;
    DocCollection coll = clusterState.getCollectionOrNull(collection);
    if (coll == null || coll.getStateFormat() == 2) return ZkStateWriter.NO_OP;

    return new ZkWriteCommand(coll.getName(),
        new DocCollection(coll.getName(), coll.getSlicesMap(), coll.getProperties(), coll.getRouter(), 0,
            ZkStateReader.getCollectionPath(collection)));
  }
}

