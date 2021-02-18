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
import java.util.Optional;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public List<ZkWriteCommand> downNode(ClusterState clusterState, ZkNodeProps message) {
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);

    log.debug("DownNode state invoked for node: {}", nodeName);

    List<ZkWriteCommand> zkWriteCommands = new ArrayList<>();

    Map<String, DocCollection> collections = clusterState.getCollectionsMap();
    for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
      String collectionName = entry.getKey();
      DocCollection docCollection = entry.getValue();

      Optional<ZkWriteCommand> zkWriteCommand = computeCollectionUpdate(nodeName, collectionName, docCollection);

      if (zkWriteCommand.isPresent()) {
        zkWriteCommands.add(zkWriteCommand.get());
      }
    }

    return zkWriteCommands;
  }

  /**
   * Returns the write command needed to update the replicas of a given collection given the identity of a node being down.
   * @return An optional with the write command or an empty one if the collection does not need any state modification.
   *    The returned write command might be for per replica state updates or for an update to state.json, depending on the
   *    configuration of the collection.
   */
  public static Optional<ZkWriteCommand> computeCollectionUpdate(String nodeName, String collectionName, DocCollection docCollection) {
    boolean needToUpdateCollection = false;
    List<String> downedReplicas = new ArrayList<>();
    Map<String,Slice> slicesCopy = new LinkedHashMap<>(docCollection.getSlicesMap());

    for (Entry<String, Slice> sliceEntry : slicesCopy.entrySet()) {
      Slice slice = sliceEntry.getValue();
      Map<String, Replica> newReplicas = slice.getReplicasCopy();

      Collection<Replica> replicas = slice.getReplicas();
      for (Replica replica : replicas) {
        String rNodeName = replica.getNodeName();
        if (rNodeName == null) {
          throw new RuntimeException("Replica without node name! " + replica);
        }
        if (rNodeName.equals(nodeName)) {
          log.debug("Update replica state for {} to {}", replica, Replica.State.DOWN);
          Map<String, Object> props = replica.shallowCopy();
          Replica newReplica = new Replica(replica.getName(), replica.node, replica.collection, slice.getName(), replica.core,
              Replica.State.DOWN, replica.type, props);
          newReplicas.put(replica.getName(), newReplica);
          needToUpdateCollection = true;
          downedReplicas.add(replica.getName());
        }
      }

      Slice newSlice = new Slice(slice.getName(), newReplicas, slice.shallowCopy(),collectionName);
      slicesCopy.put(slice.getName(), newSlice);
    }

    if (needToUpdateCollection) {
      if (docCollection.isPerReplicaState()) {
        return Optional.of(new ZkWriteCommand(collectionName, docCollection.copyWithSlices(slicesCopy),
            PerReplicaStatesOps.downReplicas(downedReplicas, docCollection.getPerReplicaStates()), false));
      } else {
        return Optional.of(new ZkWriteCommand(collectionName, docCollection.copyWithSlices(slicesCopy)));
      }
    } else {
      // No update needed for this collection
      return Optional.empty();
    }
  }
}

