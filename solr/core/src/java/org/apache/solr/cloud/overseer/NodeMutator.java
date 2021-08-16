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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected SolrZkClient zkClient;
  public NodeMutator(SolrCloudManager cloudManager) {
    zkClient = SliceMutator.getZkClient(cloudManager);
  }

  public List<ZkWriteCommand> downNode(ClusterState clusterState, ZkNodeProps message) {
    List<ZkWriteCommand> zkWriteCommands = new ArrayList<>();
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);

    log.debug("DownNode state invoked for node: {}", nodeName);

    Map<String, DocCollection> collections = clusterState.getCollectionsMap();
    for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
       List<String> downedReplicas = new ArrayList<>();

      String collection = entry.getKey();
      DocCollection docCollection = entry.getValue();

      Map<String,Slice> slicesCopy = new LinkedHashMap<>(docCollection.getSlicesMap());

      boolean needToUpdateCollection = false;
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
            props.put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
            Replica newReplica = new Replica(replica.getName(), props, collection, slice.getName());
            newReplicas.put(replica.getName(), newReplica);
            needToUpdateCollection = true;
            downedReplicas.add(replica.getName());
          }
        }

        Slice newSlice = new Slice(slice.getName(), newReplicas, slice.shallowCopy(),collection);
        sliceEntry.setValue(newSlice);
      }

      if (needToUpdateCollection) {
        if (docCollection.isPerReplicaState()) {
          zkWriteCommands.add(new ZkWriteCommand(collection, docCollection.copyWithSlices(slicesCopy),
              PerReplicaStatesOps.downReplicas(downedReplicas,
                  PerReplicaStates.fetch(docCollection.getZNode(), zkClient, docCollection.getPerReplicaStates())), false));
        } else {
          zkWriteCommands.add(new ZkWriteCommand(collection, docCollection.copyWithSlices(slicesCopy)));
        }
      }
    }

    return zkWriteCommands;
  }
}

