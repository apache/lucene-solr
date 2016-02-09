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
import java.util.Set;
import java.util.Map.Entry;

import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public NodeMutator(ZkStateReader zkStateReader) {

  }

  public List<ZkWriteCommand> downNode(ClusterState clusterState, ZkNodeProps message) {
    List<ZkWriteCommand> zkWriteCommands = new ArrayList<ZkWriteCommand>();
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);

    log.info("DownNode state invoked for node: " + nodeName);

    Set<String> collections = clusterState.getCollections();
    for (String collection : collections) {

      Map<String,Slice> slicesCopy = new LinkedHashMap<>(clusterState.getSlicesMap(collection));

      Set<Entry<String,Slice>> entries = slicesCopy.entrySet();
      for (Entry<String,Slice> entry : entries) {
        Slice slice = clusterState.getSlice(collection, entry.getKey());
        Map<String,Replica> newReplicas = new HashMap<String,Replica>();

        Collection<Replica> replicas = slice.getReplicas();
        for (Replica replica : replicas) {
          Map<String,Object> props = replica.shallowCopy();
          String rNodeName = replica.getNodeName();
          if (rNodeName.equals(nodeName)) {
            log.info("Update replica state for " + replica + " to " + Replica.State.DOWN.toString());
            props.put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
          }

          Replica newReplica = new Replica(replica.getName(), props);
          newReplicas.put(replica.getName(), newReplica);
        }

        Slice newSlice = new Slice(slice.getName(), newReplicas, slice.shallowCopy());
        slicesCopy.put(slice.getName(), newSlice);

      }

      zkWriteCommands.add(new ZkWriteCommand(collection, clusterState.getCollection(collection).copyWithSlices(slicesCopy)));
    }

    return zkWriteCommands;
  }
}

