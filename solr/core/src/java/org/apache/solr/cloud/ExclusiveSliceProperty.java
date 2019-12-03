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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.SHARD_UNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;

// Class to encapsulate processing replica properties that have at most one replica hosting a property per slice.
class ExclusiveSliceProperty {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private ClusterState clusterState;
  private final boolean onlyActiveNodes;
  private final String property;
  private final DocCollection collection;
  private final String collectionName;

  // Key structure. For each node, list all replicas on it regardless of whether they have the property or not.
  private final Map<String, List<SliceReplica>> nodesHostingReplicas = new HashMap<>();
  // Key structure. For each node, a list of the replicas _currently_ hosting the property.
  private final Map<String, List<SliceReplica>> nodesHostingProp = new HashMap<>();
  Set<String> shardsNeedingHosts = new HashSet<>();
  Map<String, Slice> changedSlices = new HashMap<>(); // Work on copies rather than the underlying cluster state.

  private int origMaxPropPerNode = 0;
  private int origModulo = 0;
  private int tmpMaxPropPerNode = 0;
  private int tmpModulo = 0;
  Random rand = new Random();

  private int assigned = 0;

  ExclusiveSliceProperty(ClusterState clusterState, ZkNodeProps message) {
    this.clusterState = clusterState;
    String tmp = message.getStr(ZkStateReader.PROPERTY_PROP);
    if (StringUtils.startsWith(tmp, OverseerCollectionMessageHandler.COLL_PROP_PREFIX) == false) {
      tmp = OverseerCollectionMessageHandler.COLL_PROP_PREFIX + tmp;
    }
    this.property = tmp.toLowerCase(Locale.ROOT);
    collectionName = message.getStr(ZkStateReader.COLLECTION_PROP);

    if (StringUtils.isBlank(collectionName) || StringUtils.isBlank(property)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Overseer '" + message.getStr(Overseer.QUEUE_OPERATION) + "'  requires both the '" + ZkStateReader.COLLECTION_PROP + "' and '" +
              ZkStateReader.PROPERTY_PROP + "' parameters. No action taken ");
    }

    Boolean shardUnique = Boolean.parseBoolean(message.getStr(SHARD_UNIQUE));
    if (shardUnique == false &&
        SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(this.property) == false) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Balancing properties amongst replicas in a slice requires that"
          + " the property be a pre-defined property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true' " +
          " Property: " + this.property + " shardUnique: " + Boolean.toString(shardUnique));
    }

    collection = clusterState.getCollection(collectionName);
    if (collection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Could not find collection ' " + collectionName + "' for overseer operation '" +
              message.getStr(Overseer.QUEUE_OPERATION) + "'. No action taken.");
    }
    onlyActiveNodes = Boolean.parseBoolean(message.getStr(ONLY_ACTIVE_NODES, "true"));
  }


  DocCollection getDocCollection() {
    return collection;
  }

  private boolean isActive(Replica replica) {
    return replica.getState() == Replica.State.ACTIVE;
  }

  // Collect a list of all the nodes that _can_ host the indicated property. Along the way, also collect any of
  // the replicas on that node that _already_ host the property as well as any slices that do _not_ have the
  // property hosted.
  //
  // Return true if anything node needs it's property reassigned. False if the property is already balanced for
  // the collection.

  private boolean collectCurrentPropStats() {
    int maxAssigned = 0;
    // Get a list of potential replicas that can host the property _and_ their counts
    // Move any obvious entries to a list of replicas to change the property on
    Set<String> allHosts = new HashSet<>();
    for (Slice slice : collection.getSlices()) {
      boolean sliceHasProp = false;
      for (Replica replica : slice.getReplicas()) {
        if (onlyActiveNodes && isActive(replica) == false) {
          if (StringUtils.isNotBlank(replica.getStr(property))) {
            removeProp(slice, replica.getName()); // Note, we won't be committing this to ZK until later.
          }
          continue;
        }
        allHosts.add(replica.getNodeName());
        String nodeName = replica.getNodeName();
        if (StringUtils.isNotBlank(replica.getStr(property))) {
          if (sliceHasProp) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "'" + BALANCESHARDUNIQUE + "' should only be called for properties that have at most one member " +
                    "in any slice with the property set. No action taken.");
          }
          if (nodesHostingProp.containsKey(nodeName) == false) {
            nodesHostingProp.put(nodeName, new ArrayList<>());
          }
          nodesHostingProp.get(nodeName).add(new SliceReplica(slice, replica));
          ++assigned;
          maxAssigned = Math.max(maxAssigned, nodesHostingProp.get(nodeName).size());
          sliceHasProp = true;
        }
        if (nodesHostingReplicas.containsKey(nodeName) == false) {
          nodesHostingReplicas.put(nodeName, new ArrayList<>());
        }
        nodesHostingReplicas.get(nodeName).add(new SliceReplica(slice, replica));
      }
    }

    // If the total number of already-hosted properties assigned to nodes
    // that have potential to host leaders is equal to the slice count _AND_ none of the current nodes has more than
    // the max number of properties, there's nothing to do.
    origMaxPropPerNode = collection.getSlices().size() / allHosts.size();

    // Some nodes can have one more of the proeprty if the numbers aren't exactly even.
    origModulo = collection.getSlices().size() % allHosts.size();
    if (origModulo > 0) {
      origMaxPropPerNode++;  // have to have some nodes with 1 more property.
    }

    // We can say for sure that we need to rebalance if we don't have as many assigned properties as slices.
    if (assigned != collection.getSlices().size()) {
      return true;
    }

    // Make sure there are no more slices at the limit than the "leftovers"
    // Let's say there's 7 slices and 3 nodes. We need to distribute the property as 3 on node1, 2 on node2 and 2 on node3
    // (3, 2, 2) We need to be careful to not distribute them as 3, 3, 1. that's what this check is all about.
    int counter = origModulo;
    for (List<SliceReplica> list : nodesHostingProp.values()) {
      if (list.size() == origMaxPropPerNode) --counter;
    }
    if (counter == 0) return false; // nodes with 1 extra leader are exactly the needed number

    return true;
  }

  private void removeSliceAlreadyHostedFromPossibles(String sliceName) {
    for (Map.Entry<String, List<SliceReplica>> entReplica : nodesHostingReplicas.entrySet()) {

      ListIterator<SliceReplica> iter = entReplica.getValue().listIterator();
      while (iter.hasNext()) {
        SliceReplica sr = iter.next();
        if (sr.slice.getName().equals(sliceName))
          iter.remove();
      }
    }
  }

  private void balanceUnassignedReplicas() {
    tmpMaxPropPerNode = origMaxPropPerNode; // A bit clumsy, but don't want to duplicate code.
    tmpModulo = origModulo;

    // Get the nodeName and shardName for the node that has the least room for this

    while (shardsNeedingHosts.size() > 0) {
      String nodeName = "";
      int minSize = Integer.MAX_VALUE;
      SliceReplica srToChange = null;
      for (String slice : shardsNeedingHosts) {
        for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingReplicas.entrySet()) {
          // A little tricky. If we don't set this to something below, then it means all possible places to
          // put this property are full up, so just put it somewhere.
          if (srToChange == null && ent.getValue().size() > 0) {
            srToChange = ent.getValue().get(0);
          }
          ListIterator<SliceReplica> iter = ent.getValue().listIterator();
          while (iter.hasNext()) {
            SliceReplica sr = iter.next();
            if (StringUtils.equals(slice, sr.slice.getName()) == false) {
              continue;
            }
            if (nodesHostingProp.containsKey(ent.getKey()) == false) {
              nodesHostingProp.put(ent.getKey(), new ArrayList<SliceReplica>());
            }
            if (minSize > nodesHostingReplicas.get(ent.getKey()).size() && nodesHostingProp.get(ent.getKey()).size() < tmpMaxPropPerNode) {
              minSize = nodesHostingReplicas.get(ent.getKey()).size();
              srToChange = sr;
              nodeName = ent.getKey();
            }
          }
        }
      }
      // Now, you have a slice and node to put it on
      shardsNeedingHosts.remove(srToChange.slice.getName());
      if (nodesHostingProp.containsKey(nodeName) == false) {
        nodesHostingProp.put(nodeName, new ArrayList<SliceReplica>());
      }
      nodesHostingProp.get(nodeName).add(srToChange);
      adjustLimits(nodesHostingProp.get(nodeName));
      removeSliceAlreadyHostedFromPossibles(srToChange.slice.getName());
      addProp(srToChange.slice, srToChange.replica.getName());
      // When you set the property, you must insure that it is _removed_ from any other replicas.
      for (Replica rep : srToChange.slice.getReplicas()) {
        if (rep.getName().equals(srToChange.replica.getName())) {
          continue;
        }
        if (rep.getProperty(property) != null) {
          removeProp(srToChange.slice, srToChange.replica.getName());
        }
      }
    }
  }

  // Adjust the min/max counts per allowed per node. Special handling here for dealing with the fact
  // that no node should have more than 1 more replica with this property than any other.
  private void adjustLimits(List<SliceReplica> changeList) {
    if (changeList.size() == tmpMaxPropPerNode) {
      if (tmpModulo < 0) return;

      --tmpModulo;
      if (tmpModulo == 0) {
        --tmpMaxPropPerNode;
        --tmpModulo;  // Prevent dropping tmpMaxPropPerNode again.
      }
    }
  }

  // Go through the list of presently-hosted properties and remove any that have too many replicas that host the property
  private void removeOverallocatedReplicas() {
    tmpMaxPropPerNode = origMaxPropPerNode; // A bit clumsy, but don't want to duplicate code.
    tmpModulo = origModulo;

    for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingProp.entrySet()) {
      while (ent.getValue().size() > tmpMaxPropPerNode) { // remove delta nodes
        ent.getValue().remove(rand.nextInt(ent.getValue().size()));
      }
      adjustLimits(ent.getValue());
    }
  }

  private void removeProp(Slice origSlice, String replicaName) {
    log.debug("Removing property {} from slice {}, replica {}", property, origSlice.getName(), replicaName);
    getReplicaFromChanged(origSlice, replicaName).getProperties().remove(property);
  }

  private void addProp(Slice origSlice, String replicaName) {
    log.debug("Adding property {} to slice {}, replica {}", property, origSlice.getName(), replicaName);
    getReplicaFromChanged(origSlice, replicaName).getProperties().put(property, "true");
  }

  // Just a place to encapsulate the fact that we need to have new slices (copy) to update before we
  // put this all in the cluster state.
  private Replica getReplicaFromChanged(Slice origSlice, String replicaName) {
    Slice newSlice = changedSlices.get(origSlice.getName());
    Replica replica;
    if (newSlice != null) {
      replica = newSlice.getReplica(replicaName);
    } else {
      newSlice = new Slice(origSlice.getName(), origSlice.getReplicasCopy(), origSlice.shallowCopy(), origSlice.collection);
      changedSlices.put(origSlice.getName(), newSlice);
      replica = newSlice.getReplica(replicaName);
    }
    if (replica == null) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "Should have been able to find replica '" +
          replicaName + "' in slice '" + origSlice.getName() + "'. No action taken");
    }
    return replica;

  }
  // Main entry point for carrying out the action. Returns "true" if we have actually moved properties around.

  boolean balanceProperty() {
    if (collectCurrentPropStats() == false) {
      return false;
    }

    // we have two lists based on nodeName
    // 1> all the nodes that _could_ host a property for the slice
    // 2> all the nodes that _currently_ host a property for the slice.

    // So, remove a replica from the nodes that have too many
    removeOverallocatedReplicas();

    // prune replicas belonging to a slice that have the property currently assigned from the list of replicas
    // that could host the property.
    for (Map.Entry<String, List<SliceReplica>> entProp : nodesHostingProp.entrySet()) {
      for (SliceReplica srHosting : entProp.getValue()) {
        removeSliceAlreadyHostedFromPossibles(srHosting.slice.getName());
      }
    }

    // Assemble the list of slices that do not have any replica hosting the property:
    for (Map.Entry<String, List<SliceReplica>> ent : nodesHostingReplicas.entrySet()) {
      ListIterator<SliceReplica> iter = ent.getValue().listIterator();
      while (iter.hasNext()) {
        SliceReplica sr = iter.next();
        shardsNeedingHosts.add(sr.slice.getName());
      }
    }

    // At this point, nodesHostingProp should contain _only_ lists of replicas that belong to slices that do _not_
    // have any replica hosting the property. So let's assign them.

    balanceUnassignedReplicas();
    for (Slice newSlice : changedSlices.values()) {
      DocCollection docCollection = CollectionMutator.updateSlice(collectionName, clusterState.getCollection(collectionName), newSlice);
      clusterState = ClusterStateMutator.newState(clusterState, collectionName, docCollection);
    }
    return true;
  }

  private static class SliceReplica {
    Slice slice;
    Replica replica;

    SliceReplica(Slice slice, Replica replica) {
      this.slice = slice;
      this.replica = replica;
    }
    public String toString() {
      StringBuilder sb = new StringBuilder(System.lineSeparator()).append(System.lineSeparator()).append("******EOE20 starting toString of SliceReplica");
      sb.append("    :").append(System.lineSeparator()).append("slice: ").append(slice.toString()).append(System.lineSeparator()).append("      replica: ").append(replica.toString()).append(System.lineSeparator());
      return sb.toString();
    }
  }
}
