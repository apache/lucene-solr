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

/**
 * This is a utility class that sorts cores in such a way as to minimize other cores
 * waiting for replicas in the current node. This helps in avoiding leaderVote timeouts
 * happening in other nodes of the cluster
 *
 */
package org.apache.solr.core;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
public class CoreSorter {
  Map<String, CountsForEachShard> shardsVsReplicaCounts = new LinkedHashMap<>();
  CoreContainer cc;
  private static final CountsForEachShard zero = new CountsForEachShard(0, 0, 0);

  public final static Comparator<CountsForEachShard> countsComparator = (c1, c2) -> {
    if (c1 == null) c1 = zero;//just to avoid  NPE
    if (c2 == null) c2 = zero;
    if (c1.totalReplicasInDownNodes < c2.totalReplicasInDownNodes) {
      //Prioritize replicas with least no:of down nodes waiting.
      //It's better to bring up a node that is a member of a shard
      //with 0 down nodes than 1 down node because it will make the shard
      // complete earlier and avoid waiting by the other live nodes
      if (c1.totalReplicasInLiveNodes > 0) {
        //means nobody else is waiting for this , so no need to prioritize
        return -1;
      }
    }
    if (c2.totalReplicasInDownNodes < c1.totalReplicasInDownNodes) {
      //same is the above, just to take care of the case where c2 has to be prioritized
      if (c2.totalReplicasInLiveNodes > 0) {
        //means nobody else is waiting for this , so no need to priotitize
        return 1;
      }
    }

    //Prioritize replicas where most no:of other nodes are waiting for
    // For example if 1 other replicas are waiting for this replica, then
    // prioritize that over the replica were zero other nodes are waiting
    if (c1.totalReplicasInLiveNodes > c2.totalReplicasInLiveNodes) return -1;
    if (c2.totalReplicasInLiveNodes > c1.totalReplicasInLiveNodes) return 1;

    //If all else is same. prioritize fewer replicas I have because that will complete the
    //quorum for shard faster. If I have only one replica for a shard I can finish it faster
    // than a shard with 2 replicas in this node
    if (c1.myReplicas < c2.myReplicas) return -1;
    if (c2.myReplicas < c1.myReplicas) return 1;
    //if everything is same return 0
    return 0;
  };


  public CoreSorter init(CoreContainer cc) {
    this.cc = cc;
    if (cc == null || !cc.isZooKeeperAware()) {
      return this;
    }
    String myNodeName = getNodeName();
    ClusterState state = cc.getZkController().getClusterState();
    for (CloudDescriptor cloudDescriptor : getCloudDescriptors()) {
      String coll = cloudDescriptor.getCollectionName();
      String sliceName = getShardName(cloudDescriptor);
      if (shardsVsReplicaCounts.containsKey(sliceName)) continue;
      CountsForEachShard c = new CountsForEachShard(0, 0, 0);
      for (Replica replica : getReplicas(state, coll, cloudDescriptor.getShardId())) {
        if (replica.getNodeName().equals(myNodeName)) {
          c.myReplicas++;
        } else {
          Set<String> liveNodes = state.getLiveNodes();
          if (liveNodes.contains(replica.getNodeName())) {
            c.totalReplicasInLiveNodes++;
          } else {
            c.totalReplicasInDownNodes++;
          }
        }
      }
      shardsVsReplicaCounts.put(sliceName, c);
    }

    return this;

  }


  public int compare(CoreDescriptor cd1, CoreDescriptor cd2) {
    String s1 = getShardName(cd1.getCloudDescriptor());
    String s2 = getShardName(cd2.getCloudDescriptor());
    if (s1 == null || s2 == null) return cd1.getName().compareTo(cd2.getName());
    CountsForEachShard c1 = shardsVsReplicaCounts.get(s1);
    CountsForEachShard c2 = shardsVsReplicaCounts.get(s2);
    int result = countsComparator.compare(c1, c2);
    return result == 0 ? s1.compareTo(s2) : result;
  }


  static class CountsForEachShard {
    public int totalReplicasInDownNodes = 0, myReplicas = 0, totalReplicasInLiveNodes = 0;

    public CountsForEachShard(int totalReplicasInDownNodes,  int totalReplicasInLiveNodes,int myReplicas) {
      this.totalReplicasInDownNodes = totalReplicasInDownNodes;
      this.myReplicas = myReplicas;
      this.totalReplicasInLiveNodes = totalReplicasInLiveNodes;
    }

    public boolean equals(Object obj) {
      if (obj instanceof CountsForEachShard) {
        CountsForEachShard that = (CountsForEachShard) obj;
        return that.totalReplicasInDownNodes == totalReplicasInDownNodes && that.myReplicas == myReplicas;

      }
      return false;
    }

    @Override
    public String toString() {
      return "down : " + totalReplicasInDownNodes + " , up :  " + totalReplicasInLiveNodes + " my : " + myReplicas;
    }


  }

  static String getShardName(CloudDescriptor cd) {
    return cd == null ?
        null :
        cd.getCollectionName()
            + "_"
            + cd.getShardId();
  }


  String getNodeName() {
    return cc.getNodeConfig().getNodeName();
  }

  /**Return all replicas for a given collection+slice combo
   */
  Collection<Replica> getReplicas(ClusterState cs, String coll, String slice) {
    DocCollection c = cs.getCollectionOrNull(coll);
    if (c == null) return emptyList();
    Slice s = c.getSlice(slice);
    if (s == null) return emptyList();
    return s.getReplicas();
  }


  /**return cloud descriptors for all cores in this node
   */
  Collection<CloudDescriptor> getCloudDescriptors() {
    return cc.getCores()
        .stream()
        .map((core) -> core.getCoreDescriptor().getCloudDescriptor())
        .collect(toList());
  }


}
