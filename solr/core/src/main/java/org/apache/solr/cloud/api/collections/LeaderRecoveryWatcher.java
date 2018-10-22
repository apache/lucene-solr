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

import java.util.Set;

import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;

/**
 * We use this watcher to wait for any eligible replica in a shard to become active so that it can become a leader.
 */
public class LeaderRecoveryWatcher implements CollectionStateWatcher {
  String collectionId;
  String shardId;
  String replicaId;
  String targetCore;
  SolrCloseableLatch latch;

  /**
   * Watch for recovery of a replica
   *
   * @param collectionId   collection name
   * @param shardId        shard id
   * @param replicaId      source replica name (coreNodeName)
   * @param targetCore     specific target core name - if null then any active replica will do
   * @param latch countdown when recovered
   */
  LeaderRecoveryWatcher(String collectionId, String shardId, String replicaId, String targetCore, SolrCloseableLatch latch) {
    this.collectionId = collectionId;
    this.shardId = shardId;
    this.replicaId = replicaId;
    this.targetCore = targetCore;
    this.latch = latch;
  }

  @Override
  public boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
    if (collectionState == null) { // collection has been deleted - don't wait
      latch.countDown();
      return true;
    }
    Slice slice = collectionState.getSlice(shardId);
    if (slice == null) { // shard has been removed - don't wait
      latch.countDown();
      return true;
    }
    for (Replica replica : slice.getReplicas()) {
      // check if another replica exists - doesn't have to be the one we're moving
      // as long as it's active and can become a leader, in which case we don't have to wait
      // for recovery of specifically the one that we've just added
      if (!replica.getName().equals(replicaId)) {
        if (replica.getType().equals(Replica.Type.PULL)) { // not eligible for leader election
          continue;
        }
        // check its state
        String coreName = replica.getStr(ZkStateReader.CORE_NAME_PROP);
        if (targetCore != null && !targetCore.equals(coreName)) {
          continue;
        }
        if (replica.isActive(liveNodes)) { // recovered - stop waiting
          latch.countDown();
          return true;
        }
      }
    }
    // set the watch again to wait for the new replica to recover
    return false;
  }
}
