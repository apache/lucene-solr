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
import java.util.List;
import java.util.Set;

import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watch for replicas to become {@link org.apache.solr.common.cloud.Replica.State#ACTIVE}. Watcher is
 * terminated (its {@link #onStateChanged(Set, DocCollection)} method returns false) when all listed
 * replicas become active.
 * <p>Additionally, the provided {@link SolrCloseableLatch} instance can be used to await
 * for all listed replicas to become active.</p>
 */
public class ActiveReplicaWatcher implements CollectionStateWatcher {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final String collection;
  private final List<String> replicaIds = new ArrayList<>();
  private final List<String> solrCoreNames = new ArrayList<>();
  private final List<Replica> activeReplicas = new ArrayList<>();

  private int lastZkVersion = -1;

  private SolrCloseableLatch latch;

  /**
   * Construct the watcher. At least one replicaId or solrCoreName must be provided.
   * @param collection collection name
   * @param replicaIds list of replica id-s
   * @param solrCoreNames list of SolrCore names
   * @param latch optional latch to await for all provided replicas to become active. This latch will be
   *                       counted down by at most the number of provided replica id-s / SolrCore names.
   */
  public ActiveReplicaWatcher(String collection, List<String> replicaIds, List<String> solrCoreNames, SolrCloseableLatch latch) {
    if (replicaIds == null && solrCoreNames == null) {
      throw new IllegalArgumentException("Either replicaId or solrCoreName must be provided.");
    }
    if (replicaIds != null) {
      this.replicaIds.addAll(replicaIds);
    }
    if (solrCoreNames != null) {
      this.solrCoreNames.addAll(solrCoreNames);
    }
    if (this.replicaIds.isEmpty() && this.solrCoreNames.isEmpty()) {
      throw new IllegalArgumentException("At least one replicaId or solrCoreName must be provided");
    }
    this.collection = collection;
    this.latch = latch;
  }

  /**
   * Collection name.
   */
  public String getCollection() {
    return collection;
  }

  /**
   * Return the list of active replicas found so far.
   */
  public List<Replica> getActiveReplicas() {
    return activeReplicas;
  }

  /**
   * Return the list of replica id-s that are not active yet (or unverified).
   */
  public List<String> getReplicaIds() {
    return replicaIds;
  }

  /**
   * Return a list of SolrCore names that are not active yet (or unverified).
   */
  public List<String> getSolrCoreNames() {
    return solrCoreNames;
  }

  @Override
  public String toString() {
    return "ActiveReplicaWatcher@" + Long.toHexString(hashCode()) + "{" +
        "collection='" + collection + '\'' +
        ", replicaIds=" + replicaIds +
        ", solrCoreNames=" + solrCoreNames +
        ", latch=" + (latch != null ? latch.getCount() : "null") + "," +
        ", activeReplicas=" + activeReplicas +
        '}';
  }

  // synchronized due to SOLR-11535
  @Override
  public synchronized boolean onStateChanged(Set<String> liveNodes, DocCollection collectionState) {
    if (log.isDebugEnabled()) {
      log.debug("-- onStateChanged@{}: replicaIds={}, solrCoreNames={} {}\ncollectionState {}"
          , Long.toHexString(hashCode()), replicaIds, solrCoreNames
          , (latch != null ? "\nlatch count=" + latch.getCount() : "")
          , collectionState); // nowarn
    }
    if (collectionState == null) { // collection has been deleted - don't wait
      if (log.isDebugEnabled()) {
        log.debug("-- collection deleted, decrementing latch by {} ", replicaIds.size() + solrCoreNames.size()); // nowarn
      }
      if (latch != null) {
        for (int i = 0; i < replicaIds.size() + solrCoreNames.size(); i++) {
          latch.countDown();
        }
      }
      replicaIds.clear();
      solrCoreNames.clear();
      return true;
    }
    if (replicaIds.isEmpty() && solrCoreNames.isEmpty()) {
      log.debug("-- already done, exiting...");
      return true;
    }
    if (collectionState.getZNodeVersion() == lastZkVersion) {
      log.debug("-- spurious call with already seen zkVersion= {}, ignoring...", lastZkVersion);
      return false;
    }
    lastZkVersion = collectionState.getZNodeVersion();

    for (Slice slice : collectionState.getSlices()) {
      for (Replica replica : slice.getReplicas()) {
        if (replicaIds.contains(replica.getName())) {
          if (replica.isActive(liveNodes)) {
            activeReplicas.add(replica);
            replicaIds.remove(replica.getName());
            if (latch != null) {
              latch.countDown();
            }
          }
        } else if (solrCoreNames.contains(replica.getStr(ZkStateReader.CORE_NAME_PROP))) {
          if (replica.isActive(liveNodes)) {
            activeReplicas.add(replica);
            solrCoreNames.remove(replica.getStr(ZkStateReader.CORE_NAME_PROP));
            if (latch != null) {
              latch.countDown();
            }
          }
        }
      }
    }
    if (log.isDebugEnabled()) {
      log.debug("-- {} now latchcount={}", Long.toHexString(hashCode()), latch.getCount());
    }
    if (replicaIds.isEmpty() && solrCoreNames.isEmpty()) {
      return true;
    } else {
      return false;
    }
  }
}
