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
package org.apache.solr.common.cloud;

import java.lang.invoke.MethodHandles;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStateUtil {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private static final int TIMEOUT_POLL_MS = 1000;
  
  /**
   * Wait to see *all* cores live and active.
   * 
   * @param zkStateReader
   *          to use for ClusterState
   * @param timeoutInMs
   *          how long to wait before giving up
   * @return false if timed out
   */
  public static boolean waitForAllActiveAndLiveReplicas(ZkStateReader zkStateReader, int timeoutInMs) {
    return waitForAllActiveAndLiveReplicas(zkStateReader, null, timeoutInMs);
  }
  
  /**
   * Wait to see *all* cores live and active.
   * 
   * @param zkStateReader
   *          to use for ClusterState
   * @param collection to look at
   * @param timeoutInMs
   *          how long to wait before giving up
   * @return false if timed out
   */
  public static boolean waitForAllActiveAndLiveReplicas(ZkStateReader zkStateReader, String collection,
      int timeoutInMs) {
    long timeout = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(timeoutInMs, TimeUnit.MILLISECONDS);
    boolean success = false;
    while (!success && System.nanoTime() < timeout) {
      success = true;
      ClusterState clusterState = zkStateReader.getClusterState();
      if (clusterState != null) {
        Map<String, DocCollection> collections = null;
        if (collection != null) {
          collections = Collections.singletonMap(collection, clusterState.getCollection(collection));
        } else {
          collections = clusterState.getCollectionsMap();
        }
        for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
          DocCollection docCollection = entry.getValue();
          Collection<Slice> slices = docCollection.getSlices();
          for (Slice slice : slices) {
            // only look at active shards
            if (slice.getState() == Slice.State.ACTIVE) {
              Collection<Replica> replicas = slice.getReplicas();
              for (Replica replica : replicas) {
                // on a live node?
                final boolean live = clusterState.liveNodesContain(replica.getNodeName());
                final boolean isActive = replica.getState() == Replica.State.ACTIVE;
                if (!live || !isActive) {
                  // fail
                  success = false;
                }
              }
            }
          }
        }
        if (!success) {
          try {
            Thread.sleep(TIMEOUT_POLL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
          }
        }
      }
    }
    
    return success;
  }
  
  /**
   * Wait to see an entry in the ClusterState with a specific coreNodeName and
   * baseUrl.
   * 
   * @param zkStateReader
   *          to use for ClusterState
   * @param collection
   *          to look in
   * @param coreNodeName
   *          to wait for
   * @param baseUrl
   *          to wait for
   * @param timeoutInMs
   *          how long to wait before giving up
   * @return false if timed out
   */
  public static boolean waitToSeeLiveReplica(ZkStateReader zkStateReader,
      String collection, String coreNodeName, String baseUrl,
      int timeoutInMs) {
    long timeout = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(timeoutInMs, TimeUnit.MILLISECONDS);
    
    while (System.nanoTime() < timeout) {
      log.debug("waiting to see replica just created live collection={} replica={} baseUrl={}",
          collection, coreNodeName, baseUrl);
      ClusterState clusterState = zkStateReader.getClusterState();
      if (clusterState != null) {
        DocCollection docCollection = clusterState.getCollection(collection);
        Collection<Slice> slices = docCollection.getSlices();
        for (Slice slice : slices) {
          // only look at active shards
          if (slice.getState() == Slice.State.ACTIVE) {
            Collection<Replica> replicas = slice.getReplicas();
            for (Replica replica : replicas) {
              // on a live node?
              boolean live = clusterState.liveNodesContain(replica.getNodeName());
              String rcoreNodeName = replica.getName();
              String rbaseUrl = replica.getBaseUrl();
              if (live && coreNodeName.equals(rcoreNodeName)
                  && baseUrl.equals(rbaseUrl)) {
                // found it
                return true;
              }
            }
          }
        }
        try {
          Thread.sleep(TIMEOUT_POLL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
        }
      }
    }
    
    log.error("Timed out waiting to see replica just created in cluster state. Continuing...");
    return false;
  }
  
  public static boolean waitForAllReplicasNotLive(ZkStateReader zkStateReader, int timeoutInMs) {
    return waitForAllReplicasNotLive(zkStateReader, null, timeoutInMs);
  }
  

  public static boolean waitForAllReplicasNotLive(ZkStateReader zkStateReader,
      String collection, int timeoutInMs) {
    long timeout = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(timeoutInMs, TimeUnit.MILLISECONDS);
    boolean success = false;
    while (!success && System.nanoTime() < timeout) {
      success = true;
      ClusterState clusterState = zkStateReader.getClusterState();
      if (clusterState != null) {
        Map<String, DocCollection> collections = null;
        if (collection != null) {
          collections = Collections.singletonMap(collection, clusterState.getCollection(collection));
        } else {
          collections = clusterState.getCollectionsMap();
        }
        for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
          DocCollection docCollection = entry.getValue();
          Collection<Slice> slices = docCollection.getSlices();
          for (Slice slice : slices) {
            // only look at active shards
            if (slice.getState() == Slice.State.ACTIVE) {
              Collection<Replica> replicas = slice.getReplicas();
              for (Replica replica : replicas) {
                // on a live node?
                boolean live = clusterState.liveNodesContain(replica
                    .getNodeName());
                if (live) {
                  // fail
                  success = false;
                }
              }
            }
          }
        }
        if (!success) {
          try {
            Thread.sleep(TIMEOUT_POLL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
          }
        }
      }
    }
    
    return success;
  }
  
  public static int getLiveAndActiveReplicaCount(ZkStateReader zkStateReader, String collection) {
    Slice[] slices;
    slices = zkStateReader.getClusterState().getCollection(collection).getActiveSlicesArr();
    int liveAndActive = 0;
    for (Slice slice : slices) {
      for (Replica replica : slice.getReplicas()) {
        boolean live = zkStateReader.getClusterState().liveNodesContain(replica.getNodeName());
        boolean active = replica.getState() == Replica.State.ACTIVE;
        if (live && active) {
          liveAndActive++;
        }
      }
    }
    return liveAndActive;
  }
  
  public static boolean waitForLiveAndActiveReplicaCount(ZkStateReader zkStateReader,
      String collection, int replicaCount, int timeoutInMs) {
    long timeout = System.nanoTime()
        + TimeUnit.NANOSECONDS.convert(timeoutInMs, TimeUnit.MILLISECONDS);
    boolean success = false;
    while (!success && System.nanoTime() < timeout) {
      success = getLiveAndActiveReplicaCount(zkStateReader, collection) == replicaCount;
      
      if (!success) {
        try {
          Thread.sleep(TIMEOUT_POLL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SolrException(ErrorCode.SERVER_ERROR, "Interrupted");
        }
      }
      
    }
    
    return success;
  }

}
