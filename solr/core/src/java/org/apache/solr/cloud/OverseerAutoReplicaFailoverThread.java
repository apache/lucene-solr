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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ClusterStateUtil;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.update.UpdateShardHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


// TODO: how to tmp exclude nodes?

// TODO: more fine grained failover rules?

// TODO: test with lots of collections

// TODO: add config for only failover if replicas is < N

// TODO: general support for non shared filesystems
// this is specialized for a shared file system, but it should
// not be much work to generalize

// NOTE: using replication can slow down failover if a whole
// shard is lost.

/**
 *
 * In this simple initial implementation we are limited in how quickly we detect
 * a failure by a worst case of roughly zk session timeout + WAIT_AFTER_EXPIRATION_SECONDS + WORK_LOOP_DELAY_MS
 * and best case of roughly zk session timeout + WAIT_AFTER_EXPIRATION_SECONDS. Also, consider the time to
 * create the SolrCore, do any recovery necessary, and warm up the readers.
 * 
 * NOTE: this will only work with collections created via the collections api because they will have defined
 * replicationFactor and maxShardsPerNode.
 * 
 * @lucene.experimental
 */
public class OverseerAutoReplicaFailoverThread implements Runnable, Closeable {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Integer lastClusterStateVersion;
  
  private final ExecutorService updateExecutor;
  private volatile boolean isClosed;
  private ZkStateReader zkStateReader;
  private final Cache<String,Long> baseUrlForBadNodes;
  private Set<String> liveNodes = Collections.EMPTY_SET;

  private final int workLoopDelay;
  private final int waitAfterExpiration;

  private volatile Thread thread;
  
  public OverseerAutoReplicaFailoverThread(CloudConfig config, ZkStateReader zkStateReader,
      UpdateShardHandler updateShardHandler) {
    this.zkStateReader = zkStateReader;
    
    this.workLoopDelay = config.getAutoReplicaFailoverWorkLoopDelay();
    this.waitAfterExpiration = config.getAutoReplicaFailoverWaitAfterExpiration();
    int badNodeExpiration = config.getAutoReplicaFailoverBadNodeExpiration();
    
    log.debug(
        "Starting "
            + this.getClass().getSimpleName()
            + " autoReplicaFailoverWorkLoopDelay={} autoReplicaFailoverWaitAfterExpiration={} autoReplicaFailoverBadNodeExpiration={}",
        workLoopDelay, waitAfterExpiration, badNodeExpiration);

    baseUrlForBadNodes = CacheBuilder.newBuilder()
        .concurrencyLevel(1).expireAfterWrite(badNodeExpiration, TimeUnit.MILLISECONDS).build();
    
    // TODO: Speed up our work loop when live_nodes changes??

    updateExecutor = updateShardHandler.getUpdateExecutor();

    
    // TODO: perhaps do a health ping periodically to each node (scaryish)
    // And/OR work on JIRA issue around self health checks (SOLR-5805)
  }
  
  @Override
  public void run() {
    this.thread = Thread.currentThread();
    while (!this.isClosed) {
      // work loop
      log.debug("do " + this.getClass().getSimpleName() + " work loop");

      // every n, look at state and make add / remove calls

      try {
        doWork();
      } catch (Exception e) {
        SolrException.log(log, this.getClass().getSimpleName()
            + " had an error in its thread work loop.", e);
      }
      
      if (!this.isClosed) {
        try {
          Thread.sleep(workLoopDelay);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
  
  private void doWork() {
    
    // TODO: extract to configurable strategy class ??
    ClusterState clusterState = zkStateReader.getClusterState();
    //check if we have disabled autoAddReplicas cluster wide
    String autoAddReplicas = zkStateReader.getClusterProperty(ZkStateReader.AUTO_ADD_REPLICAS, (String) null);
    if (autoAddReplicas != null && autoAddReplicas.equals("false")) {
      return;
    }
    if (clusterState != null) {
      if (clusterState.getZkClusterStateVersion() != null &&
          clusterState.getZkClusterStateVersion().equals(lastClusterStateVersion) && baseUrlForBadNodes.size() == 0 &&
          liveNodes.equals(clusterState.getLiveNodes())) {
        // nothing has changed, no work to do
        return;
      }

      liveNodes = clusterState.getLiveNodes();
      lastClusterStateVersion = clusterState.getZkClusterStateVersion();
      Map<String, DocCollection> collections = clusterState.getCollectionsMap();
      for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
        log.debug("look at collection={}", entry.getKey());
        DocCollection docCollection = entry.getValue();
        if (!docCollection.getAutoAddReplicas()) {
          log.debug("Collection {} is not setup to use autoAddReplicas, skipping..", docCollection.getName());
          continue;
        }
        if (docCollection.getReplicationFactor() == null) {
          log.debug("Skipping collection because it has no defined replicationFactor, name={}", docCollection.getName());
          continue;
        }
        log.debug("Found collection, name={} replicationFactor={}", entry.getKey(), docCollection.getReplicationFactor());
        
        Collection<Slice> slices = docCollection.getSlices();
        for (Slice slice : slices) {
          if (slice.getState() == Slice.State.ACTIVE) {
            
            final Collection<DownReplica> downReplicas = new ArrayList<DownReplica>();
            
            int goodReplicas = findDownReplicasInSlice(clusterState, docCollection, slice, downReplicas);
            
            log.debug("collection={} replicationFactor={} goodReplicaCount={}", docCollection.getName(), docCollection.getReplicationFactor(), goodReplicas);
            
            if (downReplicas.size() > 0 && goodReplicas < docCollection.getReplicationFactor()) {
              // badReplicaMap.put(collection, badReplicas);
              processBadReplicas(entry.getKey(), downReplicas);
            } else if (goodReplicas > docCollection.getReplicationFactor()) {
              log.debug("There are too many replicas");
            }
          }
        }
      }
     
    }
  }

  private void processBadReplicas(final String collection, final Collection<DownReplica> badReplicas) {
    for (DownReplica badReplica : badReplicas) {
      log.debug("process down replica={} from collection={}", badReplica.replica.getName(), collection);
      String baseUrl = badReplica.replica.getStr(ZkStateReader.BASE_URL_PROP);
      Long wentBadAtNS = baseUrlForBadNodes.getIfPresent(baseUrl);
      if (wentBadAtNS == null) {
        log.warn("Replica {} may need to failover.",
            badReplica.replica.getName());
        baseUrlForBadNodes.put(baseUrl, System.nanoTime());
        
      } else {
        
        long elasped = System.nanoTime() - wentBadAtNS;
        if (elasped < TimeUnit.NANOSECONDS.convert(waitAfterExpiration, TimeUnit.MILLISECONDS)) {
          // protect against ZK 'flapping', startup and shutdown
          log.debug("Looks troublesome...continue. Elapsed={}", elasped + "ns");
        } else {
          log.debug("We need to add a replica. Elapsed={}", elasped + "ns");
          
          if (addReplica(collection, badReplica)) {
            baseUrlForBadNodes.invalidate(baseUrl);
          }
        }
      }
    }
  }

  private boolean addReplica(final String collection, DownReplica badReplica) {
    // first find best home - first strategy, sort by number of cores
    // hosted where maxCoresPerNode is not violated
    final Integer maxCoreCount = zkStateReader.getClusterProperty(ZkStateReader.MAX_CORES_PER_NODE, (Integer) null);
    final String createUrl = getBestCreateUrl(zkStateReader, badReplica, maxCoreCount);
    if (createUrl == null) {
      log.warn("Could not find a node to create new replica on.");
      return false;
    }
    
    // NOTE: we send the absolute path, which will slightly change
    // behavior of these cores as they won't respond to changes
    // in the solr.hdfs.home sys prop as they would have.
    final String dataDir = badReplica.replica.getStr("dataDir");
    final String ulogDir = badReplica.replica.getStr("ulogDir");
    final String coreNodeName = badReplica.replica.getName();
    if (dataDir != null) {
      // need an async request - full shard goes down leader election
      final String coreName = badReplica.replica.getStr(ZkStateReader.CORE_NAME_PROP);
      log.debug("submit call to {}", createUrl);
      MDC.put("OverseerAutoReplicaFailoverThread.createUrl", createUrl);
      try {
        updateExecutor.submit(() -> createSolrCore(collection, createUrl, dataDir, ulogDir, coreNodeName, coreName));
      } finally {
        MDC.remove("OverseerAutoReplicaFailoverThread.createUrl");
      }

      // wait to see state for core we just created
      boolean success = ClusterStateUtil.waitToSeeLiveReplica(zkStateReader, collection, coreNodeName, createUrl, 30000);
      if (!success) {
        log.error("Creating new replica appears to have failed, timed out waiting to see created SolrCore register in the clusterstate.");
        return false;
      }
      return true;
    }
    
    log.warn("Could not find dataDir or ulogDir in cluster state.");
    
    return false;
  }

  private static int findDownReplicasInSlice(ClusterState clusterState, DocCollection collection, Slice slice, final Collection<DownReplica> badReplicas) {
    int goodReplicas = 0;
    Collection<Replica> replicas = slice.getReplicas();
    if (replicas != null) {
      for (Replica replica : replicas) {
        // on a live node?
        boolean live = clusterState.liveNodesContain(replica.getNodeName());
        final Replica.State state = replica.getState();
        
        final boolean okayState = state == Replica.State.DOWN
            || state == Replica.State.RECOVERING
            || state == Replica.State.ACTIVE;
        
        log.debug("Process replica name={} live={} state={}", replica.getName(), live, state.toString());
        
        if (live && okayState) {
          goodReplicas++;
        } else {
          DownReplica badReplica = new DownReplica();
          badReplica.replica = replica;
          badReplica.slice = slice;
          badReplica.collection = collection;
          badReplicas.add(badReplica);
        }
      }
    }
    log.debug("bad replicas for slice {}", badReplicas);
    return goodReplicas;
  }
  
  /**
   * 
   * @return the best node to replace the badReplica on or null if there is no
   *         such node
   */
  static String getBestCreateUrl(ZkStateReader zkStateReader, DownReplica badReplica, Integer maxCoreCount) {
    assert badReplica != null;
    assert badReplica.collection != null;
    assert badReplica.slice != null;
    log.debug("getBestCreateUrl for " + badReplica.replica);
    Map<String,Counts> counts = new HashMap<>();
    Set<String> unsuitableHosts = new HashSet<>();
    
    Set<String> liveNodes = new HashSet<>(zkStateReader.getClusterState().getLiveNodes());
    Map<String, Integer> coresPerNode = new HashMap<>();
    
    ClusterState clusterState = zkStateReader.getClusterState();
    if (clusterState != null) {
      Map<String, DocCollection> collections = clusterState.getCollectionsMap();
      for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
        String collection = entry.getKey();
        log.debug("look at collection {} as possible create candidate", collection);
        DocCollection docCollection = entry.getValue();
        // TODO - only operate on collections with sharedfs failover = true ??
        Collection<Slice> slices = docCollection.getSlices();
        for (Slice slice : slices) {
          // only look at active shards
          if (slice.getState() == Slice.State.ACTIVE) {
            log.debug("look at slice {} for collection {} as possible create candidate", slice.getName(), collection); 
            Collection<Replica> replicas = slice.getReplicas();

            for (Replica replica : replicas) {
              liveNodes.remove(replica.getNodeName());
              String baseUrl = replica.getStr(ZkStateReader.BASE_URL_PROP);
              if (coresPerNode.containsKey(baseUrl)) {
                Integer nodeCount = coresPerNode.get(baseUrl);
                coresPerNode.put(baseUrl, nodeCount++);
              } else {
                coresPerNode.put(baseUrl, 1);
              }
              if (baseUrl.equals(badReplica.replica.getStr(ZkStateReader.BASE_URL_PROP))) {
                continue;
              }
              // on a live node?
              log.debug("collection={} nodename={} livenodes={}", collection, replica.getNodeName(), clusterState.getLiveNodes());
              boolean live = clusterState.liveNodesContain(replica.getNodeName());
              log.debug("collection={} look at replica {} as possible create candidate, live={}", collection, replica.getName(), live); 
              if (live) {
                Counts cnt = counts.get(baseUrl);
                if (cnt == null) {
                  cnt = new Counts();
                }
                if (badReplica.collection.getName().equals(collection)) {
                  cnt.negRankingWeight += 3;
                  cnt.collectionShardsOnNode += 1;
                } else {
                  cnt.negRankingWeight += 1;
                }
                if (badReplica.collection.getName().equals(collection) && badReplica.slice.getName().equals(slice.getName())) {
                  cnt.ourReplicas++;
                }

                Integer maxShardsPerNode = badReplica.collection.getMaxShardsPerNode();
                if (maxShardsPerNode == null) {
                  log.warn("maxShardsPerNode is not defined for collection, name=" + badReplica.collection.getName());
                  maxShardsPerNode = Integer.MAX_VALUE;
                }
                log.debug("collection={} node={} maxShardsPerNode={} maxCoresPerNode={} potential hosts={}",
                    collection, baseUrl, maxShardsPerNode, maxCoreCount, cnt);

                Collection<Replica> badSliceReplicas = null;
                DocCollection c = clusterState.getCollection(badReplica.collection.getName());
                if (c != null) {
                  Slice s = c.getSlice(badReplica.slice.getName());
                  if (s != null) {
                    badSliceReplicas = s.getReplicas();
                  }
                }
                boolean alreadyExistsOnNode = replicaAlreadyExistsOnNode(zkStateReader.getClusterState(), badSliceReplicas, badReplica, baseUrl);
                if (unsuitableHosts.contains(baseUrl) || alreadyExistsOnNode || cnt.collectionShardsOnNode >= maxShardsPerNode
                    || (maxCoreCount != null && coresPerNode.get(baseUrl) >= maxCoreCount) ) {
                  counts.remove(baseUrl);
                  unsuitableHosts.add(baseUrl);
                  log.debug("not a candidate node, collection={} node={} max shards per node={} good replicas={}", collection, baseUrl, maxShardsPerNode, cnt);
                } else {
                  counts.put(baseUrl, cnt);
                  log.debug("is a candidate node, collection={} node={} max shards per node={} good replicas={}", collection, baseUrl, maxShardsPerNode, cnt);
                }
              }
            }
          }
        }
      }
    }
    
    for (String node : liveNodes) {
      counts.put(zkStateReader.getBaseUrlForNodeName(node), new Counts(0, 0));
    }
    
    if (counts.size() == 0) {
      log.debug("no suitable hosts found for getBestCreateUrl for collection={}", badReplica.collection.getName());
      return null;
    }
    
    ValueComparator vc = new ValueComparator(counts);
    Map<String,Counts> sortedCounts = new TreeMap<String, Counts>(vc);
    sortedCounts.putAll(counts);
    
    log.debug("empty nodes={} for collection={}", liveNodes, badReplica.collection.getName());
    log.debug("sorted hosts={} for collection={}", sortedCounts, badReplica.collection.getName());
    log.debug("unsuitable hosts={} for collection={}", unsuitableHosts, badReplica.collection.getName());
    
    return sortedCounts.keySet().iterator().next();
  }
  
  private static boolean replicaAlreadyExistsOnNode(ClusterState clusterState, Collection<Replica> replicas, DownReplica badReplica, String baseUrl) {
    if (replicas != null) {
      log.debug("collection={} check if replica already exists on node using replicas {}", badReplica.collection.getName(), getNames(replicas));
      for (Replica replica : replicas) {
        final Replica.State state = replica.getState();
        if (!replica.getName().equals(badReplica.replica.getName()) && replica.getStr(ZkStateReader.BASE_URL_PROP).equals(baseUrl)
            && clusterState.liveNodesContain(replica.getNodeName())
            && (state == Replica.State.ACTIVE || state == Replica.State.DOWN || state == Replica.State.RECOVERING)) {
          log.debug("collection={} replica already exists on node, bad replica={}, existing replica={}, node name={}",  badReplica.collection.getName(), badReplica.replica.getName(), replica.getName(), replica.getNodeName());
          return true;
        }
      }
    }
    log.debug("collection={} replica does not yet exist on node: {}",  badReplica.collection.getName(), baseUrl);
    return false;
  }
  
  private static Object getNames(Collection<Replica> replicas) {
    Set<String> names = new HashSet<>(replicas.size());
    for (Replica replica : replicas) {
      names.add(replica.getName());
    }
    return names;
  }

  private boolean createSolrCore(final String collection,
      final String createUrl, final String dataDir, final String ulogDir,
      final String coreNodeName, final String coreName) {

    try (HttpSolrClient client = new HttpSolrClient.Builder(createUrl).build()) {
      log.debug("create url={}", createUrl);
      client.setConnectionTimeout(30000);
      client.setSoTimeout(60000);
      Create createCmd = new Create();
      createCmd.setCollection(collection);
      createCmd.setCoreNodeName(coreNodeName);
      // TODO: how do we ensure unique coreName
      // for now, the collections API will use unique names
      createCmd.setCoreName(coreName);
      createCmd.setDataDir(dataDir);
      createCmd.setUlogDir(ulogDir.substring(0, ulogDir.length() - "/tlog".length()));
      client.request(createCmd);
    } catch (Exception e) {
      SolrException.log(log, "Exception trying to create new replica on " + createUrl, e);
      return false;
    }
    return true;
  }
  
  private static class ValueComparator implements Comparator<String> {
    Map<String,Counts> map;
    
    public ValueComparator(Map<String,Counts> map) {
      this.map = map;
    }
    
    public int compare(String a, String b) {
      if (map.get(a).negRankingWeight >= map.get(b).negRankingWeight) {
        return 1;
      } else {
        return -1;
      }
    }
  }
  
  @Override
  public void close() {
    isClosed = true;
    Thread lThread = thread;
    if (lThread != null) {
      lThread.interrupt();
    }
  }
  
  public boolean isClosed() {
    return isClosed;
  }
  
  
  private static class Counts {
    int collectionShardsOnNode = 0;
    int negRankingWeight = 0;
    int ourReplicas = 0;
    
    private Counts() {
      
    }
    
    private Counts(int totalReplicas, int ourReplicas) {
      this.negRankingWeight = totalReplicas;
      this.ourReplicas = ourReplicas;
    }
    
    @Override
    public String toString() {
      return "Counts [negRankingWeight=" + negRankingWeight + ", sameSliceCount="
          + ourReplicas + ", collectionShardsOnNode=" + collectionShardsOnNode + "]";
    }
  }
  
  static class DownReplica {
    Replica replica;
    Slice slice;
    DocCollection collection;
    
    @Override
    public String toString() {
      return "DownReplica [replica=" + replica.getName() + ", slice="
          + slice.getName() + ", collection=" + collection.getName() + "]";
    }
  }
  
}
