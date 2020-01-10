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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.solr.client.solrj.cloud.ShardTerms;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for interact with a ZK term node.
 * Each ZK term node relates to a shard of a collection and have this format (in json)
 * <p>
 * <code>
 * {
 *   "replicaNodeName1" : 1,
 *   "replicaNodeName2" : 2,
 *   ..
 * }
 * </code>
 * <p>
 * The values correspond to replicas are called terms.
 * Only replicas with highest term value are considered up to date and be able to become leader and serve queries.
 * <p>
 * Terms can only updated in two strict ways:
 * <ul>
 * <li>A replica sets its term equals to leader's term
 * <li>The leader increase its term and some other replicas by 1
 * </ul>
 * This class should not be reused after {@link org.apache.zookeeper.Watcher.Event.KeeperState#Expired} event
 */
public class ZkShardTerms implements AutoCloseable{

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final Object writingLock = new Object();
  private final String collection;
  private final String shard;
  private final String znodePath;
  private final SolrZkClient zkClient;
  private final Set<CoreTermWatcher> listeners = new HashSet<>();
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private ShardTerms terms;

  // Listener of a core for shard's term change events
  interface CoreTermWatcher {
    // return true if the listener wanna to be triggered in the next time
    boolean onTermChanged(ShardTerms terms);
  }

  public ZkShardTerms(String collection, String shard, SolrZkClient zkClient) {
    this.znodePath = ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection + "/terms/" + shard;
    this.collection = collection;
    this.shard = shard;
    this.zkClient = zkClient;
    ensureTermNodeExist();
    refreshTerms();
    retryRegisterWatcher();
    ObjectReleaseTracker.track(this);
  }

  /**
   * Ensure that leader's term is higher than some replica's terms
   * @param leader coreNodeName of leader
   * @param replicasNeedingRecovery set of replicas in which their terms should be lower than leader's term
   */
  public void ensureTermsIsHigher(String leader, Set<String> replicasNeedingRecovery) {
    if (replicasNeedingRecovery.isEmpty()) return;

    ShardTerms newTerms;
    while( (newTerms = terms.increaseTerms(leader, replicasNeedingRecovery)) != null) {
      if (forceSaveTerms(newTerms)) return;
    }
  }

  public ShardTerms getShardTerms() {
    return terms;
  }
  /**
   * Can this replica become leader?
   * @param coreNodeName of the replica
   * @return true if this replica can become leader, false if otherwise
   */
  public boolean canBecomeLeader(String coreNodeName) {
    return terms.canBecomeLeader(coreNodeName);
  }

  /**
   * Should leader skip sending updates to this replica?
   * @param coreNodeName of the replica
   * @return true if this replica has term equals to leader's term, false if otherwise
   */
  public boolean skipSendingUpdatesTo(String coreNodeName) {
    return !terms.haveHighestTermValue(coreNodeName);
  }

  /**
   * Did this replica registered its term? This is a sign to check f
   * @param coreNodeName of the replica
   * @return true if this replica registered its term, false if otherwise
   */
  public boolean registered(String coreNodeName) {
    return terms.getTerm(coreNodeName) != null;
  }

  public void close() {
    // no watcher will be registered
    isClosed.set(true);
    synchronized (listeners) {
      listeners.clear();
    }
    ObjectReleaseTracker.release(this);
  }

  // package private for testing, only used by tests
  Map<String, Long> getTerms() {
    synchronized (writingLock) {
      return terms.getTerms();
    }
  }

  /**
   * Add a listener so the next time the shard's term get updated, listeners will be called
   */
  void addListener(CoreTermWatcher listener) {
    synchronized (listeners) {
      listeners.add(listener);
    }
  }

  /**
   * Remove the coreNodeName from terms map and also remove any expired listeners
   * @return Return true if this object should not be reused
   */
  boolean removeTerm(CoreDescriptor cd) {
    int numListeners;
    synchronized (listeners) {
      // solrcore already closed
      listeners.removeIf(coreTermWatcher -> !coreTermWatcher.onTermChanged(terms));
      numListeners = listeners.size();
    }
    return removeTerm(cd.getCloudDescriptor().getCoreNodeName()) || numListeners == 0;
  }

  // package private for testing, only used by tests
  // return true if this object should not be reused
  boolean removeTerm(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.removeTerm(coreNodeName)) != null) {
      try {
        if (saveTerms(newTerms)) return false;
      } catch (KeeperException.NoNodeException e) {
        return true;
      }
    }
    return true;
  }

  /**
   * Register a replica's term (term value will be 0).
   * If a term is already associate with this replica do nothing
   * @param coreNodeName of the replica
   */
  void registerTerm(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.registerTerm(coreNodeName)) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  /**
   * Set a replica's term equals to leader's term, and remove recovering flag of a replica.
   * This call should only be used by {@link org.apache.solr.common.params.CollectionParams.CollectionAction#FORCELEADER}
   * @param coreNodeName of the replica
   */
  public void setTermEqualsToLeader(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.setTermEqualsToLeader(coreNodeName)) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  public void setTermToZero(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.setTermToZero(coreNodeName)) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as recovering
   */
  public void startRecovering(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.startRecovering(coreNodeName)) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  /**
   * Mark {@code coreNodeName} as finished recovering
   */
  public void doneRecovering(String coreNodeName) {
    ShardTerms newTerms;
    while ( (newTerms = terms.doneRecovering(coreNodeName)) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  public boolean isRecovering(String name) {
    return terms.isRecovering(name);
  }

  /**
   * When first updates come in, all replicas have some data now,
   * so we must switch from term 0 (registered) to 1 (have some data)
   */
  public void ensureHighestTermsAreNotZero() {
    ShardTerms newTerms;
    while ( (newTerms = terms.ensureHighestTermsAreNotZero()) != null) {
      if (forceSaveTerms(newTerms)) break;
    }
  }

  public long getHighestTerm() {
    return terms.getMaxTerm();
  }

  public long getTerm(String coreNodeName) {
    Long term = terms.getTerm(coreNodeName);
    return term == null? -1 : term;
  }

  // package private for testing, only used by tests
  int getNumListeners() {
    synchronized (listeners) {
      return listeners.size();
    }
  }

  /**
   * Set new terms to ZK.
   * In case of correspond ZK term node is not created, create it
   * @param newTerms to be set
   * @return true if terms is saved successfully to ZK, false if otherwise
   */
  private boolean forceSaveTerms(ShardTerms newTerms) {
    try {
      return saveTerms(newTerms);
    } catch (KeeperException.NoNodeException e) {
      ensureTermNodeExist();
      return false;
    }
  }

  /**
   * Set new terms to ZK, the version of new terms must match the current ZK term node
   * @param newTerms to be set
   * @return true if terms is saved successfully to ZK, false if otherwise
   * @throws KeeperException.NoNodeException correspond ZK term node is not created
   */
  private boolean saveTerms(ShardTerms newTerms) throws KeeperException.NoNodeException {
    byte[] znodeData = Utils.toJSON(newTerms);
    try {
      Stat stat = zkClient.setData(znodePath, znodeData, newTerms.getVersion(), true);
      setNewTerms(new ShardTerms(newTerms, stat.getVersion()));
      log.info("Successful update of terms at {} to {}", znodePath, newTerms);
      return true;
    } catch (KeeperException.BadVersionException e) {
      log.info("Failed to save terms, version is not a match, retrying");
      refreshTerms();
    } catch (KeeperException.NoNodeException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error while saving shard term for collection: " + collection, e);
    }
    return false;
  }

  /**
   * Create correspond ZK term node
   */
  private void ensureTermNodeExist() {
    String path = "/collections/" + collection + "/terms";
    try {
      path += "/" + shard;

      try {
        Map<String,Long> initialTerms = new HashMap<>();
        zkClient.makePath(path, Utils.toJSON(initialTerms), CreateMode.PERSISTENT, true);
      } catch (KeeperException.NodeExistsException e) {
        // it's okay if another beats us creating the node
      }

    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error creating shard term node in Zookeeper for collection: " + collection, e);
    } catch (KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error creating shard term node in Zookeeper for collection: " + collection, e);
    }
  }

  /**
   * Fetch latest terms from ZK
   */
  public void refreshTerms() {
    ShardTerms newTerms;
    try {
      Stat stat = new Stat();
      byte[] data = zkClient.getData(znodePath, null, stat, true);
      newTerms = new ShardTerms((Map<String, Long>) Utils.fromJSON(data), stat.getVersion());
    } catch (KeeperException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error updating shard term for collection: " + collection, e);
    }

    setNewTerms(newTerms);
  }

  /**
   * Retry register a watcher to the correspond ZK term node
   */
  private void retryRegisterWatcher() {
    while (!isClosed.get()) {
      try {
        registerWatcher();
        return;
      } catch (KeeperException.SessionExpiredException | KeeperException.AuthFailedException e) {
        isClosed.set(true);
        log.error("Failed watching shard term for collection: {} due to unrecoverable exception", collection, e);
        return;
      } catch (KeeperException e) {
        log.warn("Failed watching shard term for collection: {}, retrying!", collection, e);
        try {
          zkClient.getConnectionManager().waitForConnected(zkClient.getZkClientTimeout());
        } catch (TimeoutException te) {
          if (Thread.interrupted()) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error watching shard term for collection: " + collection, te);
          }
        }
      }
    }
  }

  /**
   * Register a watcher to the correspond ZK term node
   */
  private void registerWatcher() throws KeeperException {
    Watcher watcher = event -> {
      // session events are not change events, and do not remove the watcher
      if (Watcher.Event.EventType.None == event.getType()) {
        return;
      }
      retryRegisterWatcher();
      // Some events may be missed during register a watcher, so it is safer to refresh terms after registering watcher
      refreshTerms();
    };
    try {
      // exists operation is faster than getData operation
      zkClient.exists(znodePath, watcher, true);
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error watching shard term for collection: " + collection, e);
    }
  }


  /**
   * Atomically update {@link ZkShardTerms#terms} and call listeners
   * @param newTerms to be set
   */
  private void setNewTerms(ShardTerms newTerms) {
    boolean isChanged = false;
    synchronized (writingLock) {
      if (terms == null || newTerms.getVersion() > terms.getVersion()) {
        terms = newTerms;
        isChanged = true;
      }
    }
    if (isChanged) onTermUpdates(newTerms);
  }

  private void onTermUpdates(ShardTerms newTerms) {
    synchronized (listeners) {
      listeners.removeIf(coreTermWatcher -> !coreTermWatcher.onTermChanged(newTerms));
    }
  }
}
