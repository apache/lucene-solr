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

import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.overseer.*;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.*;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;

import static java.util.Collections.singletonMap;
import static org.apache.solr.cloud.overseer.ZkStateWriter.NO_OP;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTIONS_ZKNODE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETESHARD;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICA;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETEREPLICAPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.BALANCESHARDUNIQUE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.MODIFYCOLLECTION;

/**
 * Gives access to distributed cluster state update methods and allows code to inquire whether distributed state update is enabled.
 */
public class DistributedClusterStateUpdater {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * When {@code true} each node updates Zookeeper directly for changing state.json files. When {@code false} messages
   * are instead sent to the Overseer and the update is done there.
   */
  private final boolean useDistributedStateUpdate;

  /**
   * Builds an instance with the specified behavior regarding distribution of state updates, allowing to know distributed
   * updates are not enabled (parameter {@code useDistributedStateUpdate} is {@code false}), or when they are (parameter
   * is {@code true)}, gives access to methods and classes allowing the execution of the updates.
   *
   * @param useDistributedStateUpdate when this parameter is {@code false}, only method expected to ever be called on this
   *                                  instance is {@link #isDistributedStateUpdate}, and it will return {@code false}.
   */
  public DistributedClusterStateUpdater(boolean useDistributedStateUpdate) {
    this.useDistributedStateUpdate = useDistributedStateUpdate;
    if (log.isInfoEnabled()) {
      log.info("Creating DistributedClusterStateUpdater with useDistributedStateUpdate=" + useDistributedStateUpdate
      + ". Solr will be using " + (useDistributedStateUpdate ? "distributed" : "Overseer based") + " cluster state updates."); // nowarn
    }
  }

  /**
   * Create a new instance of {@link StateChangeRecorder} for a given collection and a given intention (collection
   * creation vs. operations on an existing collection)
   */
  public StateChangeRecorder createStateChangeRecorder(String collectionName, boolean isCollectionCreation) {
    if (!useDistributedStateUpdate) {
      // Seeing this exception or any other of this kind here means there's a big bug in the code. No user input can cause this.
      throw new IllegalStateException("Not expecting to create instances of StateChangeRecorder when not using distributed state update");
    }
    return new StateChangeRecorder(collectionName, isCollectionCreation);
  }

  /**
   * Syntactic sugar to allow a single change to the cluster state to be made in a single call.
   */
  public void doSingleStateUpdate(MutatingCommand command, ZkNodeProps message,
                                  SolrCloudManager scm, ZkStateReader zkStateReader) throws KeeperException, InterruptedException {
    if (!useDistributedStateUpdate) {
      throw new IllegalStateException("Not expecting to execute doSingleStateUpdate when not using distributed state update");
    }
    String collectionName = command.getCollectionName(message);
    final StateChangeRecorder scr = new StateChangeRecorder(collectionName, command.isCollectionCreation());
    scr.record(command, message);
    scr.executeStateUpdates(scm, zkStateReader);
  }

  public void executeNodeDownStateUpdate(String nodeName, ZkStateReader zkStateReader) {
    if (!useDistributedStateUpdate) {
      throw new IllegalStateException("Not expecting to execute executeNodeDownStateUpdate when not using distributed state update");
    }
    CollectionNodeDownChangeCalculator.executeNodeDownStateUpdate(nodeName, zkStateReader);
  }

  /**
   * When this method returns {@code false} the legacy behavior of enqueueing cluster state update messages to Overseer
   * should be used and no other method of this class should be called.
   */
  public boolean isDistributedStateUpdate() {
    return useDistributedStateUpdate;
  }

  /**
   * Naming of enum instances are the mutator object name (e.g. {@code Cluster} for {@link ClusterStateMutator} or
   * {@code Collection} for {@link CollectionMutator}) followed by the method name of the mutator.
   * For example {@link #SliceAddReplica} represents {@link SliceMutator#addReplica}.
   * <p>
   * Even though the various mutator classes do not implement any common interface, luckily their constructors and methods
   * take the same set of parameters so all can be called from the enum method {@link #buildWriteCommand(SolrCloudManager, ClusterState, ZkNodeProps)}.
   * <p>
   * Given that {@link OverseerAction#DOWNNODE} is different (it returns a list of write commands and impacts more than one collection),
   * it is handled specifically in {@link CollectionNodeDownChangeCalculator#executeNodeDownStateUpdate}.
   */
  public enum MutatingCommand {
    BalanceShardsUnique(BALANCESHARDUNIQUE, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(cs, message);
        // Next line is where the actual work is done
        if (dProp.balanceProperty()) {
          return new ZkWriteCommand(getCollectionName(message), dProp.getDocCollection());
        } else {
          return NO_OP;
        }
      }
    },
    ClusterCreateCollection(CREATE, CommonParams.NAME) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new ClusterStateMutator(scm).createCollection(cs, message);
      }

      @Override
      public boolean isCollectionCreation() {
        return true;
      }
    },
    ClusterDeleteCollection(DELETE, CommonParams.NAME) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new ClusterStateMutator(scm).deleteCollection(cs, message);
      }
    },
    CollectionDeleteShard(DELETESHARD, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new CollectionMutator(scm).deleteShard(cs, message);
      }
    },
    CollectionModifyCollection(MODIFYCOLLECTION, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new CollectionMutator(scm).modifyCollection(cs, message);
      }
    },
    CollectionCreateShard(CREATESHARD, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new CollectionMutator(scm).createShard(cs, message);
      }
    },
    ReplicaAddReplicaProperty(ADDREPLICAPROP, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new ReplicaMutator(scm).addReplicaProperty(cs, message);
      }
    },
    ReplicaDeleteReplicaProperty(DELETEREPLICAPROP, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new ReplicaMutator(scm).deleteReplicaProperty(cs, message);
      }
    },
    ReplicaSetState(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new ReplicaMutator(scm).setState(cs, message);
      }
    },
    SliceAddReplica(ADDREPLICA, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).addReplica(cs, message);
      }
    },
    SliceAddRoutingRule(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).addRoutingRule(cs, message);
      }
    },
    SliceRemoveReplica(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).removeReplica(cs, message);
      }
    },
    SliceRemoveRoutingRule(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).removeRoutingRule(cs, message);
      }
    },
    SliceSetShardLeader(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).setShardLeader(cs, message);
      }
    },
    SliceUpdateShardState(null, ZkStateReader.COLLECTION_PROP) {
      @Override
      public ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message) {
        return new SliceMutator(scm).updateShardState(cs, message);
      }
    };

    private static final EnumMap<CollectionParams.CollectionAction, MutatingCommand> actionsToCommands;

    static {
      actionsToCommands = new EnumMap<>(CollectionParams.CollectionAction.class);
      for (MutatingCommand mc : MutatingCommand.values()) {
        if (mc.collectionAction != null) {
          actionsToCommands.put(mc.collectionAction, mc);
        }
      }
    }

    private final CollectionParams.CollectionAction collectionAction;
    private final String collectionNameParamName;

    MutatingCommand(CollectionParams.CollectionAction collectionAction, String collectionNameParamName) {
      this.collectionAction = collectionAction;
      this.collectionNameParamName = collectionNameParamName;
    }

    /**
     * mutating commands that return a single ZkWriteCommand override this method
     */
    public abstract ZkWriteCommand buildWriteCommand(SolrCloudManager scm, ClusterState cs, ZkNodeProps message);

    public String getCollectionName(ZkNodeProps message) {
      return message.getStr(collectionNameParamName);
    }

    /**
     * @return the {@link MutatingCommand} corresponding to the passed {@link org.apache.solr.common.params.CollectionParams.CollectionAction} or
     * {@code null} if no cluster state update command is defined for that action (given that {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
     * are used for the Collection API and only some are used for the cluster state updates, this is expected).
     */
    public static MutatingCommand getCommandFor(CollectionParams.CollectionAction collectionAction) {
      return actionsToCommands.get(collectionAction);
    }

    /**
     * Given only one command creates a collection {@link #ClusterCreateCollection}, the default implementation is provided here.
     */
    public boolean isCollectionCreation() {
      return false;
    }
  }

  /**
   * Instances of this class are the fundamental building block of the CAS (Compare and Swap) update approach. These instances
   * accept an initial cluster state (as present in Zookeeper basically) and apply to it a set of modifications that are
   * then attempted to be written back to Zookeeper {@link ZkUpdateApplicator is driving this process}.
   * If the update fails (due to a concurrent update), the Zookeeper content is read again, the changes (updates) are
   * applied to it again and a new write attempt is made. This guarantees than an update does not overwrite data just
   * written by a concurrent update happening from the same or from another node.
   */
  interface StateChangeCalculator {
    String getCollectionName();

    /**
     * @return {@code true} if this updater is computing updates for creating a collection that does not exist yet.
     */
    boolean isCollectionCreation();

    /**
     * Given an initial {@link ClusterState}, computes after applying updates the cluster state to be written to state.json
     * (made available through {@link #getUpdatedClusterState()}) as well as the list of per replica operations (made available
     * through {@link #getPerReplicaStatesOps()}). Any or both of these methods will return {@code null} if there is no
     * corresponding update to apply.
     */
    void computeUpdates(ClusterState currentState);

    /**
     * Method can only be called after {@link #computeUpdates} has been called.
     * @return the new state to write into {@code state.json} or {@code null} if no update needed.
     */
    ClusterState getUpdatedClusterState();

    /**
     * Method can only be called after {@link #computeUpdates} has been called.
     * @return {@code null} when there are no per replica state ops
     */
    List<PerReplicaStatesOps> getPerReplicaStatesOps();
  }

  /**
   * This class is passed a {@link StateChangeCalculator} targeting a single collection that is able to apply an update to an
   * initial cluster state and return the updated cluster state. The {@link StateChangeCalculator} is used (possibly multiple times)
   * to do a Compare And Swap (a.k.a conditional update or CAS) of the collection's {@code state.json} Zookeeper file.<p>
   *
   * When there are per replica states to update, they are attempted once (they do their own Compare And Swap), before
   * the (potentially multiple) attempts to update the {@code state.json} file. This conforms to the strategy in place
   * when {@code state.json} updates are sent to the Overseer to do. See {@link ZkStateWriter#writePendingUpdates}.
   */
  static private class ZkUpdateApplicator {
    /**
     * When trying to update a {@code state.json} file that keeps getting changed by concurrent updater, the number of attempts
     * made before giving up. This is likely way too high, if we get to 50 failed attempts something else went wrong.
     * To be reconsidered once Collection API commands are distributed as well.
     */
    public static final int CAS_MAX_ATTEMPTS = 50;

    private final ZkStateReader zkStateReader;
    private final StateChangeCalculator updater;

    static void applyUpdate(ZkStateReader zkStateReader, StateChangeCalculator updater) throws KeeperException, InterruptedException {
      ZkUpdateApplicator zua = new ZkUpdateApplicator(zkStateReader, updater);
      zua.applyUpdate();
    }

    private ZkUpdateApplicator(ZkStateReader zkStateReader, StateChangeCalculator updater) {
      this.zkStateReader = zkStateReader;
      this.updater = updater;
    }

    /**
     * By delegating work to {@link PerReplicaStatesOps} for per replica state updates, and using optimistic locking
     * (with retries) to directly update the content of {@code state.json}, updates Zookeeper with the changes computed
     * by the {@link StateChangeCalculator}.
     */
    private void applyUpdate() throws KeeperException, InterruptedException {
      /* Initial slightly naive implementation (later on we should consider some caching between updates...).
       * For updates:
       * - Read the state.json file from Zookeeper
       * - Run the updater to execute the changes on top of that file
       * - Compare and Swap the file with the new version (fail if something else changed ZK in the meantime)
       * - Retry a few times all above steps if update is failing.
       *
       * For creations:
       * - Build the state.json file using the updater
       * - Try to write it to Zookeeper (do not overwrite if it exists)
       * - Fail (without retries) if write failed.
       */

      // Note we DO NOT track nor use the live nodes in the cluster state.
      // That may means the two abstractions (collection metadata vs. nodes) should be separated.
      // For now trying to diverge as little as possible from existing data structures and code given the need to
      // support both the old way (Overseer) and new way (distributed) of handling cluster state update.
      final Set<String> liveNodes = Collections.emptySet();

      // Per Replica States updates are done before all other updates and not subject to the number of attempts of CAS
      // made here, given they have their own CAS strategy and implementation (see PerReplicaStatesOps.persist()).
      boolean firstAttempt = true;

      // When there are multiple retries of state.json write and the cluster state gets updated over and over again with
      // the changes done in the per replica states, we avoid refetching those multiple times.
      PerReplicaStates fetchedPerReplicaStates = null;

      // Later on (when Collection API commands are distributed) we will have to rely on the version of state.json
      // to implement the replacement of Collection API locking. Then we should not blindly retry cluster state updates
      // as we do here but instead intelligently fail (or retry completely) the Collection API call when seeing that
      // state.json was changed by a concurrent command execution.
      // The loop below is ok for distributing cluster state updates from Overseer to all nodes while Collection API
      // commands are still executed on the Overseer and manage their locking the old fashioned way.
      for (int attempt = 0; attempt < CAS_MAX_ATTEMPTS; attempt++) {
        // Start by reading the current state.json (if this is an update).
        // TODO Eventually rethink the way each node manages and caches its copy of the cluster state. Knowing about all collections in the cluster might not be needed.
        ClusterState initialClusterState;
        if (updater.isCollectionCreation()) {
          initialClusterState = new ClusterState(liveNodes, Collections.emptyMap());
        } else {
          // Get the state for existing data in ZK (and if no data exists we should fail)
          initialClusterState = fetchStateForCollection();
        }

        // Apply the desired changes. Note that the cluster state passed to the chain of mutators is totally up to date
        // (it's read from ZK just above). So assumptions made in the mutators (like SliceMutator.removeReplica() deleting
        // the whole collection if it's not found) are ok. Actually in the removeReplica case, the collection will always
        // exist otherwise the call to fetchStateForCollection() above would have failed.
        updater.computeUpdates(initialClusterState);

        ClusterState updatedState = updater.getUpdatedClusterState();
        List<PerReplicaStatesOps> allStatesOps = updater.getPerReplicaStatesOps();

        if (firstAttempt && allStatesOps != null) {
          // Do the per replica states updates (if any) before the state.json update (if any)
          firstAttempt = false;

          // The parent node of the per replica state nodes happens to be the node of state.json.
          String prsParentNode = ZkStateReader.getCollectionPath(updater.getCollectionName());

          for (PerReplicaStatesOps prso : allStatesOps) {
            prso.persist(prsParentNode, zkStateReader.getZkClient());
          }
        }

        if (updatedState == null) {
          // No update to state.json needed
          return;
        }

        // Get the latest version of the collection from the cluster state first.
        // There is no notion of "cached" here (the boolean passed below) as we the updatedState is based on CollectionRef
        DocCollection docCollection = updatedState.getCollectionOrNull(updater.getCollectionName(), true);

        // If we did update per replica states and we're also updating state.json, update the content of state.json to reflect
        // the changes made to replica states. Not strictly necessary (the state source of truth is in per replica states), but nice to have...
        if (allStatesOps != null) {
          if (docCollection != null) {
            // Fetch the per replica states updates done previously or skip fetching if we already have them
            fetchedPerReplicaStates = PerReplicaStates.fetch(docCollection.getZNode(), zkStateReader.getZkClient(), fetchedPerReplicaStates);
            // Transpose the per replica states into the cluster state
            updatedState = updatedState.copyWith(updater.getCollectionName(), docCollection.copyWith(fetchedPerReplicaStates));
          }
        }

        try {
          // Try to do a conditional update (a.k.a. CAS: compare and swap).
          doStateDotJsonCasUpdate(updatedState);
          return; // state.json updated successfully.
        } catch (KeeperException.BadVersionException bve) {
          if (updater.isCollectionCreation()) {
            // Not expecting to see this exception when creating new state.json fails, so throwing it up the food chain.
            throw bve;
          }
        }
        // We've tried to update an existing state.json and got a BadVersionException. We'll try again a few times.
        // When only two threads compete, no point in waiting: if we lost this time we'll get it next time right away.
        // But if more threads compete, then waiting a bit (random delay) can improve our chances. The delay should likely
        // be proportional to the time between reading the cluster state and updating it. We can measure it in the loop above.
        // With "per replica states" collections, concurrent attempts of even just two threads are expected to be extremely rare.
      }

      // We made quite a few attempts but failed repeatedly. This is pretty bad but we can't loop trying forever.
      // Offering a job to the Overseer wouldn't usually fail if the ZK queue can be written to (but the Overseer can then
      // loop forever attempting the update).
      // We do want whoever called us to fail right away rather than to wait for a cluster change and timeout because it
      // didn't happen. Likely need to review call by call what is the appropriate behaviour, especially once Collection
      // API is distributed (because then the Collection API call will fail if the underlying cluster state update cannot
      // be done, and that's a desirable thing).
      throw new KeeperException.BadVersionException(ZkStateReader.getCollectionPath(updater.getCollectionName()));
    }

    /**
     * After the computing of the new {@link ClusterState} containing all needed updates to the collection based on what the
     * {@link StateChangeCalculator} computed, this method does an update in ZK to the collection's {@code state.json}. It is the
     * equivalent of Overseer's {@link ZkStateWriter#writePendingUpdates} (in its actions related to {@code state.json}
     * as opposed to the per replica states).
     * <p>
     * Note that in a similar way to what happens in {@link ZkStateWriter#writePendingUpdates}, collection delete is handled
     * as a special case. (see comment on {@link DistributedClusterStateUpdater.StateChangeRecorder.RecordedMutationsPlayer}
     * on why the code has to be duplicated)<p>
     *
     * <b>Note for the future:</b> Given this method is where the actually write to ZK is done, that's the place where we
     * can rebuild a DocCollection with updated zk version. Eventually if we maintain a cache of recently used collections,
     * we want to capture the updated collection and put it in the cache to avoid reading it again (unless it changed,
     * the CAS will fail and we will refresh).<p>
     *
     * This could serve as the basis for a strategy where each node does not need any view of all collections in the cluster
     * but only a cache of recently used collections (possibly not even needing watches on them, but we'll discuss this later).
     */
    private void doStateDotJsonCasUpdate(ClusterState updatedState) throws KeeperException, InterruptedException {
      String jsonPath = ZkStateReader.getCollectionPath(updater.getCollectionName());

      // Collection delete
      if (!updatedState.hasCollection(updater.getCollectionName())) {
        // We do not have a collection znode version to test we delete the right version of state.json. But this doesn't really matter:
        // if we had one, and the delete failed (because state.json got updated in the meantime), we would re-read the collection
        // state, update our version, run the CAS delete again and it will pass. Which means that one way or another, deletes are final.
        // I hope nobody deletes a collection then creates a new one with the same name immediately (although the creation should fail
        // if the znode still exists, so the creation would only succeed after the delete made it, and we're ok).
        // With Overseer based updates the same behavior can be observed: a collection update is enqueued followed by the
        // collection delete before the update was executed.
        log.debug("going to recursively delete state.json at {}", jsonPath);
        zkStateReader.getZkClient().clean(jsonPath);
      } else {
        // Collection update or creation
        DocCollection collection = updatedState.getCollection(updater.getCollectionName());
        byte[] stateJson = Utils.toJSON(singletonMap(updater.getCollectionName(), collection));

        if (updater.isCollectionCreation()) {
          // The state.json file does not exist yet (more precisely it is assumed not to exist)
          log.debug("going to create collection {}", jsonPath);
          zkStateReader.getZkClient().create(jsonPath, stateJson, CreateMode.PERSISTENT, true);
        } else {
          // We're updating an existing state.json
          if (log.isDebugEnabled()) {
            log.debug("going to update collection {} version: {}", jsonPath, collection.getZNodeVersion());
          }
          zkStateReader.getZkClient().setData(jsonPath, stateJson, collection.getZNodeVersion(), true);
        }
      }
    }

    /**
     * Creates a {@link ClusterState} with the state of an existing single collection, with no live nodes information.
     * Eventually this state should be reused across calls if it is fresh enough... (we have to deal anyway with failures
     * of conditional updates so trying to use non fresh data is ok, a second attempt will be made)
     */
    private ClusterState fetchStateForCollection() throws KeeperException, InterruptedException {
      String collectionStatePath = ZkStateReader.getCollectionPath(updater.getCollectionName());
      Stat stat = new Stat();
      byte[] data = zkStateReader.getZkClient().getData(collectionStatePath, null, stat, true);
      ClusterState clusterState = ClusterState.createFromJson(stat.getVersion(), data, Collections.emptySet());
      return clusterState;
    }
  }

  /**
   * Class handling the distributed updates of collection's Zookeeper files {@code state.json} based on multiple updates
   * applied to a single collection (as is sometimes done by *Cmd classes implementing the Collection API commands).<p>
   * Previously these updates were sent one by one to Overseer and then grouped by org.apache.solr.cloud.Overseer.ClusterStateUpdater.
   * <p>
   * Records desired changes to {@code state.json} files in Zookeeper (as are done by the family of mutator classes such as
   * {@link org.apache.solr.cloud.overseer.ClusterStateMutator}, {@link org.apache.solr.cloud.overseer.CollectionMutator}
   * etc.) in order to be able to later execute them on the actual content of the {@code state.json} files using optimistic
   * locking (and retry a few times if the optimistic locking failed).
   * <p>
   * Instances are <b>not</b> thread safe.
   */
  public static class StateChangeRecorder {
    final List<Pair<MutatingCommand, ZkNodeProps>> mutations;
    /**
     * The collection name for which are all recorded commands
     */
    final String collectionName;
    /**
     * {@code true} if recorded commands assume creation of the collection {@code state.json} file.<br>
     * {@code false} if an existing {@code state.json} is to be updated.<p>
     * <p>
     * This variable is used for defensive programming and catching issues. It might be removed once we're done removing and testing
     * the distribution of the cluster state update updates.
     */
    final boolean isCollectionCreation;

    /**
     * For collection creation recording, there should be only one actual creation (and it should be the first recorded command
     */
    boolean creationCommandRecorded = false;

    private StateChangeRecorder(String collectionName, boolean isCollectionCreation) {
      if (collectionName == null) {
        final String err = "Internal bug. collectionName=null (isCollectionCreation=" + isCollectionCreation + ")";
        log.error(err);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err);
      }
      mutations = new LinkedList<>();
      this.collectionName = collectionName;
      this.isCollectionCreation = isCollectionCreation;
    }

    /**
     * Records a mutation method and its parameters so that it can be executed later to modify the corresponding Zookeeper state.
     * Note the message is identical to the one used for communicating with Overseer (at least initially) so it also contains
     * the action in parameter {@link org.apache.solr.cloud.Overseer#QUEUE_OPERATION}, but that value is ignored here
     * in favor of the value passed in {@code command}.
     *
     * @param message the parameters associated with the command that are kept in the recorded mutations to be played
     *                later. Note that this call usually replaces a call to {@link org.apache.solr.cloud.Overseer#offerStateUpdate(byte[])}
     *                that is passed a <b>copy</b> of the data!<br>
     *                This means that if  {@code message} passed in here is reused before the recorded commands are replayed,
     *                things will break! Need to make sure all places calling this method do not reuse the data passed in
     *                (otherwise need to make a copy).
     */
    public void record(MutatingCommand command, ZkNodeProps message) {
      if (isCollectionCreation && !creationCommandRecorded) {
        // First received command should be collection creation
        if (!command.isCollectionCreation()) {
          final String err = "Internal bug. Creation of collection " + collectionName + " unexpected command " + command.name();
          log.error(err);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err);
        }
        creationCommandRecorded = true;
      } else {
        // If collection creation already received or not expected, should not get (another) one
        if (command.isCollectionCreation()) {
          final String err = "Internal bug. Creation of collection " + collectionName + " unexpected command " +
              command.name() + " (isCollectionCreation=" + isCollectionCreation + ", creationCommandRecorded=" + creationCommandRecorded + ")";
          log.error(err);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err);
        }
      }

      if (!collectionName.equals(command.getCollectionName(message))) {
        // All recorded commands must be for same collection
        final String err = "Internal bug. State change for collection " + collectionName +
            " received command " + command + " for collection " + command.getCollectionName(message);
        log.error(err);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err);
      }

      mutations.add(new Pair<>(command, message));
    }

    /**
     * This class allows taking the initial (passed in) cluster state, applying to it cluster mutations and returning the resulting
     * cluster state.
     * <p>
     * It is used to be able to try to apply multiple times a set of changes to cluster state when the Compare And Swap (conditional
     * update) fails due to concurrent modification.
     * <p>
     * For each mutation, a {@link ZkWriteCommand} is first created (capturing how the mutation impacts the cluster state), this is
     * the equivalent of what the Overseer is doing in ClusterStateUpdater.processMessage().<p>
     * <p>
     * Then, a new {@link ClusterState} is built by replacing the existing collection by its new value as computed in the
     * {@link ZkWriteCommand}. This is done by Overseer in {@link ZkStateWriter#enqueueUpdate} (and {@link ZkStateWriter} is hard
     * tu reuse because although it contains the logic for doing the update that would be needed here, it is coupled with the
     * actual instance of {@link ClusterState} being maintained, the stream of updates to be applied to it and applying
     * the per replica state changes).
     */
    private static class RecordedMutationsPlayer implements StateChangeCalculator {
      private final SolrCloudManager scm;
      private final String collectionName;
      private final boolean isCollectionCreation;
      final List<Pair<MutatingCommand, ZkNodeProps>> mutations;

      // null means no update to state.json needed. Set in computeUpdates()
      private ClusterState computedState = null;

      // null means no updates needed to the per replica state znodes. Set in computeUpdates()
      private List<PerReplicaStatesOps> replicaOpsList = null;

      RecordedMutationsPlayer(SolrCloudManager scm, String collectionName, boolean isCollectionCreation, List<Pair<MutatingCommand, ZkNodeProps>> mutations) {
        this.scm = scm;
        this.collectionName = collectionName;
        this.isCollectionCreation = isCollectionCreation;
        this.mutations = mutations;
      }

      @Override
      public String getCollectionName() {
        return collectionName;
      }

      @Override
      public boolean isCollectionCreation() {
        return isCollectionCreation;
      }

      @Override
      public void computeUpdates(ClusterState clusterState) {
        boolean hasJsonUpdates = false;
        List<PerReplicaStatesOps> perReplicaStateOps = new LinkedList<>();
        for (Pair<MutatingCommand, ZkNodeProps> mutation : mutations) {
          MutatingCommand mutatingCommand = mutation.first();
          ZkNodeProps message = mutation.second();
          try {
            ZkWriteCommand zkcmd = mutatingCommand.buildWriteCommand(scm, clusterState, message);
            if (zkcmd != ZkStateWriter.NO_OP) {
              hasJsonUpdates = true;
              clusterState = clusterState.copyWith(zkcmd.name, zkcmd.collection);
            }
            if (zkcmd.ops != null && zkcmd.ops.get() != null) {
              perReplicaStateOps.add(zkcmd.ops);
            }
          } catch (Exception e) {
            // Seems weird to skip rather than fail, but that's what Overseer is doing (see ClusterStateUpdater.processQueueItem()).
            // Maybe in the new distributed update world we should make the caller fail? (something Overseer cluster state updater can't do)
            // To be reconsidered once Collection API commands are distributed because then cluster updates are done synchronously and
            // have the opportunity to make the Collection API call fail directly.
            log.error("Distributed cluster state update could not process the current clusterstate state update message, skipping the message: {}", message, e);
          }
        }

        computedState = hasJsonUpdates ? clusterState : null;
        replicaOpsList = perReplicaStateOps.isEmpty() ? null : perReplicaStateOps;
      }

      @Override
      public ClusterState getUpdatedClusterState() {
        return computedState;
      }

      @Override
      public List<PerReplicaStatesOps> getPerReplicaStatesOps() {
        return replicaOpsList;
      }
    }

    /**
     * Using optimistic locking (and retries when needed) updates Zookeeper with the changes previously recorded by calls
     * to {@link #record(MutatingCommand, ZkNodeProps)}.
     */
    public void executeStateUpdates(SolrCloudManager scm, ZkStateReader zkStateReader) throws KeeperException, InterruptedException {
      if (log.isDebugEnabled()) {
        log.debug("Executing updates for collection " + collectionName + ", is creation=" + isCollectionCreation + ", " + mutations.size() + " recorded mutations.", new Exception("StackTraceOnly")); // nowarn
      }
      if (mutations.isEmpty()) {
        final String err = "Internal bug. Unexpected empty set of mutations to apply for collection " + collectionName;
        log.error(err);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, err);
      }

      RecordedMutationsPlayer mutationPlayer = new RecordedMutationsPlayer(scm, collectionName, isCollectionCreation, mutations);
      ZkUpdateApplicator.applyUpdate(zkStateReader, mutationPlayer);

      // TODO update stats here for the various commands executed successfully or not?
      // This would replace the stats about cluster state updates that the Collection API currently makes available using
      // the OVERSEERSTATUS command, but obviously would be per node and will not have stats about queues (since there
      // will be no queues). Would be useful in some tests though, for example TestSkipOverseerOperations.
      // Probably better to rethink what types of stats are expected from a distributed system rather than trying to present
      // those previously provided by a central server in the system (the Overseer).
    }
  }

  /**
   * This class handles the changes to be made as a result of a {@link OverseerAction#DOWNNODE} event.<p>
   *
   * Instances of this class deal with a single collection. Static method {@link #executeNodeDownStateUpdate} is the entry point
   * dealing with a node going down and processing all collections.
   */
  private static class CollectionNodeDownChangeCalculator implements StateChangeCalculator {
    private final String collectionName;
    private final String nodeName;

    // null means no update to state.json needed. Set in computeUpdates()
    private ClusterState computedState = null;

    // null means no updates needed to the per replica state znodes. Set in computeUpdates()
    private List<PerReplicaStatesOps> replicaOpsList = null;

    /**
     * Entry point to mark all replicas of all collections present on a single node as being DOWN (because the node is down)
     */
    public static void executeNodeDownStateUpdate(String nodeName, ZkStateReader zkStateReader) {
      // This code does a version of what NodeMutator.downNode() is doing. We can't assume we have a cache of the collections,
      // so we're going to read all of them from ZK, fetch the state.json for each and if it has any replicas on the
      // failed node, do an update (conditional of course) of the state.json

      // For Per Replica States collections there is still a need to read state.json, but the update of state.json is replaced
      // by a few znode deletions and creations. Might be faster or slower overall, depending on the number of impacted
      // replicas of such a collection and the total size of that collection's state.json.

      // Note code here also has to duplicate some of the work done in ZkStateReader because ZkStateReader couples reading of
      // the cluster state and maintaining a cached copy of the cluster state. Something likely to be refactored later (once
      // Overseer is totally removed and Zookeeper access patterns become clearer).

      log.debug("DownNode state change invoked for node: {}", nodeName);

      try {
        final List<String> collectionNames = zkStateReader.getZkClient().getChildren(COLLECTIONS_ZKNODE, null, true);

        // Collections are totally independent of each other. Multiple threads could share the load here (need a ZK connection for each though).
        for (String collectionName : collectionNames) {
          CollectionNodeDownChangeCalculator collectionUpdater = new CollectionNodeDownChangeCalculator(collectionName, nodeName);
          ZkUpdateApplicator.applyUpdate(zkStateReader, collectionUpdater);
        }
      } catch (Exception e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        // Overseer behavior is to log an error and carry on when a message fails. See Overseer.ClusterStateUpdater.processQueueItem()
        log.error("Could not successfully process DOWNNODE, giving up", e);
      }
    }

    private CollectionNodeDownChangeCalculator(String collectionName, String nodeName) {
      this.collectionName = collectionName;
      this.nodeName = nodeName;
    }

    @Override
    public String getCollectionName() {
      return collectionName;
    }

    @Override
    public boolean isCollectionCreation() {
      return false;
    }

    @Override
    public void computeUpdates(ClusterState clusterState) {
      final DocCollection docCollection = clusterState.getCollectionOrNull(collectionName);
      Optional<ZkWriteCommand> result = docCollection != null ? NodeMutator.computeCollectionUpdate(nodeName, collectionName, docCollection) : Optional.empty();

      if (docCollection == null) {
        // This is possible but should be rare. Logging warn in case it is seen often and likely a sign of another issue
        log.warn("Processing DOWNNODE, collection " + collectionName + " disappeared during iteration"); // nowarn
      }

      if (result.isPresent()) {
        ZkWriteCommand zkcmd = result.get();
        computedState = (zkcmd != ZkStateWriter.NO_OP) ? clusterState.copyWith(zkcmd.name, zkcmd.collection) : null;
        replicaOpsList = (zkcmd.ops != null && zkcmd.ops.get() != null) ? Collections.singletonList(zkcmd.ops) : null;
      } else {
        computedState = null;
        replicaOpsList = null;
      }
    }

    @Override
    public ClusterState getUpdatedClusterState() {
      return computedState;
    }

    @Override
    public List<PerReplicaStatesOps> getPerReplicaStatesOps() {
      return replicaOpsList;
    }
  }
}
