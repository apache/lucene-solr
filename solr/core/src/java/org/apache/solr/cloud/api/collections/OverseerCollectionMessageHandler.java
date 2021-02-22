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

import com.google.common.collect.ImmutableMap;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.LockTree;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerMessageHandler;
import org.apache.solr.cloud.OverseerNodePrioritizer;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.cloud.Stats;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.handler.component.HttpShardHandlerFactory;
import org.apache.solr.logging.MDCLoggingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.solr.common.cloud.ZkStateReader.*;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * A {@link OverseerMessageHandler} that handles Collections API related overseer messages.<p>
 *
 * A lot of the content that was in this class got moved to {@link CollectionHandlingUtils} and {@link CollApiCmds}.
 */
public class OverseerCollectionMessageHandler implements OverseerMessageHandler, SolrCloseable {


  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  Overseer overseer;
  HttpShardHandlerFactory shardHandlerFactory;
  String adminPath;
  ZkStateReader zkStateReader;
  SolrCloudManager cloudManager;
  String myId;
  Stats stats;
  TimeSource timeSource;
  private final CollectionCommandContext ccc;

  // Set that tracks collections that are currently being processed by a running task.
  // This is used for handling mutual exclusion of the tasks.

  final private LockTree lockTree = new LockTree();
  ExecutorService tpe = new ExecutorUtil.MDCAwareThreadPoolExecutor(5, 10, 0L, TimeUnit.MILLISECONDS,
      new SynchronousQueue<>(),
      new SolrNamedThreadFactory("OverseerCollectionMessageHandlerThreadFactory"));

  final private Map<CollectionAction, CollApiCmds.CollectionApiCommand> commandMap;

  private volatile boolean isClosed;

  public OverseerCollectionMessageHandler(ZkStateReader zkStateReader, String myId,
                                        final HttpShardHandlerFactory shardHandlerFactory,
                                        String adminPath,
                                        Stats stats,
                                        Overseer overseer,
                                        OverseerNodePrioritizer overseerPrioritizer) {
    this.zkStateReader = zkStateReader;
    this.shardHandlerFactory = shardHandlerFactory;
    this.adminPath = adminPath;
    this.myId = myId;
    this.stats = stats;
    this.overseer = overseer;
    this.cloudManager = overseer.getSolrCloudManager();
    this.timeSource = cloudManager.getTimeSource();
    this.isClosed = false;
    ccc = new OcmhCollectionCommandContext(this);
    commandMap = new ImmutableMap.Builder<CollectionAction, CollApiCmds.CollectionApiCommand>()
        .put(REPLACENODE, new ReplaceNodeCmd(ccc))
        .put(DELETENODE, new DeleteNodeCmd(ccc))
        .put(BACKUP, new BackupCmd(ccc))
        .put(RESTORE, new RestoreCmd(ccc))
        .put(DELETEBACKUP, new DeleteBackupCmd(ccc))
        .put(CREATESNAPSHOT, new CreateSnapshotCmd(ccc))
        .put(DELETESNAPSHOT, new DeleteSnapshotCmd(ccc))
        .put(SPLITSHARD, new SplitShardCmd(ccc))
        .put(ADDROLE, new OverseerRoleCmd(ccc, ADDROLE, overseerPrioritizer))
        .put(REMOVEROLE, new OverseerRoleCmd(ccc, REMOVEROLE, overseerPrioritizer))
        .put(MOCK_COLL_TASK, new CollApiCmds.MockOperationCmd())
        .put(MOCK_SHARD_TASK, new CollApiCmds.MockOperationCmd())
        .put(MOCK_REPLICA_TASK, new CollApiCmds.MockOperationCmd())
        .put(CREATESHARD, new CreateShardCmd(ccc))
        .put(MIGRATE, new MigrateCmd(ccc))
        .put(CREATE, new CreateCollectionCmd(ccc))
        .put(MODIFYCOLLECTION, new CollApiCmds.ModifyCollectionCmd(ccc))
        .put(ADDREPLICAPROP, new CollApiCmds.AddReplicaPropCmd(ccc))
        .put(DELETEREPLICAPROP, new CollApiCmds.DeleteReplicaPropCmd(ccc))
        .put(BALANCESHARDUNIQUE, new CollApiCmds.BalanceShardsUniqueCmd(ccc))
        .put(REBALANCELEADERS, new CollApiCmds.RebalanceLeadersCmd(ccc))
        .put(RELOAD, new CollApiCmds.ReloadCollectionCmd(ccc))
        .put(DELETE, new DeleteCollectionCmd(ccc))
        .put(CREATEALIAS, new CreateAliasCmd(ccc))
        .put(DELETEALIAS, new DeleteAliasCmd(ccc))
        .put(ALIASPROP, new SetAliasPropCmd(ccc))
        .put(MAINTAINROUTEDALIAS, new MaintainRoutedAliasCmd(ccc))
        .put(OVERSEERSTATUS, new OverseerStatusCmd(ccc))
        .put(DELETESHARD, new DeleteShardCmd(ccc))
        .put(DELETEREPLICA, new DeleteReplicaCmd(ccc))
        .put(ADDREPLICA, new AddReplicaCmd(ccc))
        .put(MOVEREPLICA, new MoveReplicaCmd(ccc))
        .put(REINDEXCOLLECTION, new ReindexCollectionCmd(ccc))
        .put(RENAME, new RenameCmd(ccc))
        .build()
    ;
  }

  @Override
  @SuppressWarnings("unchecked")
  public OverseerSolrResponse processMessage(ZkNodeProps message, String operation) {
    MDCLoggingContext.setCollection(message.getStr(COLLECTION));
    MDCLoggingContext.setShard(message.getStr(SHARD_ID_PROP));
    MDCLoggingContext.setReplica(message.getStr(REPLICA_PROP));
    log.debug("OverseerCollectionMessageHandler.processMessage : {} , {}", operation, message);

    @SuppressWarnings({"rawtypes"})
    NamedList results = new NamedList();
    try {
      CollectionAction action = getCollectionAction(operation);
      CollApiCmds.CollectionApiCommand command = commandMap.get(action);
      if (command != null) {
        command.call(cloudManager.getClusterStateProvider().getClusterState(), message, results);
      } else {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:"
            + operation);
      }
    } catch (Exception e) {
      String collName = message.getStr("collection");
      if (collName == null) collName = message.getStr(NAME);

      if (collName == null) {
        SolrException.log(log, "Operation " + operation + " failed", e);
      } else  {
        SolrException.log(log, "Collection: " + collName + " operation: " + operation
            + " failed", e);
      }

      results.add("Operation " + operation + " caused exception:", e);
      SimpleOrderedMap<Object> nl = new SimpleOrderedMap<>();
      nl.add("msg", e.getMessage());
      nl.add("rspCode", e instanceof SolrException ? ((SolrException)e).code() : -1);
      results.add("exception", nl);
    }
    return new OverseerSolrResponse(results);
  }

  private CollectionAction getCollectionAction(String operation) {
    CollectionAction action = CollectionAction.get(operation);
    if (action == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Unknown operation:" + operation);
    }
    return action;
  }

  @Override
  public String getName() {
    return "Overseer Collection Message Handler";
  }

  @Override
  public String getTimerName(String operation) {
    return "collection_" + operation;
  }

  @Override
  public String getTaskKey(ZkNodeProps message) {
    return message.containsKey(COLLECTION_PROP) ?
      message.getStr(COLLECTION_PROP) : message.getStr(NAME);
  }

  // -1 is not a possible batchSessionId so -1 will force initialization of lockSession
  private long sessionId = -1;
  private LockTree.Session lockSession;

  /**
   * Grabs an exclusive lock for this particular task.
   * @return <code>null</code> if locking is not possible. When locking is not possible, it will remain
   * impossible for the passed value of <code>batchSessionId</code>. This is to guarantee tasks are executed
   * in queue order (and a later task is not run earlier than its turn just because it happens that a lock got released).
   */
  @Override
  public Lock lockTask(ZkNodeProps message, long batchSessionId) {
    if (sessionId != batchSessionId) {
      //this is always called in the same thread.
      //Each batch is supposed to have a new taskBatch
      //So if taskBatch changes we must create a new Session
      lockSession = lockTree.getSession();
      sessionId = batchSessionId;
    }

    return lockSession.lock(getCollectionAction(message.getStr(Overseer.QUEUE_OPERATION)),
        Arrays.asList(
            getTaskKey(message),
            message.getStr(ZkStateReader.SHARD_ID_PROP),
            message.getStr(ZkStateReader.REPLICA_PROP))
    );
  }

  @Override
  public void close() throws IOException {
    this.isClosed = true;
    if (tpe != null) {
      if (!tpe.isShutdown()) {
        ExecutorUtil.shutdownAndAwaitTermination(tpe);
      }
    }
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }
}
