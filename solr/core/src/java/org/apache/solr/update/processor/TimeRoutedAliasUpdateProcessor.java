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

package org.apache.solr.update.processor;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.MaintainRoutedAliasCmd;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * Distributes update requests to a rolling series of collections partitioned by a timestamp field.  Issues
 * requests to create new collections on-demand.
 *
 * Depends on this core having a special core property that points to the alias name that this collection is a part of.
 * And further requires certain properties on the Alias. Collections pointed to by the alias must be named for the alias
 * plus underscored ('_') and a time stamp of ISO_DATE plus optionally _HH_mm_ss. These collections should not be
 * created by the user, but are created automatically by the time partitioning system.
 *
 * @since 7.2.0
 */
public class TimeRoutedAliasUpdateProcessor extends UpdateRequestProcessor {
  //TODO do we make this more generic to others who want to partition collections using something else?

  public static final String ALIAS_DISTRIB_UPDATE_PARAM = "alias." + DISTRIB_UPDATE_PARAM; // param

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // To avoid needless/redundant concurrent communication with the Overseer from this JVM, we
  //   maintain a Semaphore from an alias name keyed ConcurrentHashMap.
  //   Alternatively a Lock or CountDownLatch could have been used but they didn't seem
  //   to make it any easier.
  private static ConcurrentHashMap<String, Semaphore> aliasToSemaphoreMap = new ConcurrentHashMap<>(4);

  private final String thisCollection;

  private final TimeRoutedAlias timeRoutedAlias;

  private final ZkController zkController;
  private final SolrCmdDistributor cmdDistrib;
  private final CollectionsHandler collHandler;
  private final SolrParams outParamsToLeader;

  private List<Map.Entry<Instant, String>> parsedCollectionsDesc; // k=timestamp (start), v=collection.  Sorted descending
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc

  public static UpdateRequestProcessor wrap(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    //TODO get from "Collection property"
    final String aliasName = req.getCore().getCoreDescriptor()
        .getCoreProperty(TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, null);
    final DistribPhase shardDistribPhase =
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    final DistribPhase aliasDistribPhase =
        DistribPhase.parseParam(req.getParams().get(ALIAS_DISTRIB_UPDATE_PARAM));
    if (aliasName == null || aliasDistribPhase != DistribPhase.NONE || shardDistribPhase != DistribPhase.NONE) {
      // if aliasDistribPhase is not NONE, then there is no further collection routing to be done here.
      //    TODO this may eventually not be true but at the moment it is
      // if shardDistribPhase is not NONE, then the phase is after the scope of this URP
      return next;
    } else {
      return new TimeRoutedAliasUpdateProcessor(req, rsp, next, aliasName, aliasDistribPhase);
    }
  }

  protected TimeRoutedAliasUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next,
                                           String aliasName,
                                           DistribPhase aliasDistribPhase) {
    super(next);
    assert aliasDistribPhase == DistribPhase.NONE;
    final SolrCore core = req.getCore();
    this.thisCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    CoreContainer cc = core.getCoreContainer();
    zkController = cc.getZkController();
    cmdDistrib = new SolrCmdDistributor(cc.getUpdateShardHandler());
    collHandler = cc.getCollectionsHandler();

    final Map<String, String> aliasProperties = zkController.getZkStateReader().getAliases().getCollectionAliasProperties(aliasName);
    if (aliasProperties == null) {
      throw newAliasMustExistException(); // if it did exist, we'd have a non-null map
    }
    try {
      this.timeRoutedAlias = new TimeRoutedAlias(aliasName, aliasProperties);
    } catch (Exception e) { // ensure we throw SERVER_ERROR not BAD_REQUEST at this stage
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Routed alias has invalid properties: " + e, e);
    }

    ModifiableSolrParams outParams = new ModifiableSolrParams(req.getParams());
    // Don't distribute these params; they will be distributed from the local processCommit separately.
    //   (See RequestHandlerUtils.handleCommit from which this list was retrieved from)
    outParams.remove(UpdateParams.OPTIMIZE);
    outParams.remove(UpdateParams.COMMIT);
    outParams.remove(UpdateParams.SOFT_COMMIT);
    outParams.remove(UpdateParams.PREPARE_COMMIT);
    outParams.remove(UpdateParams.ROLLBACK);
    // Add these...
    //  Ensures we skip over URPs prior to DistributedURP (see UpdateRequestProcessorChain)
    outParams.set(DISTRIB_UPDATE_PARAM, DistribPhase.NONE.toString());
    //  Signal this is a distributed search from this URP (see #wrap())
    outParams.set(ALIAS_DISTRIB_UPDATE_PARAM, DistribPhase.TOLEADER.toString());
    outParams.set(DISTRIB_FROM, ZkCoreNodeProps.getCoreUrl(zkController.getBaseUrl(), core.getName()));
    outParamsToLeader = outParams;
  }

  private String getAliasName() {
    return timeRoutedAlias.getAliasName();
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    final Object routeValue = cmd.getSolrInputDocument().getFieldValue(timeRoutedAlias.getRouteField());
    final Instant routeTimestamp = parseRouteKey(routeValue);

    updateParsedCollectionAliases();
    String targetCollection;
    do { // typically we don't loop; it's only when we need to create a collection
      targetCollection = findTargetCollectionGivenTimestamp(routeTimestamp);

      if (targetCollection == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Doc " + cmd.getPrintableId() + " couldn't be routed with " + timeRoutedAlias.getRouteField() + "=" + routeTimestamp);
      }

      // Note: the following rule is tempting but not necessary and is not compatible with
      // only using this URP when the alias distrib phase is NONE; otherwise a doc may be routed to from a non-recent
      // collection to the most recent only to then go there directly instead of realizing a new collection is needed.
      //      // If it's going to some other collection (not "this") then break to just send it there
      //      if (!thisCollection.equals(targetCollection)) {
      //        break;
      //      }
      // Also tempting but not compatible:  check that we're the leader, if not then break

      // If the doc goes to the most recent collection then do some checks below, otherwise break the loop.
      final Instant mostRecentCollTimestamp = parsedCollectionsDesc.get(0).getKey();
      final String mostRecentCollName = parsedCollectionsDesc.get(0).getValue();
      if (!mostRecentCollName.equals(targetCollection)) {
        break;
      }

      // Check the doc isn't too far in the future
      final Instant maxFutureTime = Instant.now().plusMillis(timeRoutedAlias.getMaxFutureMs());
      if (routeTimestamp.isAfter(maxFutureTime)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "The document's time routed key of " + routeValue + " is too far in the future given " +
                TimeRoutedAlias.ROUTER_MAX_FUTURE + "=" + timeRoutedAlias.getMaxFutureMs());
      }

      // Create a new collection?
      final Instant nextCollTimestamp = timeRoutedAlias.computeNextCollTimestamp(mostRecentCollTimestamp);
      if (routeTimestamp.isBefore(nextCollTimestamp)) {
        break; // thus we don't need another collection
      }

      createCollectionAfter(mostRecentCollName); // *should* throw if fails for some reason but...
      final boolean updated = updateParsedCollectionAliases();
      if (!updated) { // thus we didn't make progress...
        // this is not expected, even in known failure cases, but we check just in case
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "We need to create a new time routed collection but for unknown reasons were unable to do so.");
      }
      // then retry the loop ...
    } while(true);
    assert targetCollection != null;

    if (thisCollection.equals(targetCollection)) {
      // pass on through; we've reached the right collection
      super.processAdd(cmd);
    } else {
      // send to the right collection
      SolrCmdDistributor.Node targetLeaderNode = lookupShardLeaderOfCollection(targetCollection);
      cmdDistrib.distribAdd(cmd, Collections.singletonList(targetLeaderNode), new ModifiableSolrParams(outParamsToLeader));
    }
  }

  private Instant parseRouteKey(Object routeKey) {
    final Instant docTimestamp;
    if (routeKey instanceof Instant) {
      docTimestamp = (Instant) routeKey;
    } else if (routeKey instanceof Date) {
      docTimestamp = ((Date)routeKey).toInstant();
    } else if (routeKey instanceof CharSequence) {
      docTimestamp = Instant.parse((CharSequence)routeKey);
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unexpected type of routeKey: " + routeKey);
    }
    return docTimestamp;
  }

  /**
   * Ensure {@link #parsedCollectionsAliases} is up to date. If it was modified, return true.
   * Note that this will return true if some other alias was modified or if properties were modified. These
   * are spurious and the caller should be written to be tolerant of no material changes.
   */
  private boolean updateParsedCollectionAliases() {
    final Aliases aliases = zkController.getZkStateReader().getAliases(); // note: might be different from last request
    if (this.parsedCollectionsAliases != aliases) {
      if (this.parsedCollectionsAliases != null) {
        log.debug("Observing possibly updated alias: {}", getAliasName());
      }
      this.parsedCollectionsDesc = timeRoutedAlias.parseCollections(aliases, this::newAliasMustExistException);
      this.parsedCollectionsAliases = aliases;
      return true;
    }
    return false;
  }

  /** Given the route key, finds the collection.  Returns null if too old to go in last one. */
  private String findTargetCollectionGivenTimestamp(Instant docTimestamp) {
    // Lookup targetCollection given route key.  Iterates in reverse chronological order.
    //    We're O(N) here but N should be small, the loop is fast, and usually looking for 1st.
    for (Map.Entry<Instant, String> entry : parsedCollectionsDesc) {
      Instant colStartTime = entry.getKey();
      if (!docTimestamp.isBefore(colStartTime)) {  // i.e. docTimeStamp is >= the colStartTime
        return entry.getValue(); //found it
      }
    }
    return null; //not found
  }

  private void createCollectionAfter(String mostRecentCollName) {
    // Invoke ROUTEDALIAS_CREATECOLL (in the Overseer, locked by alias name).  It will create the collection
    //   and update the alias contingent on the most recent collection name being the same as
    //   what we think so here, otherwise it will return (without error).

    // (see docs on aliasToSemaphoreMap)
    final Semaphore semaphore = aliasToSemaphoreMap.computeIfAbsent(getAliasName(), n -> new Semaphore(1));
    if (semaphore.tryAcquire()) {
      try {
        MaintainRoutedAliasCmd.remoteInvoke(collHandler, getAliasName(), mostRecentCollName);
        // we don't care about the response.  It's possible no collection was created because
        //  of a race and that's okay... we'll ultimately retry any way.

        // Ensure our view of the aliases has updated. If we didn't do this, our zkStateReader might
        //  not yet know about the new alias (thus won't see the newly added collection to it), and we might think
        //  we failed.
        zkController.getZkStateReader().aliasesManager.update();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
      } finally {
        semaphore.release(); // to signal we're done to anyone waiting on it
      }

    } else {
      // Failed to acquire permit because another URP instance on this JVM is creating a collection.
      // So wait till it's available
      log.debug("Collection creation is already in progress so we'll wait then try again.");
      try {
        if (semaphore.tryAcquire(DEFAULT_COLLECTION_OP_TIMEOUT, TimeUnit.MILLISECONDS)) {
          semaphore.release(); // we don't actually want a permit so give it back
          // return to continue...
        } else {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "Waited too long for another update thread to be done with collection creation.");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Interrupted waiting on collection creation.", e); // if we were interrupted, give up.
      }
    }
  }

  private SolrException newAliasMustExistException() {
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Collection " + thisCollection + " created for use with alias " + getAliasName() + " which doesn't exist anymore." +
            " You cannot write to this unless the alias exists.");
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    final List<SolrCmdDistributor.Node> nodes = lookupShardLeadersOfCollections();
    cmdDistrib.distribDelete(cmd, nodes, new ModifiableSolrParams(outParamsToLeader));
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {
    final List<SolrCmdDistributor.Node> nodes = lookupShardLeadersOfCollections();
    cmdDistrib.distribCommit(cmd, nodes, new ModifiableSolrParams(outParamsToLeader));
    cmdDistrib.blockAndDoRetries(); //TODO shouldn't distribCommit do this implicitly?  It doesn't.
  }

// Not supported by SolrCmdDistributor and is sketchy any way
//  @Override
//  public void processRollback(RollbackUpdateCommand cmd) throws IOException {
//  }

  @Override
  public void finish() throws IOException {
    try {
      cmdDistrib.finish();
      final List<SolrCmdDistributor.Error> errors = cmdDistrib.getErrors();
      if (!errors.isEmpty()) {
        throw new DistributedUpdateProcessor.DistributedUpdatesAsyncException(errors);
      }
    } finally {
      super.finish();
    }
  }

  @Override
  protected void doClose() {
    try {
      cmdDistrib.close();
    } finally {
      super.doClose();
    }
  }

  private List<SolrCmdDistributor.Node> lookupShardLeadersOfCollections() {
    final Aliases aliases = zkController.getZkStateReader().getAliases();
    List<String> collections = aliases.getCollectionAliasListMap().get(getAliasName());
    if (collections == null) {
      throw newAliasMustExistException();
    }
    return collections.stream().map(this::lookupShardLeaderOfCollection).collect(Collectors.toList());
  }

  private SolrCmdDistributor.Node lookupShardLeaderOfCollection(String collection) {
    //TODO consider router to get the right slice.  Refactor common code in CloudSolrClient & DistributedUrp
    final Collection<Slice> activeSlices = zkController.getClusterState().getCollection(collection).getActiveSlices();
    if (activeSlices.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Cannot route to collection " + collection);
    }
    final Slice slice = activeSlices.iterator().next();
    //TODO when should we do StdNode vs RetryNode?
    final Replica leader = slice.getLeader();
    if (leader == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "No 'leader' replica available for shard " + slice.getName() + " of collection " + collection);
    }
    return new SolrCmdDistributor.RetryNode(new ZkCoreNodeProps(leader), zkController.getZkStateReader(),
        collection, null);
  }

}
