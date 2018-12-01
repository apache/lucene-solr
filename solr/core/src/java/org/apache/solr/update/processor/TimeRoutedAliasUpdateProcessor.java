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
import java.text.ParseException;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.MaintainRoutedAliasCmd;
import org.apache.solr.cloud.api.collections.TimeRoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.apache.solr.util.DateMathParser;
import org.apache.solr.util.DefaultSolrThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.solr.common.util.ExecutorUtil.newMDCAwareSingleThreadExecutor;
import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.CreationType.ASYNC_PREEMPTIVE;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.CreationType.NONE;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.CreationType.SYNCHRONOUS;

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
  //TODO do we make this more generic to others who want to partition collections using something else besides time?

  private static final String ALIAS_DISTRIB_UPDATE_PARAM = "alias." + DISTRIB_UPDATE_PARAM; // param
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // refs to std infrastructure
  private final SolrQueryRequest req;
  private final SolrCmdDistributor cmdDistrib;
  private final CollectionsHandler collHandler;
  private final ZkController zkController;

  // Stuff specific to this class
  private final String thisCollection;
  private final TimeRoutedAlias timeRoutedAlias;
  private final SolrParams outParamsToLeader;

  // These two fields may be updated within the calling thread during processing but should
  // never be updated by any async creation thread.
  private List<Map.Entry<Instant, String>> parsedCollectionsDesc; // k=timestamp (start), v=collection.  Sorted descending
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc
  private volatile boolean executorRunning = false;

  private ExecutorService preemptiveCreationWaitExecutor = newMDCAwareSingleThreadExecutor(new DefaultSolrThreadFactory("TRA-preemptive-creation-wait"));

  public static UpdateRequestProcessor wrap(SolrQueryRequest req, UpdateRequestProcessor next) {
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
      return new TimeRoutedAliasUpdateProcessor(req, next, aliasName, aliasDistribPhase);
    }
  }

  private TimeRoutedAliasUpdateProcessor(SolrQueryRequest req, UpdateRequestProcessor next,
                                         String aliasName,
                                         DistribPhase aliasDistribPhase) {
    super(next);
    assert aliasDistribPhase == DistribPhase.NONE;
    final SolrCore core = req.getCore();
    this.thisCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.req = req;
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
    final Instant docTimestamp =
        parseRouteKey(cmd.getSolrInputDocument().getFieldValue(timeRoutedAlias.getRouteField()));

    // TODO: maybe in some cases the user would want to ignore/warn instead?
    if (docTimestamp.isAfter(Instant.now().plusMillis(timeRoutedAlias.getMaxFutureMs()))) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "The document's time routed key of " + docTimestamp + " is too far in the future given " +
              TimeRoutedAlias.ROUTER_MAX_FUTURE + "=" + timeRoutedAlias.getMaxFutureMs());
    }

    // to avoid potential for race conditions, this next method should not get called again unless
    // we have created a collection synchronously
    updateParsedCollectionAliases();

    String targetCollection = createCollectionsIfRequired(docTimestamp, cmd);

    if (thisCollection.equals(targetCollection)) {
      // pass on through; we've reached the right collection
      super.processAdd(cmd);
    } else {
      // send to the right collection
      SolrCmdDistributor.Node targetLeaderNode = routeDocToSlice(targetCollection, cmd.getSolrInputDocument());
      cmdDistrib.distribAdd(cmd, Collections.singletonList(targetLeaderNode), new ModifiableSolrParams(outParamsToLeader));
    }
  }

  /**
   * Create any required collections and return the name of the collection to which the current document should be sent.
   *
   * @param docTimestamp the date for the document taken from the field specified in the TRA config
   * @param cmd The initial calculated destination collection.
   * @return The name of the proper destination collection for the document which may or may not be a
   *         newly created collection
   */
  private String createCollectionsIfRequired(Instant docTimestamp, AddUpdateCommand cmd) {
    // Even though it is possible that multiple requests hit this code in the 1-2 sec that
    // it takes to create a collection, it's an established anti-pattern to feed data with a very large number
    // of client connections. This in mind, we only guard against spamming the overseer within a batch of
    // updates. We are intentionally tolerating a low level of redundant requests in favor of simpler code. Most
    // super-sized installations with many update clients will likely be multi-tenant and multiple tenants
    // probably don't write to the same alias. As such, we have deferred any solution to the "many clients causing
    // collection creation simultaneously" problem until such time as someone actually has that problem in a
    // real world use case that isn't just an anti-pattern.
    Map.Entry<Instant, String> candidateCollectionDesc = findCandidateGivenTimestamp(docTimestamp, cmd.getPrintableId());
    String candidateCollectionName = candidateCollectionDesc.getValue();
    try {
      switch (typeOfCreationRequired(docTimestamp, candidateCollectionDesc.getKey())) {
        case SYNCHRONOUS:
          // This next line blocks until all collections required by the current document have been created
          return createAllRequiredCollections(docTimestamp, cmd.getPrintableId(), candidateCollectionDesc);
        case ASYNC_PREEMPTIVE:
          if (!executorRunning) {
            // It's important not to add code between here and the prior call to findCandidateGivenTimestamp()
            // in processAdd() that invokes updateParsedCollectionAliases(). Doing so would update parsedCollectionsDesc
            // and create a race condition. We are relying on the fact that get(0) is returning the head of the parsed
            // collections that existed when candidateCollectionDesc was created. If this class updates it's notion of
            // parsedCollectionsDesc since candidateCollectionDesc was chosen, we could create collection n+2
            // instead of collection n+1.
            String mostRecentCollName = this.parsedCollectionsDesc.get(0).getValue();

            // This line does not block and the document can be added immediately
            preemptiveAsync(() -> createNextCollection(mostRecentCollName));
          }
          return candidateCollectionName;
        case NONE:
          return candidateCollectionName; // could use fall through, but fall through is fiddly for later editors.
        default:
          throw unknownCreateType();
      }
      // do nothing if creationType == NONE
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void preemptiveAsync(Runnable r) {
    // Note: creating an executor and throwing it away is slightly expensive, but this is only likely to happen
    // once per hour/day/week (depending on time slice size for the TRA). If the executor were retained, it
    // would need to be shut down in a close hook to avoid test failures due to thread leaks in tests which is slightly
    // more complicated from a code maintenance and readability stand point. An executor must used instead of a
    // thread to ensure we pick up the proper MDC logging stuff from ExecutorUtil.
    executorRunning  = true;
    DefaultSolrThreadFactory threadFactory = new DefaultSolrThreadFactory("TRA-preemptive-creation");
    ExecutorService preemptiveCreationExecutor = newMDCAwareSingleThreadExecutor(threadFactory);

    preemptiveCreationExecutor.execute(() -> {
      r.run();
      preemptiveCreationExecutor.shutdown();
      executorRunning = false;
    });
    
    preemptiveCreationWaitExecutor.submit(() -> ExecutorUtil.awaitTermination(preemptiveCreationExecutor));
  }

  /**
   * Determine if the a new collection will be required based on the document timestamp. Passing null for
   * preemptiveCreateInterval tells you if the document is beyond all existing collections with a response of
   * {@link CreationType#NONE} or {@link CreationType#SYNCHRONOUS}, and passing a valid date math for
   * preemptiveCreateMath additionally distinguishes the case where the document is close enough to the end of
   * the TRA to trigger preemptive creation but not beyond all existing collections with a value of
   * {@link CreationType#ASYNC_PREEMPTIVE}.
   *
   * @param docTimeStamp The timestamp from the document
   * @param targetCollectionTimestamp The timestamp for the presently selected destination collection
   * @return a {@code CreationType} indicating if and how to create a collection
   */
  private CreationType typeOfCreationRequired(Instant docTimeStamp, Instant targetCollectionTimestamp) {
    final Instant nextCollTimestamp = timeRoutedAlias.computeNextCollTimestamp(targetCollectionTimestamp);

    if (!docTimeStamp.isBefore(nextCollTimestamp)) {
      // current document is destined for a collection that doesn't exist, must create the destination
      // to proceed with this add command
      return SYNCHRONOUS;
    }

    if (isNotBlank(timeRoutedAlias.getPreemptiveCreateWindow())) {
      Instant preemptNextColCreateTime =
          calcPreemptNextColCreateTime(timeRoutedAlias.getPreemptiveCreateWindow(), nextCollTimestamp);
      if (!docTimeStamp.isBefore(preemptNextColCreateTime)) {
        return ASYNC_PREEMPTIVE;
      }
    }

    return NONE;
  }

  private Instant calcPreemptNextColCreateTime(String preemptiveCreateMath, Instant nextCollTimestamp) {
    DateMathParser dateMathParser = new DateMathParser();
    dateMathParser.setNow(Date.from(nextCollTimestamp));
    try {
      return dateMathParser.parseMath(preemptiveCreateMath).toInstant();
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Invalid Preemptive Create Window Math:'" + preemptiveCreateMath + '\'', e);
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

  /**
   * Given the route key, finds the correct collection or returns the most recent collection if the doc
   * is in the future. Future docs will potentially cause creation of a collection that does not yet exist
   * or an error if they exceed the maxFutureMs setting.
   *
   * @throws SolrException if the doc is too old to be stored in the TRA
   */
  private Map.Entry<Instant, String> findCandidateGivenTimestamp(Instant docTimestamp, String printableId) {
    // Lookup targetCollection given route key.  Iterates in reverse chronological order.
    //    We're O(N) here but N should be small, the loop is fast, and usually looking for 1st.
    for (Map.Entry<Instant, String> entry : parsedCollectionsDesc) {
      Instant colStartTime = entry.getKey();
      if (!docTimestamp.isBefore(colStartTime)) {  // i.e. docTimeStamp is >= the colStartTime
        return entry; //found it
      }
    }
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Doc " + printableId + " couldn't be routed with " + timeRoutedAlias.getRouteField() + "=" + docTimestamp);
  }

  private void createNextCollection(String mostRecentCollName) {
    // Invoke ROUTEDALIAS_CREATECOLL (in the Overseer, locked by alias name).  It will create the collection
    //   and update the alias contingent on the most recent collection name being the same as
    //   what we think so here, otherwise it will return (without error).
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
      try {
        super.doClose();
      } finally {
        ExecutorUtil.shutdownAndAwaitTermination(preemptiveCreationWaitExecutor);
      }
    }
  }

  private SolrCmdDistributor.Node routeDocToSlice(String collection, SolrInputDocument doc) {
    SchemaField uniqueKeyField = req.getSchema().getUniqueKeyField();
    // schema might not have key field...
    String idFieldName = uniqueKeyField == null ? null : uniqueKeyField.getName();
    String idValue = uniqueKeyField == null ? null : doc.getFieldValue(idFieldName).toString();
    DocCollection coll = zkController.getClusterState().getCollection(collection);
    Slice slice = coll.getRouter().getTargetSlice(idValue, doc, null, req.getParams(), coll);
    return getLeaderNode(collection, slice);
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
    final Slice[] activeSlices = zkController.getClusterState().getCollection(collection).getActiveSlicesArr();
    if (activeSlices.length == 0) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE, "Cannot route to collection " + collection);
    }
    final Slice slice = activeSlices[0];
    return getLeaderNode(collection, slice);
  }

  private SolrCmdDistributor.Node getLeaderNode(String collection, Slice slice) {
    //TODO when should we do StdNode vs RetryNode?
    final Replica leader = slice.getLeader();
    if (leader == null) {
      throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
          "No 'leader' replica available for shard " + slice.getName() + " of collection " + collection);
    }
    return new SolrCmdDistributor.ForwardNode(new ZkCoreNodeProps(leader), zkController.getZkStateReader(),
        collection, slice.getName(), DistributedUpdateProcessor.MAX_RETRIES_ON_FORWARD_DEAULT);
  }


  /**
   * Create as many collections as required. This method loops to allow for the possibility that the docTimestamp
   * requires more than one collection to be created. Since multiple threads may be invoking maintain on separate
   * requests to the same alias, we must pass in the name of the collection that this thread believes to be the most
   * recent collection. This assumption is checked when the command is executed in the overseer. When this method
   * finds that all collections required have been created it returns the (possibly new) most recent collection.
   * The return value is ignored by the calling code in the async preemptive case.
   *
   * @param docTimestamp the timestamp from the document that determines routing
   * @param printableId an identifier for the add command used in error messages
   * @param targetCollectionDesc the descriptor for the presently selected collection which should also be
   *                             the most recent collection in all cases where this method is invoked.
   * @return The latest collection, including collections created during maintenance
   */
  private String createAllRequiredCollections( Instant docTimestamp, String printableId,
                                               Map.Entry<Instant, String> targetCollectionDesc) {
    do {
      switch(typeOfCreationRequired(docTimestamp, targetCollectionDesc.getKey())) {
        case NONE:
          return targetCollectionDesc.getValue(); // we don't need another collection
        case ASYNC_PREEMPTIVE:
          // can happen when preemptive interval is longer than one time slice
          String mostRecentCollName = this.parsedCollectionsDesc.get(0).getValue();
          preemptiveAsync(() -> createNextCollection(mostRecentCollName));
          return targetCollectionDesc.getValue();
        case SYNCHRONOUS:
          createNextCollection(targetCollectionDesc.getValue()); // *should* throw if fails for some reason but...
          if (!updateParsedCollectionAliases()) { // thus we didn't make progress...
            // this is not expected, even in known failure cases, but we check just in case
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "We need to create a new time routed collection but for unknown reasons were unable to do so.");
          }
          // then retry the loop ... have to do find again in case other requests also added collections
          // that were made visible when we called updateParsedCollectionAliases()
          targetCollectionDesc = findCandidateGivenTimestamp(docTimestamp, printableId);
          break;
        default:
          throw unknownCreateType();

      }
    } while (true);
  }

  private SolrException unknownCreateType() {
    return new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown creation type while adding " +
        "document to a Time Routed Alias! This is a bug caused when a creation type has been added but " +
        "not all code has been updated to handle it.");
  }

  enum CreationType {
    NONE,
    ASYNC_PREEMPTIVE,
    SYNCHRONOUS
  }


}
