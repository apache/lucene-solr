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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.solr.cloud.ZkController;
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
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.SolrCmdDistributor;
import org.apache.solr.update.processor.DistributedUpdateProcessor.DistribPhase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.update.processor.DistributedUpdateProcessor.DISTRIB_FROM;
import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

/**
 * Distributes update requests to rolling series of collections partitioned by a timestamp field.
 *
 * Depends on this core having a special core property that points to the alias name that this collection is a part of.
 * And further requires certain metadata on the Alias.
 *
 * @since 7.2.0
 */
public class TimeRoutedAliasUpdateProcessor extends UpdateRequestProcessor {
  //TODO do we make this more generic to others who want to partition collections using something else?

  // TODO auto add new collection partitions when cross a timestamp boundary.  That needs to be coordinated to avoid
  //   race conditions, remembering that even the lead collection might have multiple instances of this URP
  //   (multiple shards or perhaps just multiple streams thus instances of this URP)

  public static final String ALIAS_DISTRIB_UPDATE_PARAM = "alias." + DISTRIB_UPDATE_PARAM; // param
  public static final String TIME_PARTITION_ALIAS_NAME_CORE_PROP = "timePartitionAliasName"; // core prop
  public static final String ROUTER_FIELD_METADATA = "router.field"; // alias metadata

  // This format must be compatible with collection name limitations
  private static final DateTimeFormatter DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE).appendPattern("[_HH[_mm[_ss]]]") //brackets mean optional
      .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
      .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
      .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
      .toFormatter(Locale.ROOT).withZone(ZoneOffset.UTC);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String thisCollection;
  private final String aliasName;
  private final String routeField;

  private final SolrCmdDistributor cmdDistrib;
  private final ZkController zkController;
  private final SolrParams outParamsToLeader;

  private List<Map.Entry<Instant, String>> parsedCollectionsDesc; // k=timestamp (start), v=collection.  Sorted descending
  private Aliases parsedCollectionsAliases; // a cached reference to the source of what we parse into parsedCollectionsDesc

  public static UpdateRequestProcessor wrap(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    final String timePartitionAliasName = req.getCore().getCoreDescriptor()
        .getCoreProperty(TIME_PARTITION_ALIAS_NAME_CORE_PROP, null);
    final DistribPhase shardDistribPhase =
        DistribPhase.parseParam(req.getParams().get(DISTRIB_UPDATE_PARAM));
    final DistribPhase aliasDistribPhase =
        DistribPhase.parseParam(req.getParams().get(ALIAS_DISTRIB_UPDATE_PARAM));
    if (timePartitionAliasName == null || aliasDistribPhase != DistribPhase.NONE || shardDistribPhase != DistribPhase.NONE) {
      // if aliasDistribPhase is not NONE, then there is no further collection routing to be done here.
      //    TODO this may eventually not be true but at the moment it is
      // if shardDistribPhase is not NONE, then the phase is after the scope of this URP
      return next;
    } else {
      return new TimeRoutedAliasUpdateProcessor(req, rsp, next, timePartitionAliasName, aliasDistribPhase);
    }
  }

  protected TimeRoutedAliasUpdateProcessor(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next,
                                           String aliasName,
                                           DistribPhase aliasDistribPhase) {
    super(next);
    assert aliasDistribPhase == DistribPhase.NONE;
    final SolrCore core = req.getCore();
    this.thisCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.aliasName = aliasName;
    CoreContainer cc = core.getCoreContainer();
    zkController = cc.getZkController();
    cmdDistrib = new SolrCmdDistributor(cc.getUpdateShardHandler());

    final Map<String, String> aliasMetadata = zkController.getZkStateReader().getAliases().getCollectionAliasMetadata(aliasName);
    if (aliasMetadata == null) {
      throw newAliasMustExistException(); // if it did exist, we'd have a non-null map
    }
    routeField = aliasMetadata.get(ROUTER_FIELD_METADATA);

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

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    final Object routeValue = cmd.getSolrInputDocument().getFieldValue(routeField);
    final String targetCollection = findTargetCollectionGivenRouteKey(routeValue);
    if (targetCollection == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Doc " + cmd.getPrintableId() + " couldn't be routed with " + routeField + "=" + routeValue);
    }
    if (thisCollection.equals(targetCollection)) {
      // pass on through; we've reached the right collection
      super.processAdd(cmd);
    } else {
      // send to the right collection
      SolrCmdDistributor.Node targetLeaderNode = lookupShardLeaderOfCollection(targetCollection);
      cmdDistrib.distribAdd(cmd, Collections.singletonList(targetLeaderNode), new ModifiableSolrParams(outParamsToLeader));
    }
  }

  protected String findTargetCollectionGivenRouteKey(Object routeKey) {
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
    final Aliases aliases = zkController.getZkStateReader().getAliases(); // note: might be different from last request
    if (this.parsedCollectionsAliases != aliases) {
      if (this.parsedCollectionsAliases != null) {
        log.info("Observing possibly updated alias {}", aliasName);
      }
      this.parsedCollectionsDesc = doParseCollections(aliases);
      this.parsedCollectionsAliases = aliases;
    }
    // iterates in reverse chronological order
    //    We're O(N) here but N should be small, the loop is fast, and usually looking for 1st.
    for (Map.Entry<Instant, String> entry : parsedCollectionsDesc) {
      Instant colStartTime = entry.getKey();
      if (!docTimestamp.isBefore(colStartTime)) {  // i.e. docTimeStamp is >= the colStartTime
        return entry.getValue(); //found it
      }
    }
    return null;
  }

  /** Parses the timestamp from the collection list and returns them in reverse sorted order (newest 1st) */
  private List<Map.Entry<Instant,String>> doParseCollections(Aliases aliases) {
    final List<String> collections = aliases.getCollectionAliasListMap().get(aliasName);
    if (collections == null) {
      throw newAliasMustExistException();
    }
    // note: I considered TreeMap but didn't like the log(N) just to grab the head when we use it later
    List<Map.Entry<Instant,String>> result = new ArrayList<>(collections.size());
    for (String collection : collections) {
      Instant colStartTime = parseInstantFromCollectionName(aliasName, collection);
      result.add(new AbstractMap.SimpleImmutableEntry<>(colStartTime, collection));
    }
    result.sort((e1, e2) -> e2.getKey().compareTo(e1.getKey())); // reverse sort by key
    return result;
  }

  private SolrException newAliasMustExistException() {
    throw new SolrException(SolrException.ErrorCode.SERVICE_UNAVAILABLE,
        "Collection " + thisCollection + " created for use with alias " + aliasName + " which doesn't exist anymore." +
            " You cannot write to this unless the alias exists.");
  }

  static Instant parseInstantFromCollectionName(String aliasName, String collection) {
    final String dateTimePart = collection.substring(aliasName.length() + 1);
    return DATE_TIME_FORMATTER.parse(dateTimePart, Instant::from);
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
    List<String> collections = aliases.getCollectionAliasListMap().get(aliasName);
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
