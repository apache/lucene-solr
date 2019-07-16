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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.RoutedAlias;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
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
 * Distributes update requests to a series of collections partitioned by a "routing" field.  Issues
 * requests to create new collections on-demand.
 *
 * Depends on this core having a special core property that points to the alias name that this collection is a part of.
 * And further requires certain properties on the Alias. Collections pointed to by the alias must be named for the alias
 * plus underscored ('_') and a routing specifier specific to the type of routed alias. These collections should not be
 * created by the user, but are created automatically by the routed alias.
 *
 * @since 7.2.0 (formerly known as TimeRoutedAliasUpdateProcessor)
 */
public class RoutedAliasUpdateProcessor extends UpdateRequestProcessor {

  private static final String ALIAS_DISTRIB_UPDATE_PARAM = "alias." + DISTRIB_UPDATE_PARAM; // param
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // make sure we don't request collection properties any more frequently than once a minute during
  // slow continuous indexing, and even less frequently during bulk indexing. (cache is updated by zk
  // watch instead of re-requested until indexing has been stopped for the duration specified here)
  public static final int CACHE_FOR_MILLIS = 60000;

  // refs to std infrastructure
  private final SolrQueryRequest req;
  private final SolrCmdDistributor cmdDistrib;
  private final ZkController zkController;

  // Stuff specific to this class
  private final String thisCollection;
  private final RoutedAlias routedAlias;
  private final SolrParams outParamsToLeader;


  public static UpdateRequestProcessor wrap(SolrQueryRequest req, UpdateRequestProcessor next) {
    String aliasName = null;
    // Demeter please don't arrest us... hide your eyes :(
    // todo: a core should have a more direct way of finding a collection name, and the collection properties
    SolrCore core = req.getCore();
    CoreDescriptor coreDescriptor = core.getCoreDescriptor();
    CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
    if (cloudDescriptor != null) {
      String collectionName = cloudDescriptor.getCollectionName();
      CoreContainer coreContainer = core.getCoreContainer();
      ZkController zkController = coreContainer.getZkController();
      ZkStateReader zkStateReader = zkController.getZkStateReader();
      Map<String, String> collectionProperties = zkStateReader.getCollectionProperties(collectionName, CACHE_FOR_MILLIS);
      aliasName = collectionProperties.get(RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP);
    }
    // fall back on core properties (legacy)
    if (StringUtils.isBlank(aliasName)) {
      aliasName = coreDescriptor.getCoreProperty(RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, null);
    }
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
      try {
        RoutedAlias alias = RoutedAlias.fromProps(aliasName, getAliasProps(req, aliasName));
        return new RoutedAliasUpdateProcessor(req, next, aliasDistribPhase, alias);
      } catch (Exception e) { // ensure we throw SERVER_ERROR not BAD_REQUEST at this stage
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Routed alias has invalid properties: " + e, e);
      }

    }
  }

  private static Map<String, String> getAliasProps(SolrQueryRequest req, String aliasName) {
    ZkController zkController = req.getCore().getCoreContainer().getZkController();
    final Map<String, String> aliasProperties = zkController.getZkStateReader().getAliases().getCollectionAliasProperties(aliasName);
    if (aliasProperties.isEmpty()) {
      throw RoutedAlias.newAliasMustExistException(aliasName); // if it did exist, we'd have a non-null map
    }
    return aliasProperties;
  }

  private RoutedAliasUpdateProcessor(SolrQueryRequest req, UpdateRequestProcessor next,
                                     DistribPhase aliasDistribPhase, RoutedAlias routedAlias) {
    super(next);
    this.routedAlias = routedAlias;
    assert aliasDistribPhase == DistribPhase.NONE;
    final SolrCore core = req.getCore();
    final CoreContainer cc = core.getCoreContainer();
    this.thisCollection = core.getCoreDescriptor().getCloudDescriptor().getCollectionName();
    this.req = req;
    this.zkController = cc.getZkController();
    this.cmdDistrib = new SolrCmdDistributor(cc.getUpdateShardHandler());



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
    return routedAlias.getAliasName();
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    routedAlias.validateRouteValue(cmd);

    // to avoid potential for race conditions, this next method should not get called again unless
    // we have created a collection synchronously
    routedAlias.updateParsedCollectionAliases(this.zkController.zkStateReader, false);

    String targetCollection = routedAlias.createCollectionsIfRequired(cmd);

    if (thisCollection.equals(targetCollection)) {
      // pass on through; we've reached the right collection
      super.processAdd(cmd);
    } else {
      // send to the right collection
      SolrCmdDistributor.Node targetLeaderNode = routeDocToSlice(targetCollection, cmd.getSolrInputDocument());
      cmdDistrib.distribAdd(cmd, Collections.singletonList(targetLeaderNode), new ModifiableSolrParams(outParamsToLeader));
    }
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
      throw RoutedAlias.newAliasMustExistException(getAliasName());
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

}
