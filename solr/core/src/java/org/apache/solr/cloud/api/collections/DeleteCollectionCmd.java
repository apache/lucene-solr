
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

import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.NonExistentCoreException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.admin.MetricsHistoryHandler;
import org.apache.solr.metrics.SolrMetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionAdminParams.COLOCATED_WITH;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.WITH_COLLECTION;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.DELETE;
import static org.apache.solr.common.params.CommonParams.NAME;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class DeleteCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private final TimeSource timeSource;

  public DeleteCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
    this.timeSource = ocmh.cloudManager.getTimeSource();
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    log.info("delete collection called");
    Object o = message.get(MaintainRoutedAliasCmd.INVOKED_BY_ROUTED_ALIAS);
    if (o != null) {
      ((Runnable)o).run(); // this will ensure the collection is removed from the alias before it disappears.
    }
    final String extCollection = message.getStr(NAME);
    ZkStateReader zkStateReader = ocmh.zkStateReader;

    if (zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      zkStateReader.aliasesManager.update(); // aliases may have been stale; get latest from ZK
    }

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    List<String> aliasReferences = checkAliasReference(zkStateReader, extCollection, followAliases);

    Aliases aliases = zkStateReader.getAliases();

    String collection;
    if (followAliases) {
      collection = aliases.resolveSimpleAlias(extCollection);
    } else {
      collection = extCollection;
    }

    checkNotColocatedWith(zkStateReader, collection);

    final boolean deleteHistory = message.getBool(CoreAdminParams.DELETE_METRICS_HISTORY, true);

    try {
      // Remove the snapshots meta-data for this collection in ZK. Deleting actual index files
      // should be taken care of as part of collection delete operation.
      SolrZkClient zkClient = zkStateReader.getZkClient();
      SolrSnapshotManager.cleanupCollectionLevelSnapshots(zkClient, collection);

      if (zkStateReader.getClusterState().getCollectionOrNull(collection) == null) {
        if (zkStateReader.getZkClient().exists(ZkStateReader.COLLECTIONS_ZKNODE + "/" + collection)) {
          // if the collection is not in the clusterstate, but is listed in zk, do nothing, it will just
          // be removed in the finally - we cannot continue, because the below code will error if the collection
          // is not in the clusterstate
          return;
        }
      }
      // remove collection-level metrics history
      if (deleteHistory) {
        MetricsHistoryHandler historyHandler = ocmh.overseer.getCoreContainer().getMetricsHistoryHandler();
        if (historyHandler != null) {
          String registry = SolrMetricManager.getRegistryName(SolrInfoBean.Group.collection, collection);
          historyHandler.removeHistory(registry);
        }
      }
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UNLOAD.toString());
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, true);
      params.set(CoreAdminParams.DELETE_DATA_DIR, true);
      params.set(CoreAdminParams.DELETE_METRICS_HISTORY, deleteHistory);


      // nocommit
      //String asyncId = message.getStr(ASYNC);

      Set<String> okayExceptions = new HashSet<>(1);
      okayExceptions.add(NonExistentCoreException.class.getName());
      ZkNodeProps internalMsg = message.plus(NAME, collection);

      @SuppressWarnings({"unchecked"})
      List<Replica> failedReplicas = ocmh.collectionCmd(internalMsg, params, results, null, null, okayExceptions);

    } finally {
      log.info("Send DELETE operation to Overseer collection={}", collection);
      ZkNodeProps m = new ZkNodeProps(Overseer.QUEUE_OPERATION, DELETE.toLower(), NAME, collection);
      ocmh.overseer.offerStateUpdate(Utils.toJSON(m));

      // wait for a while until we don't see the collection
      zkStateReader.waitForState(collection, 10, TimeUnit.SECONDS, (collectionState) -> collectionState == null);

      // we can delete any remaining unique aliases
      if (!aliasReferences.isEmpty()) {
        ocmh.zkStateReader.aliasesManager.applyModificationAndExportToZk(a -> {
          for (String alias : aliasReferences) {
            a = a.cloneWithCollectionAlias(alias, null);
          }
          return a;
        });
      }
    }
  }

  // This method returns the single collection aliases to delete, if present, or null
  private List<String> checkAliasReference(ZkStateReader zkStateReader, String extCollection, boolean followAliases) throws Exception {
    Aliases aliases = zkStateReader.getAliases();
    List<String> aliasesRefs = referencedByAlias(extCollection, aliases, followAliases);
    List<String> aliasesToDelete = new ArrayList<>();
    if (aliasesRefs.size() > 0) {
      zkStateReader.aliasesManager.update(); // aliases may have been stale; get latest from ZK
      aliases = zkStateReader.getAliases();
      aliasesRefs = referencedByAlias(extCollection, aliases, followAliases);
      String collection = followAliases ? aliases.resolveSimpleAlias(extCollection) : extCollection;
      if (aliasesRefs.size() > 0) {
        for (String alias : aliasesRefs) {
          // for back-compat in 8.x we don't automatically remove other
          // aliases that point only to this collection
          if (!extCollection.equals(alias)) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "Collection : " + collection + " is part of aliases: " + aliasesRefs + ", remove or modify the aliases before removing this collection.");
          } else {
            aliasesToDelete.add(alias);
          }
        }
      }
    }
    return aliasesToDelete;
  }

  public static List<String> referencedByAlias(String extCollection, Aliases aliases, boolean followAliases) throws IllegalArgumentException {
    Objects.requireNonNull(aliases);
    // this quickly produces error if the name is a complex alias
    String collection = followAliases ? aliases.resolveSimpleAlias(extCollection) : extCollection;
    return aliases.getCollectionAliasListMap().entrySet().stream()
        .filter(e -> !e.getKey().equals(collection))
        .filter(e -> e.getValue().contains(collection) || e.getValue().contains(extCollection))
        .map(Map.Entry::getKey) // alias name
        .collect(Collectors.toList());
  }

  private void checkNotColocatedWith(ZkStateReader zkStateReader, String collection) throws Exception {
    DocCollection docCollection = zkStateReader.getClusterState().getCollectionOrNull(collection);
    if (docCollection != null)  {
      String colocatedWith = docCollection.getStr(COLOCATED_WITH);
      if (colocatedWith != null) {
        DocCollection colocatedCollection = zkStateReader.getClusterState().getCollectionOrNull(colocatedWith);
        if (colocatedCollection != null && collection.equals(colocatedCollection.getStr(WITH_COLLECTION))) {
          // todo how do we clean up if reverse-link is not present?
          // can't delete this collection because it is still co-located with another collection
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
              "Collection: " + collection + " is co-located with collection: " + colocatedWith
                  + " remove the link using modify collection API or delete the co-located collection: " + colocatedWith);
        }
      }
    }
  }
}
