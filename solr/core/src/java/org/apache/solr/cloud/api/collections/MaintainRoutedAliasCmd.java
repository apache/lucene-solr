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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.NAME;

public class MaintainRoutedAliasCmd extends AliasCmd {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  static final String INVOKED_BY_ROUTED_ALIAS = "invokedByRoutedAlias";
  static final String ROUTED_ALIAS_TARGET_COL = "routedAliasTargetCol";

  MaintainRoutedAliasCmd(OverseerCollectionMessageHandler ocmh) {
    super(ocmh);
  }

  /**
   * Invokes this command from the client.  If there's a problem it will throw an exception.
   * Please note that is important to never add async to this invocation. This method must
   * block (up to the standard OCP timeout) to prevent large batches of add's from sending a message
   * to the overseer for every document added in RoutedAliasUpdateProcessor.
   */
  static void remoteInvoke(CollectionsHandler collHandler, String aliasName, String targetCol)
      throws Exception {
    final String operation = CollectionParams.CollectionAction.MAINTAINROUTEDALIAS.toLower();
    Map<String, Object> msg = new HashMap<>();
    msg.put(Overseer.QUEUE_OPERATION, operation);
    msg.put(CollectionParams.NAME, aliasName);
    msg.put(MaintainRoutedAliasCmd.ROUTED_ALIAS_TARGET_COL, targetCol);
    final SolrResponse rsp = collHandler.sendToOCPQueue(new ZkNodeProps(msg));
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
  }

  void addCollectionToAlias(String aliasName, ZkStateReader.AliasesManager aliasesManager, String createCollName) {
    aliasesManager.applyModificationAndExportToZk(curAliases -> {
      final List<String> curTargetCollections = curAliases.getCollectionAliasListMap().get(aliasName);
      if (curTargetCollections.contains(createCollName)) {
        return curAliases;
      } else {
        List<String> newTargetCollections = new ArrayList<>(curTargetCollections.size() + 1);
        // prepend it on purpose (thus reverse sorted). Solr alias resolution defaults to the first collection in a list
        newTargetCollections.add(createCollName);
        newTargetCollections.addAll(curTargetCollections);
        return curAliases.cloneWithCollectionAlias(aliasName, StrUtils.join(newTargetCollections, ','));
      }
    });
  }

  private void removeCollectionFromAlias(String aliasName, ZkStateReader.AliasesManager aliasesManager, String createCollName) {
    aliasesManager.applyModificationAndExportToZk(curAliases -> {
      final List<String> curTargetCollections = curAliases.getCollectionAliasListMap().get(aliasName);
      if (curTargetCollections.contains(createCollName)) {
        List<String> newTargetCollections = new ArrayList<>(curTargetCollections.size());
        newTargetCollections.addAll(curTargetCollections);
        newTargetCollections.remove(createCollName);
        return curAliases.cloneWithCollectionAlias(aliasName, StrUtils.join(newTargetCollections, ','));
      } else {
        return curAliases;
      }
    });
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    //---- PARSE PRIMARY MESSAGE PARAMS
    // important that we use NAME for the alias as that is what the Overseer will get a lock on before calling us
    final String aliasName = message.getStr(NAME);
    final String routeValue = message.getStr(ROUTED_ALIAS_TARGET_COL);

    final ZkStateReader.AliasesManager aliasesManager = ocmh.zkStateReader.aliasesManager;
    final Aliases aliases = aliasesManager.getAliases();
    final Map<String, String> aliasMetadata = aliases.getCollectionAliasProperties(aliasName);
    if (aliasMetadata.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Alias " + aliasName + " does not exist or is not a routed alias."); // if it did exist, we'd have a non-null map
    }
    final RoutedAlias ra = RoutedAlias.fromProps(aliasName, aliasMetadata);
    if (ra == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "MaintainRoutedAlias called on non-routed alias");
    }

    ra.updateParsedCollectionAliases(ocmh.zkStateReader, true);
    List<RoutedAlias.Action> actions = ra.calculateActions(routeValue);
    for (RoutedAlias.Action action : actions) {
      boolean exists = ocmh.zkStateReader.getClusterState().getCollectionOrNull(action.targetCollection) != null;
      switch (action.actionType) {
        case ENSURE_REMOVED:
          if (exists) {
            ocmh.tpe.submit(() -> {
              try {
                deleteTargetCollection(clusterState, results, aliasName, aliasesManager, action);
              } catch (Exception e) {
                log.warn("Deletion of {} by {} failed (this might be ok if two clients were " +
                        "writing to a routed alias at the same time and both caused a deletion)",
                    action.targetCollection, ra.getAliasName());
                log.debug("Exception for last message:", e);
              }
            });
          }
          break;
        case ENSURE_EXISTS:
          if (!exists) {
            addTargetCollection(clusterState, results, aliasName, aliasesManager, aliasMetadata, action);
          } else {
            // check that the collection is properly integrated into the alias (see
            // TimeRoutedAliasUpdateProcessorTest.java:141). Presently we need to ensure inclusion in the alias
            // and the presence of the appropriate collection property. Note that this only works if the collection
            // happens to fall where we would have created one already. Support for un-even collection sizes will
            // take additional work (though presently they might work if the below book keeping is done by hand)
            if (!ra.getCollectionList(aliases).contains(action.targetCollection)) {
              addCollectionToAlias(aliasName, aliasesManager, action.targetCollection);
              Map<String, String> collectionProperties = ocmh.zkStateReader
                  .getCollectionProperties(action.targetCollection, 1000);
              if (!collectionProperties.containsKey(RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP)) {
                CollectionProperties props = new CollectionProperties(ocmh.zkStateReader.getZkClient());
                props.setCollectionProperty(action.targetCollection, RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP, aliasName);
              }
            }
          }
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown action type!");
      }
    }
  }

  public void addTargetCollection(ClusterState clusterState, NamedList results, String aliasName, ZkStateReader.AliasesManager aliasesManager, Map<String, String> aliasMetadata, RoutedAlias.Action action) throws Exception {
    NamedList createResults = createCollectionAndWait(clusterState, aliasName, aliasMetadata,
        action.targetCollection, ocmh);
    if (createResults != null) {
      results.add("create", createResults);
    }
    addCollectionToAlias(aliasName, aliasesManager, action.targetCollection);
  }

  public void deleteTargetCollection(ClusterState clusterState, NamedList results, String aliasName, ZkStateReader.AliasesManager aliasesManager, RoutedAlias.Action action) throws Exception {
    Map<String, Object> delProps = new HashMap<>();
    delProps.put(INVOKED_BY_ROUTED_ALIAS,
        (Runnable) () -> removeCollectionFromAlias(aliasName, aliasesManager, action.targetCollection));
    delProps.put(NAME, action.targetCollection);
    ZkNodeProps messageDelete = new ZkNodeProps(delProps);
    new DeleteCollectionCmd(ocmh).call(clusterState, messageDelete, results);
  }
}
