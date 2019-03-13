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
import java.util.concurrent.Semaphore;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.api.collections.CategoryRoutedAlias.UNINITIALIZED;
import static org.apache.solr.common.params.CommonParams.NAME;

public class MaintainCategoryRoutedAliasCmd extends AliasCmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @SuppressWarnings("WeakerAccess")
  public static final String IF_CATEGORY_COLLECTION_NOT_FOUND = "ifCategoryCollectionNotFound";

  private static NamedSimpleSemaphore DELETE_LOCK = new NamedSimpleSemaphore();

  private final OverseerCollectionMessageHandler ocmh;

  MaintainCategoryRoutedAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  /**
   * Invokes this command from the client.  If there's a problem it will throw an exception.
   * Please note that is important to never add async to this invocation. This method must
   * block (up to the standard OCP timeout) to prevent large batches of add's from sending a message
   * to the overseer for every document added in RoutedAliasUpdateProcessor.
   */
  @SuppressWarnings("WeakerAccess")
  public static void remoteInvoke(CollectionsHandler collHandler, String aliasName, String categoryCollection)
      throws Exception {
    final String operation = CollectionParams.CollectionAction.MAINTAINCATEGORYROUTEDALIAS.toLower();
    Map<String, Object> msg = new HashMap<>();
    msg.put(Overseer.QUEUE_OPERATION, operation);
    msg.put(CollectionParams.NAME, aliasName);
    msg.put(IF_CATEGORY_COLLECTION_NOT_FOUND, categoryCollection);
    final SolrResponse rsp = collHandler.sendToOCPQueue(new ZkNodeProps(msg));
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    //---- PARSE PRIMARY MESSAGE PARAMS
    // important that we use NAME for the alias as that is what the Overseer will get a lock on before calling us
    final String aliasName = message.getStr(NAME);
    // the client believes this collection name should exist.  Our goal is to ensure it does.
    final String categoryRequired = message.getStr(IF_CATEGORY_COLLECTION_NOT_FOUND); // optional


    //---- PARSE ALIAS INFO FROM ZK
    final ZkStateReader.AliasesManager aliasesManager = ocmh.zkStateReader.aliasesManager;
    final Aliases aliases = aliasesManager.getAliases();
    final Map<String, String> aliasMetadata = aliases.getCollectionAliasProperties(aliasName);
    if (aliasMetadata == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Alias " + aliasName + " does not exist."); // if it did exist, we'd have a non-null map
    }
    final CategoryRoutedAlias categoryRoutedAlias = (CategoryRoutedAlias) RoutedAlias.fromProps(aliasName, aliasMetadata);

    if (categoryRoutedAlias == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, getClass() + " got alias metadata with an " +
          "invalid routing type and produced null");
    }


    //---- SEARCH FOR REQUESTED COLL
    Map<String, List<String>> collectionAliasListMap = aliases.getCollectionAliasListMap();

    // if we found it the collection already exists and we're done (concurrent creation on another request)
    // so this if does not need an else.
    if (!collectionAliasListMap.get(aliasName).contains(categoryRequired)) {
      //---- DETECT and REMOVE the initial place holder collection if it still exists:

      String initialCollection = categoryRoutedAlias.buildCollectionNameFromValue(UNINITIALIZED);

      // important not to delete the place holder collection it until after a second collection exists,
      // otherwise we have a situation where the alias has no collections briefly and concurrent
      // requests to the alias will fail with internal errors (incl. queries etc).

      List<String> colList = new ArrayList<>(collectionAliasListMap.get(aliasName));
      if (colList.contains(initialCollection) && colList.size() > 1 ) {

        // need to run the delete async, otherwise we may deadlock with incoming updates that are attempting
        // to create collections (they will have called getCore() but may be waiting on the overseer alias lock
        // we hold and we will be waiting for the Core reference count to reach zero). By deleting asynchronously
        // we allow this request to complete and the alias lock to be released, which allows the update to complete
        // so that we can do the delete. Additionally we don't want to cause multiple delete operations during
        // the time the delete is in progress, since that just wastes overseer cycles.
        // TODO: check TRA's are protected against this

        if (DELETE_LOCK.tryAcquire(aliasName)) {
          // note that the overseer might not have any cores (and the unit test occasionally catches this)
          ocmh.overseer.getCoreContainer().runAsync(() -> {
            aliasesManager.applyModificationAndExportToZk(curAliases -> {
              colList.remove(initialCollection);
              final String collectionsToKeepStr = StrUtils.join(colList, ',');
              return curAliases.cloneWithCollectionAlias(aliasName, collectionsToKeepStr);
            });
            final CollectionsHandler collHandler = ocmh.overseer.getCoreContainer().getCollectionsHandler();
            final SolrParams reqParams = CollectionAdminRequest
                .deleteCollection(initialCollection).getParams();
            SolrQueryResponse rsp = new SolrQueryResponse();
            try {
              collHandler.handleRequestBody(new LocalSolrQueryRequest(null, reqParams), rsp);
            } catch (Exception e) {
              log.error("Could not delete initial collection from CRA", e);
            }
            //noinspection unchecked
            results.add(UNINITIALIZED, rsp.getValues());
            DELETE_LOCK.release(aliasName);
          });
        }
      }

      //---- CREATE THE COLLECTION
      NamedList createResults = createCollectionAndWait(state, aliasName, aliasMetadata,
          categoryRequired, ocmh);
      if (createResults != null) {
        //noinspection unchecked
        results.add("create", createResults);
      }
      //---- UPDATE THE ALIAS WITH NEW COLLECTION
      updateAlias(aliasName, aliasesManager, categoryRequired);
    }
  }

  private static class NamedSimpleSemaphore {

    private final HashMap<String, Semaphore> semaphores = new HashMap<>();

    NamedSimpleSemaphore() {
    }

    boolean tryAcquire(String name) {
      return semaphores.computeIfAbsent(name, s -> new Semaphore(1)).tryAcquire();
    }

    public void release(String name) {
      semaphores.get(name).release();
    }
  }
}
