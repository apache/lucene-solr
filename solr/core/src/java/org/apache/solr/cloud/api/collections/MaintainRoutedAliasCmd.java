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
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.cloud.api.collections.TimeRoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * (Internal) For "time routed aliases", both deletes old collections and creates new collections
 * associated with routed aliases.
 *
 * Note: this logic is within an Overseer because we want to leverage the mutual exclusion
 * property afforded by the lock it obtains on the alias name.
 *
 * @since 7.3
 * @lucene.internal
 */
public class MaintainRoutedAliasCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String IF_MOST_RECENT_COLL_NAME = "ifMostRecentCollName"; //TODO rename to createAfter

  private final OverseerCollectionMessageHandler ocmh;

  public MaintainRoutedAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  /**
   * Invokes this command from the client.  If there's a problem it will throw an exception.
   * Please note that is important to never add async to this invocation. This method must
   * block (up to the standard OCP timeout) to prevent large batches of add's from sending a message
   * to the overseer for every document added in TimeRoutedAliasUpdateProcessor.
   */
  public static NamedList remoteInvoke(CollectionsHandler collHandler, String aliasName, String mostRecentCollName)
      throws Exception {
    final String operation = CollectionParams.CollectionAction.MAINTAINROUTEDALIAS.toLower();
    Map<String, Object> msg = new HashMap<>();
    msg.put(Overseer.QUEUE_OPERATION, operation);
    msg.put(CollectionParams.NAME, aliasName);
    msg.put(MaintainRoutedAliasCmd.IF_MOST_RECENT_COLL_NAME, mostRecentCollName);
    final SolrResponse rsp = collHandler.sendToOCPQueue(new ZkNodeProps(msg));
    if (rsp.getException() != null) {
      throw rsp.getException();
    }
    return rsp.getResponse();
  }

  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    //---- PARSE PRIMARY MESSAGE PARAMS
    // important that we use NAME for the alias as that is what the Overseer will get a lock on before calling us
    final String aliasName = message.getStr(NAME);
    // the client believes this is the mostRecent collection name.  We assert this if provided.
    final String ifMostRecentCollName = message.getStr(IF_MOST_RECENT_COLL_NAME); // optional

    // TODO collection param (or intervalDateMath override?), useful for data capped collections

    //---- PARSE ALIAS INFO FROM ZK
    final ZkStateReader.AliasesManager aliasesManager = ocmh.zkStateReader.aliasesManager;
    final Aliases aliases = aliasesManager.getAliases();
    final Map<String, String> aliasMetadata = aliases.getCollectionAliasProperties(aliasName);
    if (aliasMetadata == null) {
      throw newAliasMustExistException(aliasName); // if it did exist, we'd have a non-null map
    }
    final TimeRoutedAlias timeRoutedAlias = new TimeRoutedAlias(aliasName, aliasMetadata);

    final List<Map.Entry<Instant, String>> parsedCollections =
        timeRoutedAlias.parseCollections(aliases, () -> newAliasMustExistException(aliasName));

    //---- GET MOST RECENT COLL
    final Map.Entry<Instant, String> mostRecentEntry = parsedCollections.get(0);
    final Instant mostRecentCollTimestamp = mostRecentEntry.getKey();
    final String mostRecentCollName = mostRecentEntry.getValue();
    if (ifMostRecentCollName != null) {
      if (!mostRecentCollName.equals(ifMostRecentCollName)) {
        // Possibly due to race conditions in URPs on multiple leaders calling us at the same time
        String msg = IF_MOST_RECENT_COLL_NAME + " expected " + ifMostRecentCollName + " but it's " + mostRecentCollName;
        if (parsedCollections.stream().map(Map.Entry::getValue).noneMatch(ifMostRecentCollName::equals)) {
          msg += ". Furthermore this collection isn't in the list of collections referenced by the alias.";
        }
        log.info(msg);
        results.add("message", msg);
        return;
      }
    } else if (mostRecentCollTimestamp.isAfter(Instant.now())) {
      final String msg = "Most recent collection is in the future, so we won't create another.";
      log.info(msg);
      results.add("message", msg);
      return;
    }

    //---- COMPUTE NEXT COLLECTION NAME
    final Instant nextCollTimestamp = timeRoutedAlias.computeNextCollTimestamp(mostRecentCollTimestamp);
    final String createCollName = TimeRoutedAlias.formatCollectionNameFromInstant(aliasName, nextCollTimestamp);

    //---- DELETE OLDEST COLLECTIONS AND REMOVE FROM ALIAS (if configured)
    NamedList deleteResults = deleteOldestCollectionsAndUpdateAlias(timeRoutedAlias, aliasesManager, nextCollTimestamp);
    if (deleteResults != null) {
      results.add("delete", deleteResults);
    }

    //---- CREATE THE COLLECTION
    NamedList createResults = createCollectionAndWait(clusterState, aliasName, aliasMetadata,
        createCollName, ocmh);
    if (createResults != null) {
      results.add("create", createResults);
    }

    //---- UPDATE THE ALIAS WITH NEW COLLECTION
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

  /**
   * Deletes some of the oldest collection(s) based on {@link TimeRoutedAlias#getAutoDeleteAgeMath()}. If not present
   * then does nothing.  Returns non-null results if something was deleted (or if we tried to).
   * {@code now} is the date from which the math is relative to.
   */
  NamedList deleteOldestCollectionsAndUpdateAlias(TimeRoutedAlias timeRoutedAlias,
                                                  ZkStateReader.AliasesManager aliasesManager,
                                                  Instant now) throws Exception {
    final String autoDeleteAgeMathStr = timeRoutedAlias.getAutoDeleteAgeMath();
    if (autoDeleteAgeMathStr == null) {
      return null;
    }
    final Instant delBefore;
    try {
      delBefore = new DateMathParser(Date.from(now), timeRoutedAlias.getTimeZone()).parseMath(autoDeleteAgeMathStr).toInstant();
    } catch (ParseException e) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, e); // note: should not happen by this point
    }

    String aliasName = timeRoutedAlias.getAliasName();

    Collection<String> collectionsToDelete = new LinkedHashSet<>();

    // First update the alias    (there may be no change to make!)
    aliasesManager.applyModificationAndExportToZk(curAliases -> {
      // note: we could re-parse the TimeRoutedAlias object from curAliases but I don't think there's a point to it.

      final List<Map.Entry<Instant, String>> parsedCollections =
          timeRoutedAlias.parseCollections(curAliases, () -> newAliasMustExistException(aliasName));

      //iterating from newest to oldest, find the first collection that has a time <= "before".  We keep this collection
      // (and all newer to left) but we delete older collections, which are the ones that follow.
      // This logic will always keep the first collection, which we can't delete.
      int numToKeep = 0;
      for (Map.Entry<Instant, String> parsedCollection : parsedCollections) {
        numToKeep++;
        final Instant colInstant = parsedCollection.getKey();
        if (colInstant.isBefore(delBefore) || colInstant.equals(delBefore)) {
          break;
        }
      }
      if (numToKeep == parsedCollections.size()) {
        log.debug("No old time routed collections to delete.");
        return curAliases;
      }

      final List<String> targetList = curAliases.getCollectionAliasListMap().get(aliasName);
      // remember to delete these... (oldest to newest)
      for (int i = targetList.size() - 1; i >= numToKeep; i--) {
        collectionsToDelete.add(targetList.get(i));
      }
      // new alias list has only "numToKeep" first items
      final List<String> collectionsToKeep = targetList.subList(0, numToKeep);
      final String collectionsToKeepStr = StrUtils.join(collectionsToKeep, ',');
      return curAliases.cloneWithCollectionAlias(aliasName, collectionsToKeepStr);
    });

    if (collectionsToDelete.isEmpty()) {
      return null;
    }

    log.info("Removing old time routed collections: {}", collectionsToDelete);
    // Should this be done asynchronously?  If we got "ASYNC" then probably.
    //   It would shorten the time the Overseer holds a lock on the alias name
    //   (deleting the collections will be done later and not use that lock).
    //   Don't bother about parallel; it's unusual to have more than 1.
    // Note we don't throw an exception here under most cases; instead the response will have information about
    //   how each delete request went, possibly including a failure message.
    final CollectionsHandler collHandler = ocmh.overseer.getCoreContainer().getCollectionsHandler();
    NamedList results = new NamedList();
    for (String collection : collectionsToDelete) {
      final SolrParams reqParams = CollectionAdminRequest.deleteCollection(collection).getParams();
      SolrQueryResponse rsp = new SolrQueryResponse();
      collHandler.handleRequestBody(new LocalSolrQueryRequest(null, reqParams), rsp);
      results.add(collection, rsp.getValues());
    }
    return results;
  }

  /**
   * Creates a collection (for use in a routed alias), waiting for it to be ready before returning.
   * If the collection already exists then this is not an error.
   * IMPORTANT: Only call this from an {@link OverseerCollectionMessageHandler.Cmd}.
   */
  static NamedList createCollectionAndWait(ClusterState clusterState, String aliasName, Map<String, String> aliasMetadata,
                                           String createCollName, OverseerCollectionMessageHandler ocmh) throws Exception {
    // Map alias metadata starting with a prefix to a create-collection API request
    final ModifiableSolrParams createReqParams = new ModifiableSolrParams();
    for (Map.Entry<String, String> e : aliasMetadata.entrySet()) {
      if (e.getKey().startsWith(CREATE_COLLECTION_PREFIX)) {
        createReqParams.set(e.getKey().substring(CREATE_COLLECTION_PREFIX.length()), e.getValue());
      }
    }
    if (createReqParams.get(COLL_CONF) == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "We require an explicit " + COLL_CONF );
    }
    createReqParams.set(NAME, createCollName);
    createReqParams.set("property." + ROUTED_ALIAS_NAME_CORE_PROP, aliasName);
    // a CollectionOperation reads params and produces a message (Map) that is supposed to be sent to the Overseer.
    //   Although we could create the Map without it, there are a fair amount of rules we don't want to reproduce.
    final Map<String, Object> createMsgMap = CollectionsHandler.CollectionOperation.CREATE_OP.execute(
        new LocalSolrQueryRequest(null, createReqParams),
        null,
        ocmh.overseer.getCoreContainer().getCollectionsHandler());
    createMsgMap.put(Overseer.QUEUE_OPERATION, "create");

    NamedList results = new NamedList();
    try {
      // Since we are running in the Overseer here, send the message directly to the Overseer CreateCollectionCmd.
      // note: there's doesn't seem to be any point in locking on the collection name, so we don't. We currently should
      //   already have a lock on the alias name which should be sufficient.
      ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, new ZkNodeProps(createMsgMap), results);
    } catch (SolrException e) {
      // The collection might already exist, and that's okay -- we can adopt it.
      if (!e.getMessage().contains("collection already exists")) {
        throw e;
      }
    }

    CollectionsHandler.waitForActiveCollection(createCollName, ocmh.overseer.getCoreContainer(),
        new OverseerSolrResponse(results));
    return results;
  }

  private SolrException newAliasMustExistException(String aliasName) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Alias " + aliasName + " does not exist.");
  }

}
