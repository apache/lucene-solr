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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

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
import org.apache.solr.util.DateMathParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MaintainTimeRoutedAliasCmd extends AliasCmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String IF_MOST_RECENT_COLL_NAME = "ifMostRecentCollName"; //TODO rename to createAfter

  private final OverseerCollectionMessageHandler ocmh;

  public MaintainTimeRoutedAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  /**
   * Invokes this command from the client.  If there's a problem it will throw an exception.
   * Please note that is important to never add async to this invocation. This method must
   * block (up to the standard OCP timeout) to prevent large batches of add's from sending a message
   * to the overseer for every document added in RoutedAliasUpdateProcessor.
   */
  public static NamedList remoteInvoke(CollectionsHandler collHandler, String aliasName, String mostRecentCollName)
      throws Exception {
    final String operation = CollectionParams.CollectionAction.MAINTAINTIMEROUTEDALIAS.toLower();
    Map<String, Object> msg = new HashMap<>();
    msg.put(Overseer.QUEUE_OPERATION, operation);
    msg.put(CollectionParams.NAME, aliasName);
    msg.put(MaintainTimeRoutedAliasCmd.IF_MOST_RECENT_COLL_NAME, mostRecentCollName);
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
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "Alias " + aliasName + " does not exist."); // if it did exist, we'd have a non-null map
    }
    final TimeRoutedAlias timeRoutedAlias = new TimeRoutedAlias(aliasName, aliasMetadata);

    final List<Map.Entry<Instant, String>> parsedCollections =
        timeRoutedAlias.parseCollections(aliases);

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
    updateAlias(aliasName, aliasesManager, createCollName);

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
          timeRoutedAlias.parseCollections(curAliases);

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

}
