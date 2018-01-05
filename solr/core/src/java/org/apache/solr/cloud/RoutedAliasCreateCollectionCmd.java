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

package org.apache.solr.cloud;

import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor;
import org.apache.solr.util.TimeZoneUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.cloud.OverseerCollectionMessageHandler.COLL_CONF;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.ROUTER_FIELD_METADATA;
import static org.apache.solr.update.processor.TimeRoutedAliasUpdateProcessor.ROUTER_INTERVAL_METADATA;

/**
 * For "routed aliases", creates another collection and adds it to the alias. In some cases it will not
 * add a new collection.
 * If a collection is created, then collection creation info is returned.
 *
 * Note: this logic is within an Overseer because we want to leverage the mutual exclusion
 * property afforded by the lock it obtains on the alias name.
 * @since 7.3
 */
public class RoutedAliasCreateCollectionCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String IF_MOST_RECENT_COLL_NAME = "ifMostRecentCollName";

  public static final String COLL_METAPREFIX = "collection-create.";

  private final OverseerCollectionMessageHandler ocmh;

  public RoutedAliasCreateCollectionCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  /* TODO:
  There are a few classes related to time routed alias processing.  We need to share some logic better.
   */


  @Override
  public void call(ClusterState clusterState, ZkNodeProps message, NamedList results) throws Exception {
    //---- PARSE PRIMARY MESSAGE PARAMS
    // important that we use NAME for the alias as that is what the Overseer will get a lock on before calling us
    final String aliasName = message.getStr(NAME);
    // the client believes this is the mostRecent collection name.  We assert this if provided.
    final String ifMostRecentCollName = message.getStr(IF_MOST_RECENT_COLL_NAME); // optional

    // TODO collection param (or intervalDateMath override?), useful for data capped collections

    //---- PARSE ALIAS INFO FROM ZK
    final ZkStateReader.AliasesManager aliasesHolder = ocmh.zkStateReader.aliasesHolder;
    final Aliases aliases = aliasesHolder.getAliases();
    final Map<String, String> aliasMetadata = aliases.getCollectionAliasMetadata(aliasName);
    if (aliasMetadata == null) {
      throw newAliasMustExistException(aliasName); // if it did exist, we'd have a non-null map
    }

    String routeField = aliasMetadata.get(ROUTER_FIELD_METADATA);
    if (routeField == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          "This command only works on time routed aliases.  Expected alias metadata not found.");
    }
    String intervalDateMath = aliasMetadata.getOrDefault(ROUTER_INTERVAL_METADATA, "+1DAY");
    TimeZone intervalTimeZone = TimeZoneUtils.parseTimezone(aliasMetadata.get(CommonParams.TZ));

    //TODO this is ugly; how can we organize the code related to this feature better?
    final List<Map.Entry<Instant, String>> parsedCollections =
        TimeRoutedAliasUpdateProcessor.parseCollections(aliasName, aliases, () -> newAliasMustExistException(aliasName));

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
    final Instant nextCollTimestamp = TimeRoutedAliasUpdateProcessor.computeNextCollTimestamp(mostRecentCollTimestamp, intervalDateMath, intervalTimeZone);
    assert nextCollTimestamp.isAfter(mostRecentCollTimestamp);
    final String createCollName = TimeRoutedAliasUpdateProcessor.formatCollectionNameFromInstant(aliasName, nextCollTimestamp);

    //---- CREATE THE COLLECTION
    // Map alias metadata starting with a prefix to a create-collection API request
    final ModifiableSolrParams createReqParams = new ModifiableSolrParams();
    for (Map.Entry<String, String> e : aliasMetadata.entrySet()) {
      if (e.getKey().startsWith(COLL_METAPREFIX)) {
        createReqParams.set(e.getKey().substring(COLL_METAPREFIX.length()), e.getValue());
      }
    }
    if (createReqParams.get(COLL_CONF) == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "We require an explicit " + COLL_CONF );
    }
    createReqParams.set(NAME, createCollName);
    createReqParams.set("property." + TimeRoutedAliasUpdateProcessor.TIME_PARTITION_ALIAS_NAME_CORE_PROP, aliasName);
    // a CollectionOperation reads params and produces a message (Map) that is supposed to be sent to the Overseer.
    //   Although we could create the Map without it, there are a fair amount of rules we don't want to reproduce.
    final Map<String, Object> createMsgMap = CollectionsHandler.CollectionOperation.CREATE_OP.execute(
        new LocalSolrQueryRequest(null, createReqParams),
        null,
        ocmh.overseer.getCoreContainer().getCollectionsHandler());
    createMsgMap.put(Overseer.QUEUE_OPERATION, "create");
    // Since we are running in the Overseer here, send the message directly to the Overseer CreateCollectionCmd
    ocmh.commandMap.get(CollectionParams.CollectionAction.CREATE).call(clusterState, new ZkNodeProps(createMsgMap), results);

    CollectionsHandler.waitForActiveCollection(createCollName, null, ocmh.overseer.getCoreContainer(), new OverseerSolrResponse(results));

    //TODO delete some of the oldest collection(s) ?

    //---- UPDATE THE ALIAS
    aliasesHolder.applyModificationAndExportToZk(curAliases -> {
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

  private SolrException newAliasMustExistException(String aliasName) {
    return new SolrException(SolrException.ErrorCode.BAD_REQUEST,
        "Alias " + aliasName + " does not exist.");
  }

}
