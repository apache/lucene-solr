
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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.util.DateMathParser;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

public class CreateAliasCmd implements OverseerCollectionMessageHandler.Cmd {

  private final OverseerCollectionMessageHandler ocmh;

  private static boolean anyRoutingParams(ZkNodeProps message) {
    return message.keySet().stream().anyMatch(k -> k.startsWith(TimeRoutedAlias.ROUTER_PREFIX));
  }

  public CreateAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results)
      throws Exception {
    final String aliasName = message.getStr(CommonParams.NAME);
    ZkStateReader zkStateReader = ocmh.zkStateReader;

    if (!anyRoutingParams(message)) {
      callCreatePlainAlias(message, aliasName, zkStateReader);
    } else {
      callCreateRoutedAlias(message, aliasName, zkStateReader, state);
    }

    // Sleep a bit to allow ZooKeeper state propagation.
    //
    // THIS IS A KLUDGE.
    //
    // Solr's view of the cluster is eventually consistent. *Eventually* all nodes and CloudSolrClients will be aware of
    // alias changes, but not immediately. If a newly created alias is queried, things should work right away since Solr
    // will attempt to see if it needs to get the latest aliases when it can't otherwise resolve the name.  However
    // modifications to an alias will take some time.
    //
    // We could levy this requirement on the client but they would probably always add an obligatory sleep, which is
    // just kicking the can down the road.  Perhaps ideally at this juncture here we could somehow wait until all
    // Solr nodes in the cluster have the latest aliases?
    Thread.sleep(100);
  }

  private void callCreatePlainAlias(ZkNodeProps message, String aliasName, ZkStateReader zkStateReader) {
    final List<String> canonicalCollectionList = parseCollectionsParameter(message.get("collections"));
    final String canonicalCollectionsString = StrUtils.join(canonicalCollectionList, ',');
    validateAllCollectionsExistAndNoDups(canonicalCollectionList, zkStateReader);
    zkStateReader.aliasesManager
        .applyModificationAndExportToZk(aliases -> aliases.cloneWithCollectionAlias(aliasName, canonicalCollectionsString));
  }

  /**
   * The v2 API directs that the 'collections' parameter be provided as a JSON array (e.g. ["a", "b"]).  We also
   * maintain support for the legacy format, a comma-separated list (e.g. a,b).
   */
  @SuppressWarnings("unchecked")
  private List<String> parseCollectionsParameter(Object colls) {
    if (colls == null) throw new SolrException(BAD_REQUEST, "missing collections param");
    if (colls instanceof List) return (List<String>) colls;
    return StrUtils.splitSmart(colls.toString(), ",", true).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

  private void callCreateRoutedAlias(ZkNodeProps message, String aliasName, ZkStateReader zkStateReader, ClusterState state) throws Exception {
    // Validate we got everything we need
    if (!message.getProperties().keySet().containsAll(TimeRoutedAlias.REQUIRED_ROUTER_PARAMS)) {
      throw new SolrException(BAD_REQUEST, "A routed alias requires these params: " + TimeRoutedAlias.REQUIRED_ROUTER_PARAMS
      + " plus some create-collection prefixed ones.");
    }

    Map<String, String> aliasProperties = new LinkedHashMap<>();
    message.getProperties().entrySet().stream()
        .filter(entry -> TimeRoutedAlias.PARAM_IS_PROP.test(entry.getKey()))
        .forEach(entry -> aliasProperties.put(entry.getKey(), (String) entry.getValue())); // way easier than .collect

    TimeRoutedAlias timeRoutedAlias = new TimeRoutedAlias(aliasName, aliasProperties); // validates as well

    String start = message.getStr(TimeRoutedAlias.ROUTER_START);
    Instant startTime = parseStart(start, timeRoutedAlias.getTimeZone());

    String initialCollectionName = TimeRoutedAlias.formatCollectionNameFromInstant(aliasName, startTime);

    // Create the collection
    MaintainRoutedAliasCmd.createCollectionAndWait(state, aliasName, aliasProperties, initialCollectionName, ocmh);
    validateAllCollectionsExistAndNoDups(Collections.singletonList(initialCollectionName), zkStateReader);

    // Create/update the alias
    zkStateReader.aliasesManager.applyModificationAndExportToZk(aliases -> aliases
        .cloneWithCollectionAlias(aliasName, initialCollectionName)
        .cloneWithCollectionAliasProperties(aliasName, aliasProperties));
  }

  private Instant parseStart(String str, TimeZone zone) {
    Instant start = DateMathParser.parseMath(new Date(), str, zone).toInstant();
    checkMilis(start);
    return start;
  }

  private void checkMilis(Instant date) {
    if (!date.truncatedTo(ChronoUnit.SECONDS).equals(date)) {
      throw new SolrException(BAD_REQUEST,
          "Date or date math for start time includes milliseconds, which is not supported. " +
              "(Hint: 'NOW' used without rounding always has this problem)");
    }
  }

  private void validateAllCollectionsExistAndNoDups(List<String> collectionList, ZkStateReader zkStateReader) {
    final String collectionStr = StrUtils.join(collectionList, ',');

    if (new HashSet<>(collectionList).size() != collectionList.size()) {
      throw new SolrException(BAD_REQUEST,
          String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', since it contains duplicates", collectionStr));
    }
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> aliasNames = zkStateReader.getAliases().getCollectionAliasListMap().keySet();
    for (String collection : collectionList) {
      if (clusterState.getCollectionOrNull(collection) == null && !aliasNames.contains(collection)) {
        throw new SolrException(BAD_REQUEST,
            String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', '%s' is not an existing collection or alias", collectionStr, collection));
      }
    }
  }

}
