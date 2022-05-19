
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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.SolrException.ErrorCode.SERVER_ERROR;

public class CreateAliasCmd extends AliasCmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());


  private static boolean anyRoutingParams(ZkNodeProps message) {
    return message.keySet().stream().anyMatch(k -> k.startsWith(CollectionAdminParams.ROUTER_PREFIX));
  }

  @SuppressWarnings("WeakerAccess")
  public CreateAliasCmd(OverseerCollectionMessageHandler ocmh) {
    super(ocmh);
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results)
      throws Exception {
    final String aliasName = message.getStr(CommonParams.NAME);
    ZkStateReader zkStateReader = ocmh.zkStateReader;
    // make sure we have the latest version of existing aliases
    if (zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      zkStateReader.aliasesManager.update();
    }

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
    if (canonicalCollectionList.isEmpty()) {
      throw new SolrException(BAD_REQUEST, "'collections' parameter doesn't contain any collection names.");
    }
    final String canonicalCollectionsString = StrUtils.join(canonicalCollectionList, ',');
    validateAllCollectionsExistAndNoDuplicates(canonicalCollectionList, zkStateReader);
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
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private void callCreateRoutedAlias(ZkNodeProps message, String aliasName, ZkStateReader zkStateReader, ClusterState state) throws Exception {
    // Validate we got a basic minimum
    if (!message.getProperties().keySet().containsAll(RoutedAlias.MINIMAL_REQUIRED_PARAMS)) {
      throw new SolrException(BAD_REQUEST, "A routed alias requires these params: " + RoutedAlias.MINIMAL_REQUIRED_PARAMS
      + " plus some create-collection prefixed ones.");
    }

    // convert values to strings
    Map<String, String> props = new LinkedHashMap<>();
    message.getProperties().forEach((key, value) -> props.put(key, String.valueOf(value)));

    // Further validation happens here
    RoutedAlias routedAlias = RoutedAlias.fromProps(aliasName, props);
    if (routedAlias == null) {
      // should never happen here, but keep static analysis in IDE's happy...
      throw new SolrException(SERVER_ERROR,"Tried to create a routed alias with no type!");
    }

    if (!props.keySet().containsAll(routedAlias.getRequiredParams())) {
      throw new SolrException(BAD_REQUEST, "Not all required params were supplied. Missing params: " +
          StrUtils.join(Sets.difference(routedAlias.getRequiredParams(), props.keySet()), ','));
    }

    // Create the first collection.
    Aliases aliases = zkStateReader.aliasesManager.getAliases();

    List<String> collectionList = aliases.resolveAliases(aliasName);
    String collectionListStr = String.join(",", collectionList);
    if (!aliases.isRoutedAlias(aliasName)) {
      // Create the first collection.
      collectionListStr = routedAlias.computeInitialCollectionName();
      ensureAliasCollection(
          aliasName, zkStateReader, state, routedAlias.getAliasMetadata(), collectionListStr);
    }
    // Create/update the alias
    String finalCollectionListStr = collectionListStr;
      // Create/update the alias
      zkStateReader.aliasesManager.applyModificationAndExportToZk(a -> a
          .cloneWithCollectionAlias(aliasName, finalCollectionListStr)
          .cloneWithCollectionAliasProperties(aliasName, routedAlias.getAliasMetadata()));
  }

  private void ensureAliasCollection(String aliasName, ZkStateReader zkStateReader, ClusterState state, Map<String, String> aliasProperties, String initialCollectionName) throws Exception {
    // Create the collection
    createCollectionAndWait(state, aliasName, aliasProperties, initialCollectionName, ocmh);
    validateAllCollectionsExistAndNoDuplicates(Collections.singletonList(initialCollectionName), zkStateReader);
  }

  private void validateAllCollectionsExistAndNoDuplicates(List<String> collectionList, ZkStateReader zkStateReader) {
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
