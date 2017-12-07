
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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;

import static org.apache.solr.common.params.CommonParams.NAME;


public class CreateAliasCmd implements Cmd {
  private final OverseerCollectionMessageHandler ocmh;

  public CreateAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results)
      throws Exception {
    final String aliasName = message.getStr(NAME);
    final List<String> canonicalCollectionList = parseCollectionsParameter(message.get("collections"));
    final String canonicalCollectionsString = StrUtils.join(canonicalCollectionList, ',');

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    validateAllCollectionsExistAndNoDups(canonicalCollectionList, zkStateReader);

    zkStateReader.aliasesHolder.applyModificationAndExportToZk(aliases -> aliases.cloneWithCollectionAlias(aliasName, canonicalCollectionsString));

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

  private void validateAllCollectionsExistAndNoDups(List<String> collectionList, ZkStateReader zkStateReader) {
    final String collectionStr = StrUtils.join(collectionList, ',');

    if (new HashSet<>(collectionList).size() != collectionList.size()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', since it contains duplicates", collectionStr));
    }
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> aliasNames = zkStateReader.getAliases().getCollectionAliasListMap().keySet();
    for (String collection : collectionList) {
      if (clusterState.getCollectionOrNull(collection) == null && !aliasNames.contains(collection)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', '%s' is not an existing collection or alias", collectionStr, collection));
      }
    }
  }
  
  /**
   * The v2 API directs that the 'collections' parameter be provided as a JSON array (e.g. ["a", "b"]).  We also
   * maintain support for the legacy format, a comma-separated list (e.g. a,b).
   */
  @SuppressWarnings("unchecked")
  private List<String> parseCollectionsParameter(Object colls) {
    if (colls == null) throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing collections param");
    if (colls instanceof List) return (List<String>) colls;
    return StrUtils.splitSmart(colls.toString(), ",", true).stream()
        .map(String::trim)
        .collect(Collectors.toList());
  }

}
