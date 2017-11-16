
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
    String aliasName = message.getStr(NAME);
    String collections = message.getStr("collections"); // could be comma delimited list

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    validateAllCollectionsExistAndNoDups(collections, zkStateReader);

    zkStateReader.aliasesHolder.applyModificationAndExportToZk(aliases -> aliases.cloneWithCollectionAlias(aliasName, collections));
  }

  private void validateAllCollectionsExistAndNoDups(String collections, ZkStateReader zkStateReader) {
    List<String> collectionArr = StrUtils.splitSmart(collections, ",", true);
    if (new HashSet<>(collectionArr).size() != collectionArr.size()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
          String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', since it contains duplicates", collections));
    }
    ClusterState clusterState = zkStateReader.getClusterState();
    Set<String> aliasNames = zkStateReader.getAliases().getCollectionAliasListMap().keySet();
    for (String collection : collectionArr) {
      if (clusterState.getCollectionOrNull(collection) == null && !aliasNames.contains(collection)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', '%s' is not an existing collection or alias", collections, collection));
      }
    }
  }

}
