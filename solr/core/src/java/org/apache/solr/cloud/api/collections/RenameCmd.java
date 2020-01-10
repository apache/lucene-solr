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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;

/**
 *
 */
public class RenameCmd implements OverseerCollectionMessageHandler.Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final OverseerCollectionMessageHandler ocmh;

  public RenameCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results) throws Exception {
    String extCollectionName = message.getStr(CoreAdminParams.NAME);
    String target = message.getStr(CollectionAdminParams.TARGET);

    if (ocmh.zkStateReader.aliasesManager != null) { // not a mock ZkStateReader
      ocmh.zkStateReader.aliasesManager.update();
    }

    if (extCollectionName == null || target == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "both collection 'name' and 'target' name must be specified");
    }
    Aliases aliases = ocmh.zkStateReader.getAliases();

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = aliases.resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }
    if (!state.hasCollection(collectionName)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "source collection '" + collectionName + "' not found.");
    }
    if (ocmh.zkStateReader.getAliases().hasAlias(target)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "target alias '" + target + "' exists: "
          + ocmh.zkStateReader.getAliases().getCollectionAliasListMap().get(target));
    }

    ocmh.zkStateReader.aliasesManager.applyModificationAndExportToZk(a -> a.cloneWithRename(extCollectionName, target));
  }
}
