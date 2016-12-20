
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

import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.solr.cloud.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.util.TimeOut;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CreateAliasCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;

  public CreateAliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList results)
      throws Exception {
    String aliasName = message.getStr(NAME);
    String collections = message.getStr("collections");

    ZkStateReader zkStateReader = ocmh.zkStateReader;
    Map<String, String> prevColAliases = zkStateReader.getAliases().getCollectionAliasMap();
    validateAllCollectionsExist(collections, prevColAliases, zkStateReader.getClusterState());

    Map<String, Map<String, String>> newAliasesMap = new HashMap<>();
    Map<String, String> newCollectionAliasesMap = new HashMap<>();
    if (prevColAliases != null) {
      newCollectionAliasesMap.putAll(prevColAliases);
    }
    newCollectionAliasesMap.put(aliasName, collections);
    newAliasesMap.put("collection", newCollectionAliasesMap);
    Aliases newAliases = new Aliases(newAliasesMap);
    byte[] jsonBytes = null;
    if (newAliases.collectionAliasSize() > 0) { // only sub map right now
      jsonBytes = Utils.toJSON(newAliases.getAliasMap());
    }
    try {
      zkStateReader.getZkClient().setData(ZkStateReader.ALIASES, jsonBytes, true);

      checkForAlias(aliasName, collections);
      // some fudge for other nodes
      Thread.sleep(100);
    } catch (KeeperException e) {
      log.error("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } catch (InterruptedException e) {
      log.warn("", e);
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }
  }

  private void validateAllCollectionsExist(String collections, Map<String,String> prevColAliases, ClusterState clusterState) {
    String[] collectionArr = collections.split(",");
    for (String collection:collectionArr) {
      if (clusterState.getCollectionOrNull(collection) == null && (prevColAliases == null || !prevColAliases.containsKey(collection))) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            String.format(Locale.ROOT,  "Can't create collection alias for collections='%s', '%s' is not an existing collection or alias", collections, collection));
      }
    }
  }

  private void checkForAlias(String name, String value) {

    TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS);
    boolean success = false;
    Aliases aliases;
    while (!timeout.hasTimedOut()) {
      aliases = ocmh.zkStateReader.getAliases();
      String collections = aliases.getCollectionAlias(name);
      if (collections != null && collections.equals(value)) {
        success = true;
        break;
      }
    }
    if (!success) {
      log.warn("Timeout waiting to be notified of Alias change...");
    }
  }
}
