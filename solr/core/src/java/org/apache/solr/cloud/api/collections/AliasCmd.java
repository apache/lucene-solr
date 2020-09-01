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

import java.util.Map;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.OverseerSolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionProperties;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;

import static org.apache.solr.cloud.api.collections.RoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.cloud.api.collections.RoutedAlias.ROUTED_ALIAS_NAME_CORE_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CommonParams.NAME;

/**
 * Common superclass for commands that maintain or manipulate aliases. In the routed alias parlance, "maintain"
 * means, given the current state of the alias and some information from a routed field in a document that
 * may imply a need for changes, create, delete or otherwise modify collections as required.
 */
abstract class AliasCmd implements OverseerCollectionMessageHandler.Cmd {

  final OverseerCollectionMessageHandler ocmh;

  AliasCmd(OverseerCollectionMessageHandler ocmh) {
    this.ocmh = ocmh;
  }

  /**
   * Creates a collection (for use in a routed alias), waiting for it to be ready before returning.
   * If the collection already exists then this is not an error.<p>
   */
  @SuppressWarnings({"rawtypes"})
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
          "We require an explicit " + COLL_CONF);
    }
    createReqParams.set(NAME, createCollName);
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
    CollectionProperties collectionProperties = new CollectionProperties(ocmh.zkStateReader.getZkClient());
    collectionProperties.setCollectionProperty(createCollName,ROUTED_ALIAS_NAME_CORE_PROP,aliasName);
    while (!ocmh.zkStateReader.getCollectionProperties(createCollName,1000).containsKey(ROUTED_ALIAS_NAME_CORE_PROP)) {
      Thread.sleep(50);
    }
    return results;
  }


}
