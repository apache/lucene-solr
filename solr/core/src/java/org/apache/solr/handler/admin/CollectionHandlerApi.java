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

package org.apache.solr.handler.admin;

import java.util.Arrays;
import java.util.List;

import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation.*;

public class CollectionHandlerApi extends BaseHandlerApiSupport {
  final CollectionsHandler handler;

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
  }

  @Override
  protected List<ApiCommand> getCommands() {
    return Arrays.asList(Cmd.values());
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(CollectionApiMapping.EndPoint.values());
  }


  enum Cmd implements ApiCommand {
    GET_COLLECTIONS(Meta.GET_COLLECTIONS,LIST_OP),
    GET_CLUSTER(Meta.GET_CLUSTER,LIST_OP),
    GET_CLUSTER_OVERSEER(Meta.GET_CLUSTER_OVERSEER,OVERSEERSTATUS_OP),
    GET_CLUSTER_STATUS_CMD(Meta.GET_CLUSTER_STATUS_CMD,REQUESTSTATUS_OP),
    DELETE_CLUSTER_STATUS(Meta.DELETE_CLUSTER_STATUS,DELETESTATUS_OP),
    GET_A_COLLECTION(Meta.GET_A_COLLECTION,CLUSTERSTATUS_OP),
    LIST_ALIASES(Meta.LIST_ALIASES,LISTALIASES_OP),
    CREATE_COLLECTION(Meta.CREATE_COLLECTION, CREATE_OP),
    DELETE_COLL(Meta.DELETE_COLL, DELETE_OP),
    RELOAD_COLL(Meta.RELOAD_COLL, RELOAD_OP),
    MODIFYCOLLECTION(Meta.MODIFYCOLLECTION, MODIFYCOLLECTION_OP),
    MIGRATE_DOCS(Meta.MIGRATE_DOCS,MIGRATE_OP),
    REBALANCELEADERS(Meta.REBALANCELEADERS, REBALANCELEADERS_OP),
    CREATE_ALIAS(Meta.CREATE_ALIAS, CREATEALIAS_OP),
    DELETE_ALIAS(Meta.DELETE_ALIAS, DELETEALIAS_OP),
    CREATE_SHARD(Meta.CREATE_SHARD,CREATESHARD_OP),
    SPLIT_SHARD(Meta.SPLIT_SHARD, SPLITSHARD_OP),
    DELETE_SHARD(Meta.DELETE_SHARD,DELETESHARD_OP),
    CREATE_REPLICA(Meta.CREATE_REPLICA,ADDREPLICA_OP),
    DELETE_REPLICA(Meta.DELETE_REPLICA,DELETEREPLICA_OP),
    SYNC_SHARD(Meta.SYNC_SHARD, SYNCSHARD_OP),
    ADDREPLICAPROP(Meta.ADDREPLICAPROP, ADDREPLICAPROP_OP),
    DELETEREPLICAPROP(Meta.DELETEREPLICAPROP, DELETEREPLICAPROP_OP),
    ADDROLE(Meta.ADDROLE, ADDROLE_OP),
    REMOVEROLE(Meta.REMOVEROLE, REMOVEROLE_OP),
    CLUSTERPROP(Meta.CLUSTERPROP,CLUSTERPROP_OP),
    BACKUP(Meta.BACKUP, BACKUP_OP),
    RESTORE(Meta.RESTORE, RESTORE_OP),
    GET_NODES(Meta.GET_NODES, null) {
      @Override
      public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
        rsp.add("nodes", ((CollectionHandlerApi) apiHandler).handler.coreContainer.getZkController().getClusterState().getLiveNodes());
      }
    },
    FORCELEADER(Meta.FORCELEADER,FORCELEADER_OP),
    SYNCSHARD(Meta.SYNCSHARD,SYNCSHARD_OP),
    BALANCESHARDUNIQUE(Meta.BALANCESHARDUNIQUE,BALANCESHARDUNIQUE_OP)

    ;

    public final CollectionApiMapping.CommandMeta meta;

    public final CollectionOperation target;

    Cmd(CollectionApiMapping.CommandMeta meta, CollectionOperation target) {
      this.meta = meta;
      this.target = target;
    }

    @Override
    public CollectionApiMapping.CommandMeta meta() {
      return meta;
    }

    public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler)
        throws Exception {
      ((CollectionHandlerApi) apiHandler).handler.invokeAction(req, rsp, ((CollectionHandlerApi) apiHandler).handler.coreContainer, target.action, target);
    }
  }

}
