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

package org.apache.solr.client.solrj.request;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ConfigSetParams.ConfigSetAction;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.Utils;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.ConfigSetEndPoint.CONFIG_COMMANDS;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.ConfigSetEndPoint.CONFIG_DEL;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.ConfigSetEndPoint.LIST_CONFIG;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER_ALIASES;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER_CMD;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER_CMD_STATUS;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER_CMD_STATUS_DELETE;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.CLUSTER_NODES;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.COLLECTIONS;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.COLLECTIONS_COMMANDS;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.COLLECTION_STATE;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION_PER_SHARD_COMMANDS;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION_PER_SHARD_DELETE;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE;
import static org.apache.solr.client.solrj.request.CollectionApiMapping.EndPoint.PER_COLLECTION_SHARDS_COMMANDS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.*;
import static org.apache.solr.common.params.CommonParams.NAME;

/** stores the mapping of v1 API parameters to v2 API parameters
 * for collection API and configset API
 *
 */
public class CollectionApiMapping {

  public enum Meta implements CommandMeta {
    GET_COLLECTIONS(COLLECTIONS, GET, LIST),
    GET_CLUSTER(CLUSTER, GET, LIST, "/cluster", null),
    GET_CLUSTER_OVERSEER(CLUSTER, GET, OVERSEERSTATUS, "/cluster/overseer", null),
    GET_CLUSTER_STATUS_CMD(CLUSTER_CMD_STATUS, GET, REQUESTSTATUS),
    DELETE_CLUSTER_STATUS(CLUSTER_CMD_STATUS_DELETE, DELETE, DELETESTATUS),
    GET_A_COLLECTION(COLLECTION_STATE, GET, CLUSTERSTATUS),
    LIST_ALIASES(CLUSTER_ALIASES, GET, LISTALIASES),
    CREATE_COLLECTION(COLLECTIONS_COMMANDS,
        POST,
        CREATE,
        CREATE.toLower(),
        Utils.makeMap(
            "collection.configName", "config",
            "createNodeSet.shuffle", "shuffleNodes",
            "createNodeSet", "nodeSet"
        ),
        Utils.makeMap("properties.", "property.")),

    DELETE_COLL(EndPoint.PER_COLLECTION_DELETE,
        DELETE,
        CollectionAction.DELETE,
        CollectionAction.DELETE.toLower(),
        Utils.makeMap(NAME, "collection")),

    RELOAD_COLL(PER_COLLECTION,
        POST,
        RELOAD,
        RELOAD.toLower(),
        Utils.makeMap(NAME, "collection")),
    MODIFY_COLLECTION(PER_COLLECTION,
        POST,
        MODIFYCOLLECTION,
        "modify",null),
    MIGRATE_DOCS(PER_COLLECTION,
        POST,
        MIGRATE,
        "migrate-docs",
        Utils.makeMap("split.key", "splitKey",
            "target.collection", "target",
            "forward.timeout", "forwardTimeout"
        )),
    REBALANCE_LEADERS(PER_COLLECTION,
        POST,
        REBALANCELEADERS,
        "rebalance-leaders", null),
    CREATE_ALIAS(COLLECTIONS_COMMANDS,
        POST,
        CREATEALIAS,
        "create-alias",
        null),

    DELETE_ALIAS(COLLECTIONS_COMMANDS,
        POST,
        DELETEALIAS,
        "delete-alias",
        null),
    CREATE_SHARD(PER_COLLECTION_SHARDS_COMMANDS,
        POST,
        CREATESHARD,
        "create",
        Utils.makeMap("createNodeSet", "nodeSet"),
        Utils.makeMap("coreProperties.", "property.")) {
      @Override
      public String getParamSubstitute(String param) {
        return super.getParamSubstitute(param);
      }
    },

    SPLIT_SHARD(PER_COLLECTION_SHARDS_COMMANDS,
        POST,
        SPLITSHARD,
        "split",
        Utils.makeMap(
            "split.key", "splitKey"),
        Utils.makeMap("coreProperties.", "property.")),
    DELETE_SHARD(PER_COLLECTION_PER_SHARD_DELETE,
        DELETE, DELETESHARD),

    CREATE_REPLICA(PER_COLLECTION_SHARDS_COMMANDS,
        POST,
        ADDREPLICA,
        "add-replica",
        null,
        Utils.makeMap("coreProperties.", "property.")),

    DELETE_REPLICA(PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE,
        DELETE, DELETEREPLICA),

    SYNC_SHARD(PER_COLLECTION_PER_SHARD_COMMANDS,
        POST,
        CollectionAction.SYNCSHARD,
        "synch-shard",
        null),
    ADD_REPLICA_PROPERTY(PER_COLLECTION,
        POST,
        CollectionAction.ADDREPLICAPROP,
        "add-replica-property",
        Utils.makeMap("property", "name", "property.value", "value")),
    DELETE_REPLICA_PROPERTY(PER_COLLECTION,
        POST,
        DELETEREPLICAPROP,
        "delete-replica-property",
        null),
    ADD_ROLE(CLUSTER_CMD,
        POST,
        ADDROLE,
        "add-role",null),
    REMOVE_ROLE(CLUSTER_CMD,
        POST,
        REMOVEROLE,
        "remove-role",null),

    SET_CLUSTER_PROPERTY(CLUSTER_CMD,
        POST,
        CLUSTERPROP,
        "set-property",null),

    BACKUP_COLLECTION(COLLECTIONS_COMMANDS,
        POST,
        BACKUP,
        "backup-collection", null
    ),
    RESTORE_COLLECTION(COLLECTIONS_COMMANDS,
        POST,
        RESTORE,
        "restore-collection",
        null
    ),
    GET_NODES(CLUSTER_NODES, GET, null),
    FORCE_LEADER(PER_COLLECTION_PER_SHARD_COMMANDS, POST, CollectionAction.FORCELEADER, "force-leader", null),
    BALANCE_SHARD_UNIQUE(PER_COLLECTION, POST, BALANCESHARDUNIQUE,"balance-shard-unique" , null)
    ;

    public final String commandName;
    public final EndPoint endPoint;
    public final SolrRequest.METHOD method;
    //mapping of http param name to json attribute
    public final Map<String, String> paramstoAttr;
    //mapping of old prefix to new for instance properties.a=val can be substituted with property:{a:val}
    public final Map<String, String> prefixSubstitutes;
    public final CollectionAction action;

    public SolrRequest.METHOD getMethod() {
      return method;
    }


    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action) {
      this(endPoint, method, action, null, null);
    }

    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action,
         String commandName, Map paramstoAttr) {
      this(endPoint, method, action, commandName, paramstoAttr, Collections.EMPTY_MAP);

    }

    Meta(EndPoint endPoint, SolrRequest.METHOD method, CollectionAction action,
         String commandName, Map paramstoAttr, Map prefixSubstitutes) {
      this.action = action;
      this.commandName = commandName;
      this.endPoint = endPoint;
      this.method = method;
      this.paramstoAttr = paramstoAttr == null ? Collections.EMPTY_MAP : Collections.unmodifiableMap(paramstoAttr);
      this.prefixSubstitutes = Collections.unmodifiableMap(prefixSubstitutes);

    }

    @Override
    public String getName() {
      return commandName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }


    @Override
    public Collection<String> getParamNames(CommandOperation op) {
      Collection<String> paramNames = getParamNames_(op, this);
      if (!prefixSubstitutes.isEmpty()) {
        Collection<String> result = new ArrayList<>(paramNames.size());
        for (Map.Entry<String, String> e : prefixSubstitutes.entrySet()) {
          for (String paramName : paramNames) {
            if (paramName.startsWith(e.getKey())) {
              result.add(paramName.replace(e.getKey(), e.getValue()));
            } else {
              result.add(paramName);
            }
          }
          paramNames = result;
        }
      }

      return paramNames;
    }

    @Override
    public String getParamSubstitute(String param) {
      String s = paramstoAttr.containsKey(param) ? paramstoAttr.get(param) : param;
      if (prefixSubstitutes != null) {
        for (Map.Entry<String, String> e : prefixSubstitutes.entrySet()) {
          if (s.startsWith(e.getValue())) return s.replace(e.getValue(), e.getKey());
        }
      }
      return s;
    }
    public Object getReverseParamSubstitute(String param) {
      String s = paramstoAttr.containsKey(param) ? paramstoAttr.get(param) : param;

      if (prefixSubstitutes != null) {
        for (Map.Entry<String, String> e : prefixSubstitutes.entrySet()) {
          if(param.startsWith(e.getValue())){
            return new Pair<>(e.getKey().substring(0, e.getKey().length() - 1), param.substring(e.getValue().length()));
          }
        }
      }
      return s;

    }

  }

  public enum EndPoint implements V2EndPoint {
    CLUSTER("cluster"),
    CLUSTER_ALIASES("cluster.aliases"),
    CLUSTER_CMD("cluster.Commands"),
    CLUSTER_NODES("cluster.nodes"),
    CLUSTER_CMD_STATUS("cluster.commandstatus"),
    CLUSTER_CMD_STATUS_DELETE("cluster.commandstatus.delete"),
    COLLECTIONS_COMMANDS("collections.Commands"),
    COLLECTIONS("collections"),
    COLLECTION_STATE("collections.collection"),
    PER_COLLECTION("collections.collection.Commands"),
    PER_COLLECTION_DELETE("collections.collection.delete"),
    PER_COLLECTION_SHARDS_COMMANDS("collections.collection.shards.Commands"),
    PER_COLLECTION_PER_SHARD_COMMANDS("collections.collection.shards.shard.Commands"),
    PER_COLLECTION_PER_SHARD_DELETE("collections.collection.shards.shard.delete"),
    PER_COLLECTION_PER_SHARD_PER_REPLICA_DELETE("collections.collection.shards.shard.replica.delete");
    final String specName;


    EndPoint(String specName) {
      this.specName = specName;
    }

    @Override
    public String getSpecName() {
      return specName;
    }
  }

  public interface V2EndPoint {

    String getSpecName();
  }

  public enum ConfigSetMeta implements CommandMeta {
    LIST(LIST_CONFIG, GET,null, ConfigSetAction.LIST),
    CREATE(CONFIG_COMMANDS, POST, "create", ConfigSetAction.CREATE),
    DEL(CONFIG_DEL,  DELETE, null, ConfigSetAction.DELETE)
    ;
    public final ConfigSetEndPoint endPoint;
    public final SolrRequest.METHOD method;
    public final String cmdName;
    public final ConfigSetAction action;


    ConfigSetMeta(ConfigSetEndPoint endPoint, SolrRequest.METHOD method, String cmdName, ConfigSetAction action) {
      this.cmdName = cmdName;
      this.endPoint = endPoint;
      this.method = method;
      this.action = action;
    }

    @Override
    public String getName() {
      return cmdName;
    }

    @Override
    public SolrRequest.METHOD getHttpMethod() {
      return method;
    }

    @Override
    public V2EndPoint getEndPoint() {
      return endPoint;
    }


  }
  public enum ConfigSetEndPoint implements V2EndPoint {
    LIST_CONFIG("cluster.configs"),
    CONFIG_COMMANDS("cluster.configs.Commands"),
    CONFIG_DEL("cluster.configs.delete");

    public final String spec;

    ConfigSetEndPoint(String spec) {
      this.spec = spec;
    }

    @Override
    public String getSpecName() {
      return spec;
    }
  }



  private static Collection<String> getParamNames_(CommandOperation op, CommandMeta command) {
    List<String> result = new ArrayList<>();
    Object o = op.getCommandData();
    if (o instanceof Map) {
      Map map = (Map) o;
      collectKeyNames(map, result, "");
    }
    return result;

  }

  public static void collectKeyNames(Map<String, Object> map, List<String> result, String prefix) {
    for (Map.Entry<String, Object> e : map.entrySet()) {
      if (e.getValue() instanceof Map) {
        collectKeyNames((Map) e.getValue(), result, prefix + e.getKey() + ".");
      } else {
        result.add(prefix + e.getKey());
      }
    }
  }
  public interface CommandMeta {
    String getName();

    /**
     * the http method supported by this command
     */
    SolrRequest.METHOD getHttpMethod();

    V2EndPoint getEndPoint();

    default Collection<String> getParamNames(CommandOperation op) {
      return getParamNames_(op, CommandMeta.this);
    }


    default String getParamSubstitute(String name) {
      return name;
    }
  }
}
