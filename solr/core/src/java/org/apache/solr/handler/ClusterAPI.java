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

package org.apache.solr.handler;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.ClusterPropInfo;
import org.apache.solr.client.solrj.request.beans.CreateConfigInfo;
import org.apache.solr.cloud.OverseerConfigSetMessageHandler;
import org.apache.solr.cluster.placement.impl.PlacementPluginConfigImpl;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.REQUESTID;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

public class ClusterAPI {
//  private final CoreContainer coreContainer;
  private final CollectionsHandler collectionsHandler;
  private final ConfigSetsHandler configSetsHandler;

  public  final Commands commands = new Commands();
  public  final ConfigSetCommands configSetCommands = new ConfigSetCommands();

  public ClusterAPI(CollectionsHandler ch, ConfigSetsHandler configSetsHandler) {
    this.collectionsHandler = ch;
    this.configSetsHandler = configSetsHandler;
  }

  @EndPoint(method = GET,
      path = "/cluster/overseer",
      permission = COLL_READ_PERM)
  public void getOverseerStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    collectionsHandler.getCoreContainer().getCollectionsHandler().handleRequestBody(wrapParams(req, "action", OVERSEERSTATUS.toString()), rsp);
  }

  @EndPoint(method = GET,
      path = "/cluster",
      permission = COLL_READ_PERM)
  public void getCluster(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.LIST_OP.execute(req, rsp, collectionsHandler.getCoreContainer().getCollectionsHandler());
  }

  @EndPoint(method = DELETE,
      path = "/cluster/command-status/{id}",
      permission = COLL_EDIT_PERM)
  public void deleteCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    wrapParams(req, REQUESTID, req.getPathTemplateValues().get("id"));
    CollectionsHandler.CollectionOperation.DELETESTATUS_OP.execute(req, rsp, collectionsHandler);
  }

  @EndPoint(method = DELETE,
      path =   "/cluster/configs/{name}",
      permission = CONFIG_EDIT_PERM
  )
  public void deleteConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req,
        "action", ConfigSetParams.ConfigSetAction.DELETE.toString(),
        CommonParams.NAME, req.getPathTemplateValues().get("name"));
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @EndPoint(method = GET,
      path = "/cluster/configs",
      permission = CONFIG_READ_PERM)
  public void listConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    ConfigSetsHandler.ConfigSetOperation.LIST_OP.call(req, rsp, configSetsHandler);
  }

  @EndPoint(method = POST,
  path = "/cluster/configs",
      permission = CONFIG_EDIT_PERM
  )
  public class ConfigSetCommands {

    @Command(name = "create")
    @SuppressWarnings("unchecked")
    public void create(PayloadObj<CreateConfigInfo> obj) throws Exception {
      Map<String, Object> mapVals = obj.get().toMap(new HashMap<>());
      Map<String,Object> customProps = (Map<String, Object>) mapVals.remove("properties");
      if(customProps!= null) {
        customProps.forEach((k, o) -> mapVals.put(OverseerConfigSetMessageHandler.PROPERTY_PREFIX+"."+ k, o));
      }
      mapVals.put("action", ConfigSetParams.ConfigSetAction.CREATE.toString());
      configSetsHandler.handleRequestBody(wrapParams(obj.getRequest(), mapVals), obj.getResponse());
    }

  }

  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Object... def) {
    Map<String, Object> m = Utils.makeMap(def);
    return wrapParams(req, m);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Map m) {
    ModifiableSolrParams solrParams = new ModifiableSolrParams();
    m.forEach((k, v) -> {
      if(v == null) return;
      solrParams.add(k.toString(), String.valueOf(v));
    });
    DefaultSolrParams dsp = new DefaultSolrParams(req.getParams(),solrParams);
    req.setParams(dsp);
    return req;
  }

  @EndPoint(method = GET,
      path = "/cluster/command-status",
      permission = COLL_READ_PERM)
  public void getCommandStatus(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.REQUESTSTATUS_OP.execute(req, rsp, collectionsHandler);
  }

  @EndPoint(method = GET,
      path = "/cluster/nodes",
      permission = COLL_READ_PERM)
  public void getNodes(SolrQueryRequest req, SolrQueryResponse rsp) {
    rsp.add("nodes", collectionsHandler.getCoreContainer().getZkController().getClusterState().getLiveNodes());
  }

  @EndPoint(method = POST,
      path = "/cluster",
      permission = COLL_EDIT_PERM)
  public class Commands {
    @Command(name = "add-role")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void addRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map m = info.toMap(new HashMap<>());
      m.put("action", ADDROLE.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "remove-role")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void removeRole(PayloadObj<RoleInfo> obj) throws Exception {
      RoleInfo info = obj.get();
      Map m = info.toMap(new HashMap<>());
      m.put("action", REMOVEROLE.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "set-obj-property")
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setObjProperty(PayloadObj<ClusterPropInfo> obj) {
      //Not using the object directly here because the API differentiate between {name:null} and {}
      Map m = obj.getDataMap();
      ClusterProperties clusterProperties = new ClusterProperties(collectionsHandler.getCoreContainer().getZkController().getZkClient());
      try {
        clusterProperties.setClusterProperties(m);
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }
    }

    @Command(name = "set-property")
    public void setProperty(PayloadObj<Map<String,String>> obj) throws Exception {
      Map<String,Object> m =  obj.getDataMap();
      m.put("action", CLUSTERPROP.toString());
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(),m ), obj.getResponse());
    }

    @Command(name = "set-placement-plugin")
    public void setPlacementPlugin(PayloadObj<Map<String, Object>> obj) {
      Map<String, Object> placementPluginConfig = obj.getDataMap();
      ClusterProperties clusterProperties = new ClusterProperties(collectionsHandler.getCoreContainer().getZkController().getZkClient());
      // When the json contains { "set-placement-plugin" : null }, the map is empty, not null.
      final boolean unset = placementPluginConfig.isEmpty();
      // Very basic sanity check. Real validation will be done when the config is used...
      if (!unset && !placementPluginConfig.containsKey(PlacementPluginConfigImpl.CONFIG_CLASS)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must contain " + PlacementPluginConfigImpl.CONFIG_CLASS + " attribute (or be null)");
      }
      try {
        // Need to reset to null first otherwise the mappings in placementPluginConfig are added to existing ones
        // in /clusterprops.json rather than replacing them. If removing the config, that's all we do.
        clusterProperties.setClusterProperties(
                Collections.singletonMap(PlacementPluginConfigImpl.PLACEMENT_PLUGIN_CONFIG_KEY, null));
        if (!unset) {
          clusterProperties.setClusterProperties(
                  Collections.singletonMap(PlacementPluginConfigImpl.PLACEMENT_PLUGIN_CONFIG_KEY, placementPluginConfig));
        }
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }
    }
  }

  public static class RoleInfo implements ReflectMapWriter {
    @JsonProperty(required = true)
    public String node;
    @JsonProperty(required = true)
    public String role;
  }
}
