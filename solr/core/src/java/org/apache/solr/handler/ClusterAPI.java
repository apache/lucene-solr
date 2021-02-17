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

import java.util.HashMap;
import java.util.Map;

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.ClusterPropPayload;
import org.apache.solr.client.solrj.request.beans.CreateConfigPayload;
import org.apache.solr.client.solrj.request.beans.RateLimiterPayload;
import org.apache.solr.cloud.OverseerConfigSetMessageHandler;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.params.DefaultSolrParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.ReflectMapWriter;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.DELETE;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.client.solrj.SolrRequest.METHOD.PUT;
import static org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.REQUESTID;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.ADDROLE;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CLUSTERPROP;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.OVERSEERSTATUS;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.REMOVEROLE;
import static org.apache.solr.core.RateLimiterConfig.RL_CONFIG_KEY;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

/** All V2 APIs that have  a prefix of /api/cluster/
 *
 */
public class ClusterAPI {
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
    getCoreContainer().getCollectionsHandler().handleRequestBody(wrapParams(req, "action", OVERSEERSTATUS.toString()), rsp);
  }

  @EndPoint(method = GET,
      path = "/cluster",
      permission = COLL_READ_PERM)
  public void getCluster(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.LIST_OP.execute(req, rsp, getCoreContainer().getCollectionsHandler());
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
    public void create(PayloadObj<CreateConfigPayload> obj) throws Exception {
      Map<String, Object> mapVals = obj.get().toMap(new HashMap<>());
      Map<String,Object> customProps = (Map<String, Object>) mapVals.remove("properties");
      if(customProps!= null) {
        customProps.forEach((k, o) -> mapVals.put(OverseerConfigSetMessageHandler.PROPERTY_PREFIX+"."+ k, o));
      }
      mapVals.put("action", ConfigSetParams.ConfigSetAction.CREATE.toString());
      configSetsHandler.handleRequestBody(wrapParams(obj.getRequest(), mapVals), obj.getResponse());
    }

  }

  @EndPoint(method = PUT,
      path =   "/cluster/configs/{name}",
      permission = CONFIG_EDIT_PERM
  )
  public void uploadConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req,
            "action", ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
            CommonParams.NAME, req.getPathTemplateValues().get("name"),
            ConfigSetParams.OVERWRITE, true,
            ConfigSetParams.CLEANUP, false);
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @EndPoint(method = PUT,
      path =   "/cluster/configs/{name}/*",
      permission = CONFIG_EDIT_PERM
  )
  public void insertIntoConfigSet(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    String path = req.getPathTemplateValues().get("*");
    if (path == null || path.isBlank()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "In order to insert a file in a configSet, a filePath must be provided in the url after the name of the configSet.");
    }
    req = wrapParams(req,
            "action", ConfigSetParams.ConfigSetAction.UPLOAD.toString(),
            CommonParams.NAME, req.getPathTemplateValues().get("name"),
            ConfigSetParams.FILE_PATH, path,
            ConfigSetParams.OVERWRITE, true,
            ConfigSetParams.CLEANUP, false);
    configSetsHandler.handleRequestBody(req, rsp);
  }

  @SuppressWarnings({"rawtypes"})
  public static SolrQueryRequest wrapParams(SolrQueryRequest req, Object... def) {
    Map m = Utils.makeMap(def);
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
    rsp.add("nodes", getCoreContainer().getZkController().getClusterState().getLiveNodes());
  }

  private CoreContainer getCoreContainer() {
    return collectionsHandler.getCoreContainer();
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
    public void setObjProperty(PayloadObj<ClusterPropPayload> obj) {
      //Not using the object directly here because the API differentiate between {name:null} and {}
      Map m = obj.getDataMap();
      ClusterProperties clusterProperties = new ClusterProperties(getCoreContainer().getZkController().getZkClient());
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
      collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), m), obj.getResponse());
    }

    @Command(name = "set-ratelimiter")
    public void setRateLimiters(PayloadObj<RateLimiterPayload> payLoad) {
      RateLimiterPayload rateLimiterConfig = payLoad.get();
      ClusterProperties clusterProperties = new ClusterProperties(getCoreContainer().getZkController().getZkClient());

      try {
        clusterProperties.update(rateLimiterConfig == null?
                null:
                rateLimiterConfig,
                RL_CONFIG_KEY);
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
