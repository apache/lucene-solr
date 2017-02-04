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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.api.ApiBag.ReqHandlerToApi;
import org.apache.solr.api.SpecProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader.ConfigData;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.ConfigEditablePlugin;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.CommandOperation;
import org.apache.solr.util.JsonSchemaValidator;
import org.apache.zookeeper.KeeperException;

public class SecurityConfHandler extends RequestHandlerBase implements PermissionNameProvider {
  private CoreContainer cores;

  public SecurityConfHandler(CoreContainer coreContainer) {
    this.cores = coreContainer;
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return PermissionNameProvider.Name.SECURITY_READ_PERM;
      case "POST":
        return PermissionNameProvider.Name.SECURITY_EDIT_PERM;
      default:
        return null;
    }
  }

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrConfigHandler.setWt(req, CommonParams.JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    String path = (String) req.getContext().get("path");
    String key = path.substring(path.lastIndexOf('/')+1);
    if ("GET".equals(httpMethod)) {
      getConf(rsp, key);
    } else if ("POST".equals(httpMethod)) {
      Object plugin = getPlugin(key);
      doEdit(req, rsp, path, key, plugin);
    }
  }

  private void doEdit(SolrQueryRequest req, SolrQueryResponse rsp, String path, final String key, final Object plugin)
      throws IOException {
    ConfigEditablePlugin configEditablePlugin = null;

    if (plugin == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No " + key + " plugin configured");
    }
    if (plugin instanceof ConfigEditablePlugin) {
      configEditablePlugin = (ConfigEditablePlugin) plugin;
    } else {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, key + " plugin is not editable");
    }

    if (req.getContentStreams() == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No contentStream");
    }
    List<CommandOperation> ops = CommandOperation.readCommands(req.getContentStreams(), rsp);
    if (ops == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No commands");
    }
    for (; ; ) {
      ConfigData data = getSecurityProps(true);
      Map<String, Object> latestConf = (Map<String, Object>) data.data.get(key);
      if (latestConf == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "No configuration present for " + key);
      }
      List<CommandOperation> commandsCopy = CommandOperation.clone(ops);
      Map<String, Object> out = configEditablePlugin.edit(Utils.getDeepCopy(latestConf, 4) , commandsCopy);
      if (out == null) {
        List<Map> errs = CommandOperation.captureErrors(commandsCopy);
        if (!errs.isEmpty()) {
          rsp.add(CommandOperation.ERR_MSGS, errs);
          return;
        }
        //no edits
        return;
      } else {
        if(!Objects.equals(latestConf.get("class") , out.get("class"))){
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "class cannot be modified");
        }
        Map meta = getMapValue(out, "");
        meta.put("v", data.version+1);//encode the expected zkversion
        data.data.put(key, out);
        if(persistConf("/security.json", Utils.toJSON(data.data), data.version)) return;
      }
    }
  }

  Object getPlugin(String key) {
    Object plugin = null;
    if ("authentication".equals(key)) plugin = cores.getAuthenticationPlugin();
    if ("authorization".equals(key)) plugin = cores.getAuthorizationPlugin();
    return plugin;
  }

  ConfigData getSecurityProps(boolean getFresh) {
    return cores.getZkController().getZkStateReader().getSecurityProps(getFresh);
  }

  boolean persistConf(String path,  byte[] buf, int version) {
    try {
      cores.getZkController().getZkClient().setData(path,buf,version, true);
      return true;
    } catch (KeeperException.BadVersionException bdve){
      return false;
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, " Unable to persist conf",e);
    }
  }


  private void getConf(SolrQueryResponse rsp, String key) {
    ConfigData map = cores.getZkController().getZkStateReader().getSecurityProps(false);
    Object o = map == null ? null : map.data.get(key);
    if (o == null) {
      rsp.add(CommandOperation.ERR_MSGS, Collections.singletonList("No " + key + " configured"));
    } else {
      rsp.add(key+".enabled", getPlugin(key)!=null);
      rsp.add(key, o);
    }
  }

  public static Map<String, Object> getMapValue(Map<String, Object> lookupMap, String key) {
    Map<String, Object> m = (Map<String, Object>) lookupMap.get(key);
    if (m == null) lookupMap.put(key, m = new LinkedHashMap<>());
    return m;
  }
  public static List getListValue(Map<String, Object> lookupMap, String key) {
    List l = (List) lookupMap.get(key);
    if (l == null) lookupMap.put(key, l= new ArrayList());
    return l;
  }

  @Override
  public String getDescription() {
    return "Edit or read security configuration";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }


  private Collection<Api> apis;
  private AuthenticationPlugin authcPlugin;
  private AuthorizationPlugin authzPlugin;

  @Override
  public Collection<Api> getApis() {
    if (apis == null) {
      synchronized (this) {
        if (apis == null) {
          Collection<Api> apis = new ArrayList<>();
          final SpecProvider authcCommands = ApiBag.getSpec("cluster.security.authentication.Commands");
          final SpecProvider authzCommands = ApiBag.getSpec("cluster.security.authorization.Commands");
          apis.add(new ReqHandlerToApi(this, ApiBag.getSpec("cluster.security.authentication")));
          apis.add(new ReqHandlerToApi(this, ApiBag.getSpec("cluster.security.authorization")));
          SpecProvider authcSpecProvider = () -> {
            AuthenticationPlugin authcPlugin = cores.getAuthenticationPlugin();
            return authcPlugin != null && authcPlugin instanceof SpecProvider ?
                ((SpecProvider) authcPlugin).getSpec() :
                authcCommands.getSpec();
          };

          apis.add(new ReqHandlerToApi(this, authcSpecProvider) {
            @Override
            public synchronized Map<String, JsonSchemaValidator> getCommandSchema() {
              //it is possible that the Authentication plugin is modified since the last call. invalidate the
              // the cached commandSchema
              if(SecurityConfHandler.this.authcPlugin != cores.getAuthenticationPlugin()) commandSchema = null;
              SecurityConfHandler.this.authcPlugin = cores.getAuthenticationPlugin();
              return super.getCommandSchema();
            }
          });

          SpecProvider authzSpecProvider = () -> {
            AuthorizationPlugin authzPlugin = cores.getAuthorizationPlugin();
            return authzPlugin != null && authzPlugin instanceof SpecProvider ?
                ((SpecProvider) authzPlugin).getSpec() :
                authzCommands.getSpec();
          };
          apis.add(new ApiBag.ReqHandlerToApi(this, authzSpecProvider) {
            @Override
            public synchronized Map<String, JsonSchemaValidator> getCommandSchema() {
              //it is possible that the Authorization plugin is modified since the last call. invalidate the
              // the cached commandSchema
              if(SecurityConfHandler.this.authzPlugin != cores.getAuthorizationPlugin()) commandSchema = null;
              SecurityConfHandler.this.authzPlugin = cores.getAuthorizationPlugin();
              return super.getCommandSchema();
            }
          });

          this.apis = ImmutableList.copyOf(apis);
        }
      }
    }
    return this.apis;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}

