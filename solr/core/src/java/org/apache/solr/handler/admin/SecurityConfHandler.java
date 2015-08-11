package org.apache.solr.handler.admin;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkStateReader.ConfigData;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.ConfigEditablePlugin;
import org.apache.solr.util.CommandOperation;
import org.apache.zookeeper.KeeperException;

public class SecurityConfHandler extends RequestHandlerBase {
  private CoreContainer cores;

  public SecurityConfHandler(CoreContainer coreContainer) {
    this.cores = coreContainer;
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


  }

