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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.core.ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG;

public class SolrConfigHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String CONFIGSET_EDITING_DISABLED_ARG = "disable.configEdit";
  public static final boolean configEditing_disabled = Boolean.getBoolean(CONFIGSET_EDITING_DISABLED_ARG);
  private Lock reloadLock = new ReentrantLock(true);

  public Lock getReloadLock() {
    return reloadLock;
  }

  private boolean isImmutableConfigSet = false;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {

    RequestHandlerUtils.setWt(req, CommonParams.JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    if ("POST".equals(httpMethod)) {
      if (configEditing_disabled || isImmutableConfigSet) {
        final String reason = configEditing_disabled ? "due to " + CONFIGSET_EDITING_DISABLED_ARG : "because ConfigSet is immutable";
        throw new SolrException(SolrException.ErrorCode.FORBIDDEN, " solrconfig editing is not enabled " + reason);
      }
      new SolrConfigManager().handlePOST(req, rsp, req.getCore().getSolrConfig(), req.getCore().getResourceLoader());
    } else {
      new SolrConfigManager().handleGET(req, rsp, reloadLock, req.getCore().getSolrConfig(), req.getCore().getResourceLoader());
    }
  }

  @Override
  public void inform(SolrCore core) {
    isImmutableConfigSet = getImmutable(core);
  }

  public static boolean getImmutable(SolrCore core) {
    NamedList configSetProperties = core.getConfigSetProperties();
    if (configSetProperties == null) return false;
    Object immutable = configSetProperties.get(IMMUTABLE_CONFIGSET_ARG);
    return immutable != null && Boolean.parseBoolean(immutable.toString());
  }

  @Override
  public SolrRequestHandler getSubHandler(String path) {
    if (subPaths.contains(path)) return this;
    if (path.startsWith("/params/")) return this;
    return null;
  }


  private static Set<String> subPaths = new HashSet<>(Arrays.asList("/overlay", "/params", "/updateHandler",
      "/query", "/jmx", "/requestDispatcher", "/znodeVersion"));

  static {
    for (SolrConfig.SolrPluginInfo solrPluginInfo : SolrConfig.plugins)
      subPaths.add("/" + solrPluginInfo.getCleanTag());

  }

  //////////////////////// SolrInfoMBeans methods //////////////////////


  @Override
  public String getDescription() {
    return "Edit solrconfig.xml";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return Name.CONFIG_READ_PERM;
      case "POST":
        return Name.CONFIG_EDIT_PERM;
      default:
        return null;
    }
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this,
        "core.config",
        "core.config.Commands",
        "core.config.Params",
        "core.config.Params.Commands");
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
