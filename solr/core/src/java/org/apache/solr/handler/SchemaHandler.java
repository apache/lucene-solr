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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JSON;

public class SchemaHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean isImmutableConfigSet = false;

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    RequestHandlerUtils.setWt(req, JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    if ("POST".equals(httpMethod)) {
      if (isImmutableConfigSet) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ConfigSet is immutable");
      }
      if (req.getContentStreams() == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "no stream");
      }

      try {
        List errs = new SchemaManager().doCmdOperations(req.getCommands(false), req.getCore(), req.getParams());
        if (!errs.isEmpty())
          throw new ApiBag.ExceptionWithErrObject(SolrException.ErrorCode.BAD_REQUEST,"error processing commands", errs);
      } catch (IOException e) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Error reading input String " + e.getMessage(), e);
      }
    } else {
      handleGET(req, rsp);
    }
  }

  @Override
  public PermissionNameProvider.Name getPermissionName(AuthorizationContext ctx) {
    switch (ctx.getHttpMethod()) {
      case "GET":
        return PermissionNameProvider.Name.SCHEMA_READ_PERM;
      case "POST":
        return PermissionNameProvider.Name.SCHEMA_EDIT_PERM;
      default:
        return null;
    }
  }

  private void handleGET(SolrQueryRequest req, SolrQueryResponse rsp) {
    String path = (String) req.getContext().get("path");
    try {
      Map<String, Object> params = new SchemaManager().executeGET(path, req.getSchema(), req.getParams(), req.getCore().getResourceLoader());
      Iterator<String> iterator = params.keySet().iterator();
      while (iterator.hasNext()) {
        String key = iterator.next();
        rsp.add(key, params.get(key));
      }
    } catch (Exception e) {
      rsp.setException(e);
    }
  }


  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    List<String> parts = StrUtils.splitSmart(subPath, '/', true);
    String prefix =  parts.get(0);
    if (SchemaManager.getSubPaths().contains(prefix)) return this;

    return null;
  }

  @Override
  public String getDescription() {
    return "CRUD operations over the Solr schema";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public void inform(SolrCore core) {
    isImmutableConfigSet = SolrConfigHandler.getImmutable(core);
  }

  @Override
  public Collection<Api> getApis() {
    return ApiBag.wrapRequestHandlers(this, "core.SchemaRead",
        "core.SchemaRead.fields",
        "core.SchemaRead.copyFields",
        "core.SchemaEdit",
        "core.SchemaRead.dynamicFields_fieldTypes"
        );

  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }
}
