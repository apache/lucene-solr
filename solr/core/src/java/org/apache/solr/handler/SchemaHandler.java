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
import java.util.*;
import java.util.function.BiConsumer;

import org.apache.solr.api.Api;
import org.apache.solr.api.ApiBag;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrClassLoader;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.pkg.PackageListeningClassLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.rest.RestManager;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.security.PermissionNameProvider;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;
import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.COPY_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.DYNAMIC_FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELDS;
import static org.apache.solr.schema.IndexSchema.SchemaProps.Handler.FIELD_TYPES;

@SuppressWarnings({"unchecked"})
public class SchemaHandler extends RequestHandlerBase implements SolrCoreAware, PermissionNameProvider {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean isImmutableConfigSet = false;
  private SolrRequestHandler managedResourceRequestHandler;

  private static final Map<String, String> level2;

  static {
    @SuppressWarnings({"rawtypes"})
    Map s = Utils.makeMap(
        FIELD_TYPES.nameLower, null,
        FIELDS.nameLower, "fl",
        DYNAMIC_FIELDS.nameLower, "fl",
        COPY_FIELDS.nameLower, null
    );

    level2 = Collections.unmodifiableMap(s);
  }


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
        @SuppressWarnings({"rawtypes"})
        List errs = new SchemaManager(req).performOperations();
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
      case "PUT":
      case "DELETE":
      case "POST":
        return PermissionNameProvider.Name.SCHEMA_EDIT_PERM;
      default:
        return null;
    }
  }

  private void handleGET(SolrQueryRequest req, SolrQueryResponse rsp) {
    try {
      String path = (String) req.getContext().get("path");
      switch (path) {
        case "/schema":
          rsp.add(IndexSchema.SCHEMA, req.getSchema().getNamedPropertyValues());
          break;
        case "/schema/version":
          rsp.add(IndexSchema.VERSION, req.getSchema().getVersion());
          break;
        case "/schema/uniquekey":
          rsp.add(IndexSchema.UNIQUE_KEY, req.getSchema().getUniqueKeyField().getName());
          break;
        case "/schema/similarity":
          rsp.add(IndexSchema.SIMILARITY, req.getSchema().getSimilarityFactory().getNamedPropertyValues());
          break;
        case "/schema/name": {
          final String schemaName = req.getSchema().getSchemaName();
          if (null == schemaName) {
            String message = "Schema has no name";
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, message);
          }
          rsp.add(IndexSchema.NAME, schemaName);
          break;
        }
        case "/schema/zkversion": {
          int refreshIfBelowVersion = -1;
          Object refreshParam = req.getParams().get("refreshIfBelowVersion");
          if (refreshParam != null)
            refreshIfBelowVersion = (refreshParam instanceof Number) ? ((Number) refreshParam).intValue()
                : Integer.parseInt(refreshParam.toString());
          int zkVersion = -1;
          IndexSchema schema = req.getSchema();
          if (schema instanceof ManagedIndexSchema) {
            ManagedIndexSchema managed = (ManagedIndexSchema) schema;
            zkVersion = managed.getSchemaZkVersion();
            if (refreshIfBelowVersion != -1 && zkVersion < refreshIfBelowVersion) {
              log.info("REFRESHING SCHEMA (refreshIfBelowVersion={}, currentVersion={}) before returning version!"
                  , refreshIfBelowVersion, zkVersion);
              ZkSolrResourceLoader zkSolrResourceLoader = (ZkSolrResourceLoader) req.getCore().getResourceLoader();
              ZkIndexSchemaReader zkIndexSchemaReader = zkSolrResourceLoader.getZkIndexSchemaReader();
              managed = zkIndexSchemaReader.refreshSchemaFromZk(refreshIfBelowVersion);
              zkVersion = managed.getSchemaZkVersion();
            }
          }
          rsp.add("zkversion", zkVersion);
          break;
        }
        default: {
          List<String> parts = StrUtils.splitSmart(path, '/', true);
          if (parts.size() > 1 && level2.containsKey(parts.get(1))) {
            String realName = parts.get(1);
            String fieldName = IndexSchema.nameMapping.get(realName);

            String pathParam = level2.get(realName);
            if (parts.size() > 2) {
              req.setParams(SolrParams.wrapDefaults(new MapSolrParams(singletonMap(pathParam, parts.get(2))), req.getParams()));
            }
            @SuppressWarnings({"rawtypes"})
            Map propertyValues = req.getSchema().getNamedPropertyValues(realName, req.getParams());
            Object o = propertyValues.get(fieldName);
            if(parts.size()> 2) {
              String name = parts.get(2);
              if (o instanceof List) {
                @SuppressWarnings({"rawtypes"})
                List list = (List) o;
                for (Object obj : list) {
                  if (obj instanceof SimpleOrderedMap) {
                    @SuppressWarnings({"rawtypes"})
                    SimpleOrderedMap simpleOrderedMap = (SimpleOrderedMap) obj;
                    if(name.equals(simpleOrderedMap.get("name"))) {
                      rsp.add(fieldName.substring(0, realName.length() - 1), simpleOrderedMap);
                      insertPackageInfo(rsp.getValues(), req);
                      return;
                    }
                  }
                }
              }
              throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
            } else {
              rsp.add(fieldName, o);
            }
            insertPackageInfo(rsp.getValues(), req);
            return;
          }

          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
        }
      }

    } catch (Exception e) {
      rsp.setException(e);
    }
  }

  /**
   * If a plugin is loaded from a package, the version of the package being used should be added
   * to the response
   */
  @SuppressWarnings("rawtypes")
  private void insertPackageInfo(Object o, SolrQueryRequest req) {
    if (!req.getParams().getBool("meta", false)) return;
    if (o instanceof List) {
      List l = (List) o;
      for (Object o1 : l) {
        if (o1 instanceof NamedList || o1 instanceof List) insertPackageInfo(o1, req);
      }

    } else if (o instanceof NamedList) {
      NamedList nl = (NamedList) o;
      nl.forEach((BiConsumer) (n, v) -> {
        if (v instanceof NamedList || v instanceof List) insertPackageInfo(v, req);
      });
      Object v = nl.get("class");
      if (v instanceof String) {
        String klas = (String) v;
        PluginInfo.ClassName parsedClassName = new PluginInfo.ClassName(klas);
        if (parsedClassName.pkg != null) {
          SolrClassLoader solrClassLoader = req.getCore().getLatestSchema().getSolrClassLoader();
          MapWriter mw = solrClassLoader instanceof PackageListeningClassLoader ?
              ((PackageListeningClassLoader) solrClassLoader).getPackageVersion(parsedClassName) : null;
          if (mw != null) nl.add("_packageinfo_", mw);
        }
      }

    }

  }

  private static Set<String> subPaths = new HashSet<>(Arrays.asList(
      "version",
      "uniquekey",
      "name",
      "similarity",
      "defaultsearchfield",
      "solrqueryparser",
      "zkversion"
  ));
  static {
    subPaths.addAll(level2.keySet());
  }

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    List<String> parts = StrUtils.splitSmart(subPath, '/', true);
    String prefix =  parts.get(0);
    if(subPaths.contains(prefix)) return this;

    if(managedResourceRequestHandler != null) return managedResourceRequestHandler;

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
    this.managedResourceRequestHandler =  new ManagedResourceRequestHandler(core.getRestManager());
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

  private  class ManagedResourceRequestHandler extends RequestHandlerBase implements PermissionNameProvider {


    private final RestManager restManager;

    private ManagedResourceRequestHandler(RestManager restManager) {
      this.restManager = restManager;
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      RestManager.ManagedEndpoint me = new RestManager.ManagedEndpoint(restManager);
      me.doInit(req, rsp);
      me.delegateRequestToManagedResource();
    }

    @Override
    public Name getPermissionName(AuthorizationContext ctx) {
      return SchemaHandler.this.getPermissionName(ctx);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public String getDescription() {
      return null;
    }
  }
}
