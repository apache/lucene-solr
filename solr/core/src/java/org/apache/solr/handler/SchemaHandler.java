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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JSON;

public class SchemaHandler extends RequestHandlerBase implements SolrCoreAware {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private boolean isImmutableConfigSet = false;

  private static final Map<String, String> level2;

  static {
    Set<String> s = ImmutableSet.of(
        IndexSchema.FIELD_TYPES,
        IndexSchema.FIELDS,
        IndexSchema.DYNAMIC_FIELDS,
        IndexSchema.COPY_FIELDS
    );
    Map<String, String> m = new HashMap<>();
    for (String s1 : s) {
      m.put(s1, s1);
      m.put(s1.toLowerCase(Locale.ROOT), s1);
    }
    level2 = ImmutableMap.copyOf(m);
  }


  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    SolrConfigHandler.setWt(req, JSON);
    String httpMethod = (String) req.getContext().get("httpMethod");
    if ("POST".equals(httpMethod)) {
      if (isImmutableConfigSet) {
        rsp.add("errors", "ConfigSet is immutable");
        return;
      }
      if (req.getContentStreams() == null) {
        rsp.add("errors", "no stream");
        return;
      }

      for (ContentStream stream : req.getContentStreams()) {
        try {
          List errs = new SchemaManager(req).performOperations(stream.getReader());
          if (!errs.isEmpty()) rsp.add("errors", errs);
        } catch (IOException e) {
          rsp.add("errors", Collections.singletonList("Error reading input String " + e.getMessage()));
          rsp.setException(e);
        }
        break;
      }
    } else {
      handleGET(req, rsp);
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
        case "/schema/defaultsearchfield": {
          final String defaultSearchFieldName = req.getSchema().getDefaultSearchFieldName();
          if (null == defaultSearchFieldName) {
            final String message = "undefined " + IndexSchema.DEFAULT_SEARCH_FIELD;
            throw new SolrException(SolrException.ErrorCode.NOT_FOUND, message);
          }
          rsp.add(IndexSchema.DEFAULT_SEARCH_FIELD, defaultSearchFieldName);
          break;
        }
        case "/schema/solrqueryparser": {
          SimpleOrderedMap<Object> props = new SimpleOrderedMap<>();
          props.add(IndexSchema.DEFAULT_OPERATOR, req.getSchema().getQueryParserDefaultOperator());
          rsp.add(IndexSchema.SOLR_QUERY_PARSER, props);
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
              log.info("REFRESHING SCHEMA (refreshIfBelowVersion=" + refreshIfBelowVersion +
                  ", currentVersion=" + zkVersion + ") before returning version!");
              ZkSolrResourceLoader zkSolrResourceLoader = (ZkSolrResourceLoader) req.getCore().getResourceLoader();
              ZkIndexSchemaReader zkIndexSchemaReader = zkSolrResourceLoader.getZkIndexSchemaReader();
              managed = zkIndexSchemaReader.refreshSchemaFromZk(refreshIfBelowVersion);
              zkVersion = managed.getSchemaZkVersion();
            }
          }
          rsp.add("zkversion", zkVersion);
          break;
        }
        case "/schema/solrqueryparser/defaultoperator": {
          rsp.add(IndexSchema.DEFAULT_OPERATOR, req.getSchema().getQueryParserDefaultOperator());
          break;
        }
        default: {
          List<String> parts = StrUtils.splitSmart(path, '/');
          if (parts.get(0).isEmpty()) parts.remove(0);
          if (parts.size() > 1 && level2.containsKey(parts.get(1))) {
            String realName = level2.get(parts.get(1));
            SimpleOrderedMap<Object> propertyValues = req.getSchema().getNamedPropertyValues(req.getParams());
            Object o = propertyValues.get(realName);
            if(parts.size()> 2) {
              String name = parts.get(2);
              if (o instanceof List) {
                List list = (List) o;
                for (Object obj : list) {
                  if (obj instanceof SimpleOrderedMap) {
                    SimpleOrderedMap simpleOrderedMap = (SimpleOrderedMap) obj;
                    if(name.equals(simpleOrderedMap.get("name"))) {
                      rsp.add(realName.substring(0, realName.length() - 1), simpleOrderedMap);
                      return;
                    }
                  }
                }
              }
              throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
            } else {
              rsp.add(realName, o);
            }
            return;
          }

          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
        }
      }

    } catch (Exception e) {
      rsp.setException(e);
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
    List<String> parts = StrUtils.splitSmart(subPath, '/');
    if (parts.get(0).isEmpty()) parts.remove(0);
    String prefix =  parts.get(0);
    if(subPaths.contains(prefix)) return this;

    return null;
  }

  @Override
  public String getDescription() {
    return "CRUD operations over the Solr schema";
  }

  @Override
  public void inform(SolrCore core) {
    isImmutableConfigSet = SolrConfigHandler.getImmutable(core);
  }
}
