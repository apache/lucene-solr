package org.apache.solr.handler;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.ManagedIndexSchema;
import org.apache.solr.schema.SchemaManager;
import org.apache.solr.schema.ZkIndexSchemaReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.CommonParams.JSON;
import static org.apache.solr.core.ConfigSetProperties.IMMUTABLE_CONFIGSET_ARG;

public class SchemaHandler extends RequestHandlerBase {
  private static final Logger log = LoggerFactory.getLogger(SchemaHandler.class);
  private boolean isImmutableConfigSet = false;

  @Override
  public void init(NamedList args) {
    super.init(args);
    Object immutable = args.get(IMMUTABLE_CONFIGSET_ARG);
    isImmutableConfigSet = immutable  != null ? Boolean.parseBoolean(immutable.toString()) : false;
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
          throw new SolrException(SolrException.ErrorCode.NOT_FOUND, "No such path " + path);
        }
      }

    } catch (Exception e) {
      rsp.setException(e);
    }
  }

  private static Set<String> subPaths = new HashSet<>(Arrays.asList(
      "/version",
      "/uniquekey",
      "/name",
      "/similarity",
      "/defaultsearchfield",
      "/solrqueryparser",
      "/zkversion",
      "/solrqueryparser/defaultoperator"
  ));

  @Override
  public SolrRequestHandler getSubHandler(String subPath) {
    if (subPaths.contains(subPath)) return this;
    return null;
  }

  @Override
  public String getDescription() {
    return "CRUD operations over the Solr schema";
  }
}
