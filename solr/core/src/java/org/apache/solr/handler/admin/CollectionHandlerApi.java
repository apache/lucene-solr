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

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.common.Callable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionHandlerApi extends BaseHandlerApiSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CollectionsHandler handler;
  static Collection<ApiCommand> apiCommands = createCollMapping();

  private static Collection<ApiCommand> createCollMapping() {
    Map<Meta, ApiCommand> result = new EnumMap<>(Meta.class);

    for (Meta meta : Meta.values()) {
      for (CollectionOperation op : CollectionOperation.values()) {
        if (op.action == meta.action) {
          result.put(meta, new ApiCommand() {
            @Override
            public CommandMeta meta() {
              return meta;
            }

            @Override
            public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
              ((CollectionHandlerApi) apiHandler).handler.invokeAction(req, rsp, ((CollectionHandlerApi) apiHandler).handler.coreContainer, op.action, op);
            }
          });
        }
      }
    }
    //The following APIs have only V2 implementations
    addApi(result, Meta.GET_NODES, params -> params.rsp.add("nodes", ((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getClusterState().getLiveNodes()));
    addApi(result, Meta.SET_CLUSTER_PROPERTY_OBJ, params -> {
      List<CommandOperation> commands = params.req.getCommands(true);
      if (commands == null || commands.isEmpty()) throw new RuntimeException("Empty commands");
      ClusterProperties clusterProperties = new ClusterProperties(((CollectionHandlerApi) params.apiHandler).handler.coreContainer.getZkController().getZkClient());

      try {
        clusterProperties.setClusterProperties(commands.get(0).getDataMap());
      } catch (Exception e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error in API", e);
      }
    });

    for (Meta meta : Meta.values()) {
      if (result.get(meta) == null) {
        log.error("ERROR_INIT. No corresponding API implementation for : " + meta.commandName);
      }
    }

    return result.values();
  }

  private static void addApi(Map<Meta, ApiCommand> result, Meta metaInfo, Callable<ApiParams> fun) {
    result.put(metaInfo, new ApiCommand() {
      @Override
      public CommandMeta meta() {
        return metaInfo;
      }

      @Override
      public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
        fun.call(new ApiParams(req, rsp, apiHandler));
      }
    });
  }

  static class ApiParams {
    final SolrQueryRequest req;
    final SolrQueryResponse rsp;
    final BaseHandlerApiSupport apiHandler;

    ApiParams(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) {
      this.req = req;
      this.rsp = rsp;
      this.apiHandler = apiHandler;
    }
  }

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
  }

  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }

  @Override
  protected List<V2EndPoint> getEndPoints() {
    return Arrays.asList(CollectionApiMapping.EndPoint.values());
  }

}
