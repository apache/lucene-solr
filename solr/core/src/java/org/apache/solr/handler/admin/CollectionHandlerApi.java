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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.api.Api;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Arrays.asList;

public class CollectionHandlerApi extends BaseHandlerApiSupport {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  final CollectionsHandler handler;
  final Collection<ApiCommand> apiCommands;

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
    apiCommands = createApiMapping();
  }

  private Collection<ApiCommand> createApiMapping() {

    //there
    Map<CommandMeta, ApiCommand> apiMapping = new HashMap<>();

    for (Meta meta : Meta.values()) {
      for (CollectionOperation op : CollectionOperation.values()) {
        if (op.action == meta.action) {
          apiMapping.put(meta, new ApiCommand() {
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

    return apiMapping.values();
  }




  @Override
  protected List<V2EndPoint> getEndPoints() {
    return asList(CollectionApiMapping.EndPoint.values());
  }

  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }

  //the commands at /cluster are mixed ,V2Only and mixed. So, if a command is not found in the V2Only
  // set it should use a fallack
  Api clusterPathAPI;

  @Override
  protected Api getApi(Map<SolrRequest.METHOD, Map<V2EndPoint, List<ApiCommand>>> commandsMapping, V2EndPoint op) {
    Api api = super.getApi(commandsMapping, op);
    if (op.getSpecName().equals(CollectionApiMapping.EndPoint.CLUSTER_CMD.getSpecName())) {

      this.clusterPathAPI = api;
    }
    return api;
  }

  @Override
  protected Collection<Api> getV2OnlyApis() {
    return new ClusterAPI(handler.getCoreContainer()).getAllApis(this);
  }

}
