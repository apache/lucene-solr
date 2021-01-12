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

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionApiMapping;
import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.Meta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.handler.admin.CollectionsHandler.CollectionOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class CollectionHandlerApi extends BaseHandlerApiSupport {

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

    return result.values();
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
