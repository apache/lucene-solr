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
import java.util.Map;

import org.apache.solr.client.solrj.request.CollectionApiMapping.CommandMeta;
import org.apache.solr.client.solrj.request.CollectionApiMapping.V2EndPoint;
import org.apache.solr.client.solrj.request.CoreApiMapping;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class CoreAdminHandlerApi extends BaseHandlerApiSupport {
  private final CoreAdminHandler handler;
  static Collection<ApiCommand> apiCommands = createMapping();

  private static Collection<ApiCommand> createMapping() {
    Map<CoreApiMapping.Meta, ApiCommand> result = new EnumMap<>(CoreApiMapping.Meta.class);

    for (CoreApiMapping.Meta meta : CoreApiMapping.Meta.values()) {

      for (CoreAdminOperation op : CoreAdminOperation.values()) {
        if (op.action == meta.action) {
          result.put(meta, new ApiCommand() {
            @Override
            public CommandMeta meta() {
              return meta;
            }

            @Override
            public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
              op.execute(new CoreAdminHandler.CallInfo(((CoreAdminHandlerApi) apiHandler).handler,
                  req,
                  rsp,
                  op));
            }
          });
        }
      }
    }

    for (CoreApiMapping.Meta meta : CoreApiMapping.Meta.values()) {
      if (result.get(meta) == null) {
        throw new RuntimeException("No implementation for " + meta.name());
      }
    }

    return result.values();
  }

  public CoreAdminHandlerApi(CoreAdminHandler handler) {
    this.handler = handler;
  }


  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }

  @Override
  protected Collection<V2EndPoint> getEndPoints() {
    return Arrays.asList(CoreApiMapping.EndPoint.values());
  }


}
