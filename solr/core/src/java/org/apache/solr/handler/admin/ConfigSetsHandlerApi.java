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
import org.apache.solr.client.solrj.request.CollectionApiMapping.ConfigSetMeta;
import org.apache.solr.handler.admin.ConfigSetsHandler.ConfigSetOperation;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class ConfigSetsHandlerApi extends BaseHandlerApiSupport {

  final public static String DEFAULT_CONFIGSET_NAME = "_default";

  final ConfigSetsHandler configSetHandler;
  static Collection<ApiCommand> apiCommands = createMapping();

  private static Collection<ApiCommand> createMapping() {
    Map<ConfigSetMeta, ApiCommand> result = new EnumMap<>(ConfigSetMeta.class);

    for (ConfigSetMeta meta : ConfigSetMeta.values())
      for (ConfigSetOperation op : ConfigSetOperation.values()) {
        if (op.action == meta.action) {
          result.put(meta, new ApiCommand() {
            @Override
            public CollectionApiMapping.CommandMeta meta() {
              return meta;
            }

            @Override
            public void invoke(SolrQueryRequest req, SolrQueryResponse rsp, BaseHandlerApiSupport apiHandler) throws Exception {
              ((ConfigSetsHandlerApi) apiHandler).configSetHandler.invokeAction(req, rsp, op.action);
            }
          });
        }
      }

    for (ConfigSetMeta meta : ConfigSetMeta.values()) {
      if(result.get(meta) == null){
        throw new RuntimeException("No implementation for "+ meta.name());
      }
    }

    return result.values();
  }

  public ConfigSetsHandlerApi(ConfigSetsHandler configSetHandler) {
    this.configSetHandler = configSetHandler;
  }


  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }

  @Override
  protected List<CollectionApiMapping.V2EndPoint> getEndPoints() {
    return Arrays.asList(CollectionApiMapping.ConfigSetEndPoint.values());
  }

}
