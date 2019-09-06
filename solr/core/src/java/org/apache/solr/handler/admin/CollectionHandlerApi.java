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
  static Collection<ApiCommand> apiCommands = createApiMapping();

  public CollectionHandlerApi(CollectionsHandler handler) {
    this.handler = handler;
  }

  private static Collection<ApiCommand> createApiMapping() {

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
    //The following APIs have only V2 implementations

    for (ClusterAPI.Commands api : ClusterAPI.Commands.values()) {
      apiMapping.put(api.meta(), api );
    }

//    addApi(apiMapping, Meta.GET_NODES, CollectionHandlerApi::getNodes);
//    addApi(apiMapping, Meta.POST_BLOB, CollectionHandlerApi::postBlob);
//    addApi(apiMapping, Meta.SET_CLUSTER_PROPERTY_OBJ, CollectionHandlerApi::setClusterObj);
//    addApi(apiMapping, Meta.ADD_PACKAGE, wrap(CollectionHandlerApi::addUpdatePackage));
//    addApi(apiMapping, Meta.UPDATE_PACKAGE, wrap(CollectionHandlerApi::addUpdatePackage));
//    addApi(apiMapping, Meta.DELETE_RUNTIME_LIB, wrap(CollectionHandlerApi::deletePackage));
//    addApi(apiMapping, Meta.ADD_REQ_HANDLER, wrap(CollectionHandlerApi::addRequestHandler));
//    addApi(apiMapping, Meta.DELETE_REQ_HANDLER, wrap(CollectionHandlerApi::deleteReqHandler));

    for (Meta meta : Meta.values()) {
      if (apiMapping.get(meta) == null) {
        log.error("ERROR_INIT. No corresponding API implementation for : " + meta.commandName);
      }
    }

    return apiMapping.values();
  }



 /* static Command wrap(Command cmd) {
    return info -> {
      CoreContainer cc = ((CollectionHandlerApi) info.apiHandler).handler.coreContainer;
      boolean modified = false;
      try {
        modified = cmd.call(info);
      } catch (SolrException e) {
        log.error("error executing command : " + info.op.jsonStr(), e);
        throw e;
      } catch (Exception e) {
        log.error("error executing command : " + info.op.jsonStr(), e);
        throw new SolrException(ErrorCode.SERVER_ERROR, "error executing command : ", e);
      }
      if (modified) {
        syncClusterProps(info, cc);

      }
      if (info.op != null && info.op.hasError()) {
        log.error("Error in running command {} , current clusterprops.json : {}", Utils.toJSONString(info.op), Utils.toJSONString(new ClusterProperties(cc.getZkController().getZkClient()).getClusterProperties()));
      }
      return modified;

    };
  }*/



  @Override
  protected List<V2EndPoint> getEndPoints() {
    return asList(CollectionApiMapping.EndPoint.values());
  }

  @Override
  protected Collection<ApiCommand> getCommands() {
    return apiCommands;
  }


}
