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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.BackupCollectionBody;
import org.apache.solr.client.solrj.request.beans.CreateAliasBody;
import org.apache.solr.client.solrj.request.beans.CreateBody;
import org.apache.solr.client.solrj.request.beans.DeleteAliasBody;
import org.apache.solr.client.solrj.request.beans.RestoreCollectionBody;
import org.apache.solr.client.solrj.request.beans.SetAliasPropertyBody;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.*;
import static org.apache.solr.common.params.CollectionAdminParams.*;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

/**
 * All V2 APIs for collection management
 *
 */
public class CollectionsAPI {

  private final CollectionsHandler collectionsHandler;

  public  final CollectionsCommands collectionsCommands = new CollectionsCommands();

  public CollectionsAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @EndPoint(
      path = {"/c", "/collections"},
      method = GET,
      permission = COLL_READ_PERM)
  public void getCollections(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    CollectionsHandler.CollectionOperation.LIST_OP.execute(req, rsp, collectionsHandler);
  }

    @EndPoint(
            path = {"/c", "/collections"},
            method = POST,
            permission = COLL_EDIT_PERM)
    public class CollectionsCommands {

        @Command(name = "backup-collection")
        @SuppressWarnings("unchecked")
        public void backupCollection(PayloadObj<BackupCollectionBody> obj) throws Exception {
            final Map<String, Object> v1Params = obj.get().toMap(new HashMap<>());
            v1Params.put(ACTION, CollectionAction.BACKUP.toLower());

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = "restore-collection")
        @SuppressWarnings("unchecked")
        public void restoreBackup(PayloadObj<RestoreCollectionBody> obj) throws Exception {
            final RestoreCollectionBody v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.RESTORE.toLower());
            if (v2Body.createCollectionParams != null && !v2Body.createCollectionParams.isEmpty()) {
                final Map<String, Object> createCollParams = (Map<String, Object>) v1Params.remove("create-collection");
                convertV2CreateCollectionMapToV1ParamMap(createCollParams);
                v1Params.putAll(createCollParams);
            }

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = "create-alias")
        @SuppressWarnings("unchecked")
        public void createAlias(PayloadObj<CreateAliasBody> obj) throws Exception {
            final CreateAliasBody v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.CREATEALIAS.toLower());
            if (! CollectionUtils.isEmpty(v2Body.collections)) {
                final String collectionsStr = String.join(",", v2Body.collections);
                v1Params.remove("collections");
                v1Params.put("collections", collectionsStr);
            }
            if (v2Body.router != null) {
                Map<String, Object> routerProperties = (Map<String, Object>) v1Params.remove("router");
                flattenMapWithPrefix(routerProperties, v1Params, "router.");
            }
            if (v2Body.createCollectionParams != null && !v2Body.createCollectionParams.isEmpty()) {
                final Map<String, Object> createCollectionMap = (Map<String, Object>) v1Params.remove("create-collection");
                convertV2CreateCollectionMapToV1ParamMap(createCollectionMap);
                flattenMapWithPrefix(createCollectionMap, v1Params, "create-collection.");
            }

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name="set-alias-property")
        @SuppressWarnings("unchecked")
        public void setAliasProperty(PayloadObj<SetAliasPropertyBody> obj) throws Exception {
            final SetAliasPropertyBody v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.ALIASPROP.toLower());
            // Flatten "properties" map into individual prefixed params
            final Map<String, Object> propertiesMap = (Map<String, Object>) v1Params.remove("properties");
            flattenMapWithPrefix(propertiesMap, v1Params, "property.");

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name= "delete-alias")
        @SuppressWarnings("unchecked")
        public void deleteAlias(PayloadObj<DeleteAliasBody> obj) throws Exception {
            final DeleteAliasBody v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
            v1Params.put(ACTION, CollectionAction.DELETEALIAS.toLower());

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = "create")
        @SuppressWarnings("unchecked")
        public void create(PayloadObj<CreateBody> obj) throws Exception {
            final CreateBody v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.CREATE.toLower());
            convertV2CreateCollectionMapToV1ParamMap(v1Params);

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        private void convertV2CreateCollectionMapToV1ParamMap(Map<String, Object> v2MapVals) {
            // Keys are copied so that map can be modified as keys are looped through.
            final Set<String> v2Keys = v2MapVals.keySet().stream().collect(Collectors.toSet());
            for (String key : v2Keys) {
                switch (key) {
                    case "properties":
                        final Map<String, Object> propertiesMap = (Map<String, Object>) v2MapVals.remove("properties");
                        flattenMapWithPrefix(propertiesMap, v2MapVals, "property.");
                        break;
                    case "router":
                        final Map<String, Object> routerProperties = (Map<String, Object>) v2MapVals.remove("router");
                        flattenMapWithPrefix(routerProperties, v2MapVals, "router.");
                        break;
                    case "config":
                        v2MapVals.put("collection.configName", v2MapVals.remove("config"));
                        break;
                    case "shuffleNodes":
                        v2MapVals.put("createNodeSet.shuffle", v2MapVals.remove("shuffleNodes"));
                        break;
                    case "nodeSet":
                        final List<String> nodeSetList = (List<String>) v2MapVals.remove("nodeSet");
                        final String nodeSetStr = String.join(",", nodeSetList);
                        v2MapVals.put("createNodeSet", nodeSetStr);
                        break;
                    default:
                        break;
                }
            }
        }

        private void flattenMapWithPrefix(Map<String, Object> toFlatten, Map<String, Object> destination,
                                          String additionalPrefix) {
            if (toFlatten == null || toFlatten.isEmpty() || destination == null) {
                return;
            }

            toFlatten.forEach((k, v) -> destination.put(additionalPrefix + k, v));
        }
  }

  @EndPoint(path = {"/c/{collection}", "/collections/{collection}"},
      method = DELETE,
      permission = COLL_EDIT_PERM)
  public void deleteCollection(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    req = wrapParams(req, "action",
        CollectionAction.DELETE.toString(),
        NAME, req.getPathTemplateValues().get(ZkStateReader.COLLECTION_PROP));
    collectionsHandler.handleRequestBody(req, rsp);
  }

}
