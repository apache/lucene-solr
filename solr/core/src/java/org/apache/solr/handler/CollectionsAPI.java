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
import org.apache.solr.client.solrj.request.beans.BackupCollectionPayload;
import org.apache.solr.client.solrj.request.beans.CreateAliasPayload;
import org.apache.solr.client.solrj.request.beans.CreatePayload;
import org.apache.solr.client.solrj.request.beans.DeleteAliasPayload;
import org.apache.solr.client.solrj.request.beans.RestoreCollectionPayload;
import org.apache.solr.client.solrj.request.beans.SetAliasPropertyPayload;
import org.apache.solr.client.solrj.request.beans.V2ApiConstants;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.*;
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.ROUTER_KEY;
import static org.apache.solr.cloud.api.collections.RoutedAlias.CREATE_COLLECTION_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.ROUTER_PREFIX;
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

    public static final String V2_CREATE_COLLECTION_CMD = "create";
    public static final String V2_BACKUP_CMD = "backup-collection";
    public static final String V2_RESTORE_CMD = "restore-collection";
    public static final String V2_CREATE_ALIAS_CMD = "create-alias";
    public static final String V2_SET_ALIAS_PROP_CMD = "set-alias-property";
    public static final String V2_DELETE_ALIAS_CMD = "delete-alias";

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

        @Command(name = V2_BACKUP_CMD)
        @SuppressWarnings("unchecked")
        public void backupCollection(PayloadObj<BackupCollectionPayload> obj) throws Exception {
            final Map<String, Object> v1Params = obj.get().toMap(new HashMap<>());
            v1Params.put(ACTION, CollectionAction.BACKUP.toLower());

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = V2_RESTORE_CMD)
        @SuppressWarnings("unchecked")
        public void restoreBackup(PayloadObj<RestoreCollectionPayload> obj) throws Exception {
            final RestoreCollectionPayload v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.RESTORE.toLower());
            if (v2Body.createCollectionParams != null && !v2Body.createCollectionParams.isEmpty()) {
                final Map<String, Object> createCollParams = (Map<String, Object>) v1Params.remove(V2ApiConstants.CREATE_COLLECTION_KEY);
                convertV2CreateCollectionMapToV1ParamMap(createCollParams);
                v1Params.putAll(createCollParams);
            }

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = V2_CREATE_ALIAS_CMD)
        @SuppressWarnings("unchecked")
        public void createAlias(PayloadObj<CreateAliasPayload> obj) throws Exception {
            final CreateAliasPayload v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.CREATEALIAS.toLower());
            if (! CollectionUtils.isEmpty(v2Body.collections)) {
                final String collectionsStr = String.join(",", v2Body.collections);
                v1Params.remove(V2ApiConstants.COLLECTIONS);
                v1Params.put(V2ApiConstants.COLLECTIONS, collectionsStr);
            }
            if (v2Body.router != null) {
                Map<String, Object> routerProperties = (Map<String, Object>) v1Params.remove(V2ApiConstants.ROUTER_KEY);
                flattenMapWithPrefix(routerProperties, v1Params, ROUTER_PREFIX);
            }
            if (v2Body.createCollectionParams != null && !v2Body.createCollectionParams.isEmpty()) {
                final Map<String, Object> createCollectionMap = (Map<String, Object>) v1Params.remove(V2ApiConstants.CREATE_COLLECTION_KEY);
                convertV2CreateCollectionMapToV1ParamMap(createCollectionMap);
                flattenMapWithPrefix(createCollectionMap, v1Params, CREATE_COLLECTION_PREFIX);
            }

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name= V2_SET_ALIAS_PROP_CMD)
        @SuppressWarnings("unchecked")
        public void setAliasProperty(PayloadObj<SetAliasPropertyPayload> obj) throws Exception {
            final SetAliasPropertyPayload v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.ALIASPROP.toLower());
            // Flatten "properties" map into individual prefixed params
            final Map<String, Object> propertiesMap = (Map<String, Object>) v1Params.remove(V2ApiConstants.PROPERTIES_KEY);
            flattenMapWithPrefix(propertiesMap, v1Params, PROPERTY_PREFIX);

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name= V2_DELETE_ALIAS_CMD)
        @SuppressWarnings("unchecked")
        public void deleteAlias(PayloadObj<DeleteAliasPayload> obj) throws Exception {
            final DeleteAliasPayload v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());
            v1Params.put(ACTION, CollectionAction.DELETEALIAS.toLower());

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @Command(name = V2_CREATE_COLLECTION_CMD)
        @SuppressWarnings("unchecked")
        public void create(PayloadObj<CreatePayload> obj) throws Exception {
            final CreatePayload v2Body = obj.get();
            final Map<String, Object> v1Params = v2Body.toMap(new HashMap<>());

            v1Params.put(ACTION, CollectionAction.CREATE.toLower());
            convertV2CreateCollectionMapToV1ParamMap(v1Params);

            collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
        }

        @SuppressWarnings("unchecked")
        private void convertV2CreateCollectionMapToV1ParamMap(Map<String, Object> v2MapVals) {
            // Keys are copied so that map can be modified as keys are looped through.
            final Set<String> v2Keys = v2MapVals.keySet().stream().collect(Collectors.toSet());
            for (String key : v2Keys) {
                switch (key) {
                    case V2ApiConstants.PROPERTIES_KEY:
                        final Map<String, Object> propertiesMap = (Map<String, Object>) v2MapVals.remove(V2ApiConstants.PROPERTIES_KEY);
                        flattenMapWithPrefix(propertiesMap, v2MapVals, PROPERTY_PREFIX);
                        break;
                    case ROUTER_KEY:
                        final Map<String, Object> routerProperties = (Map<String, Object>) v2MapVals.remove(V2ApiConstants.ROUTER_KEY);
                        flattenMapWithPrefix(routerProperties, v2MapVals, CollectionAdminParams.ROUTER_PREFIX);
                        break;
                    case V2ApiConstants.CONFIG:
                        v2MapVals.put(CollectionAdminParams.COLL_CONF, v2MapVals.remove(V2ApiConstants.CONFIG));
                        break;
                    case V2ApiConstants.SHUFFLE_NODES:
                        v2MapVals.put(CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM, v2MapVals.remove(V2ApiConstants.SHUFFLE_NODES));
                        break;
                    case V2ApiConstants.NODE_SET:
                        final Object nodeSetValUncast = v2MapVals.remove(V2ApiConstants.NODE_SET);
                        if (nodeSetValUncast instanceof String) {
                            v2MapVals.put(CollectionAdminParams.CREATE_NODE_SET_PARAM, nodeSetValUncast);
                        } else {
                            final List<String> nodeSetList = (List<String>) nodeSetValUncast;
                            final String nodeSetStr = String.join(",", nodeSetList);
                            v2MapVals.put(CollectionAdminParams.CREATE_NODE_SET_PARAM, nodeSetStr);
                        }
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
    req = wrapParams(req, ACTION,
        CollectionAction.DELETE.toString(),
        NAME, req.getPathTemplateValues().get(ZkStateReader.COLLECTION_PROP));
    collectionsHandler.handleRequestBody(req, rsp);
  }

}
