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

import org.apache.solr.api.Command;
import org.apache.solr.api.EndPoint;
import org.apache.solr.api.PayloadObj;
import org.apache.solr.client.solrj.request.beans.DeleteBackupPayload;
import org.apache.solr.client.solrj.request.beans.ListBackupPayload;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.handler.admin.CollectionsHandler;

import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.POST;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

/**
 * V2 API definitions for
 */
@EndPoint(
        path = {"/c/backups", "/collections/backups"},
        method = POST,
        permission = COLL_EDIT_PERM)
public class CollectionBackupsAPI {

  public static final String LIST_BACKUP_CMD = "list-backups";
  public static final String DELETE_BACKUP_CMD = "delete-backup";

  private final CollectionsHandler collectionsHandler;

  public CollectionBackupsAPI(CollectionsHandler collectionsHandler) {
    this.collectionsHandler = collectionsHandler;
  }

  @Command(name = LIST_BACKUP_CMD)
  public void listBackups(PayloadObj<ListBackupPayload> obj) throws Exception {
    final Map<String, Object> v1Params = obj.get().toMap(new HashMap<>());
    v1Params.put(ACTION, CollectionParams.CollectionAction.LISTBACKUP.toLower());

    collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }

  @Command(name = DELETE_BACKUP_CMD)
  public void deleteBackups(PayloadObj<DeleteBackupPayload> obj) throws Exception {
    final Map<String, Object> v1Params = obj.get().toMap(new HashMap<>());
    v1Params.put(ACTION, CollectionParams.CollectionAction.DELETEBACKUP.toLower());

    collectionsHandler.handleRequestBody(wrapParams(obj.getRequest(), v1Params), obj.getResponse());
  }
}
