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

import java.net.URI;
import java.util.Optional;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.RestoreCore;

import static org.apache.solr.common.params.CommonParams.NAME;


class RestoreCoreOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    ZkController zkController = it.handler.coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
    }

    final SolrParams params = it.req.getParams();
    String cname = params.get(CoreAdminParams.CORE);
    if (cname == null) {
      throw new IllegalArgumentException(CoreAdminParams.CORE + " is required");
    }

    String name = params.get(NAME);
    if (name == null) {
      throw new IllegalArgumentException(CoreAdminParams.NAME + " is required");
    }

    String repoName = params.get(CoreAdminParams.BACKUP_REPOSITORY);
    BackupRepository repository = it.handler.coreContainer.newBackupRepository(Optional.ofNullable(repoName));

    String location = repository.getBackupLocation(params.get(CoreAdminParams.BACKUP_LOCATION));
    if (location == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'location' is not specified as a query"
          + " parameter or as a default repository property");
    }

    URI locationUri = repository.createURI(location);
    try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
      RestoreCore restoreCore = new RestoreCore(repository, core, locationUri, name);
      boolean success = restoreCore.doRestore();
      if (!success) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to restore core=" + core.getName());
      }
    }
  }
}
