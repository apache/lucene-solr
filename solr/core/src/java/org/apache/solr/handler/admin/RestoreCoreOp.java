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

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.RestoreCore;

import static org.apache.solr.common.params.CommonParams.NAME;


class RestoreCoreOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.CORE);
    String name = params.get(NAME);
    String shardBackupIdStr = params.get(CoreAdminParams.SHARD_BACKUP_ID);
    String repoName = params.get(CoreAdminParams.BACKUP_REPOSITORY);

    if (shardBackupIdStr == null && name == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Either 'name' or 'shardBackupId' must be specified");
    }

    ZkController zkController = it.handler.coreContainer.getZkController();
    if (zkController == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Only valid for SolrCloud");
    }

    try (BackupRepository repository = it.handler.coreContainer.newBackupRepository(Optional.ofNullable(repoName));
         SolrCore core = it.handler.coreContainer.getCore(cname)) {
      String location = repository.getBackupLocation(params.get(CoreAdminParams.BACKUP_LOCATION));
      if (location == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'location' is not specified as a query"
                + " parameter or as a default repository property");
      }

      URI locationUri = repository.createDirectoryURI(location);
      CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
      // this core must be the only replica in its shard otherwise
      // we cannot guarantee consistency between replicas because when we add data (or restore index) to this replica
      Slice slice = zkController.getClusterState().getCollection(cd.getCollectionName()).getSlice(cd.getShardId());
      if (slice.getReplicas().size() != 1 && !core.readOnly) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Failed to restore core=" + core.getName() + ", the core must be the only replica in its shard or it must be read only");
      }

      RestoreCore restoreCore;
      if (shardBackupIdStr != null) {
        final ShardBackupId shardBackupId = ShardBackupId.from(shardBackupIdStr);
        restoreCore = RestoreCore.createWithMetaFile(repository, core, locationUri, shardBackupId);
      } else {
        restoreCore = RestoreCore.create(repository, core, locationUri, name);
      }
      boolean success = restoreCore.doRestore();
      if (!success) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Failed to restore core=" + core.getName());
      }
      // other replicas to-be-created will know that they are out of date by
      // looking at their term : 0 compare to term of this core : 1
      zkController.getShardTerms(cd.getCollectionName(), cd.getShardId()).ensureHighestTermsAreNotZero();
    }
  }
}
