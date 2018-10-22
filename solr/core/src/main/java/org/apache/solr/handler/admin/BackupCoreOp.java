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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.SnapShooter;

import static org.apache.solr.common.params.CommonParams.NAME;


class BackupCoreOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();

    String cname = params.required().get(CoreAdminParams.CORE);
    String name = params.required().get(NAME);

    String repoName = params.get(CoreAdminParams.BACKUP_REPOSITORY);
    BackupRepository repository = it.handler.coreContainer.newBackupRepository(Optional.ofNullable(repoName));

    String location = repository.getBackupLocation(params.get(CoreAdminParams.BACKUP_LOCATION));
    if (location == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "'location' is not specified as a query"
          + " parameter or as a default repository property");
    }

    // An optional parameter to describe the snapshot to be backed-up. If this
    // parameter is not supplied, the latest index commit is backed-up.
    String commitName = params.get(CoreAdminParams.COMMIT_NAME);

    URI locationUri = repository.createURI(location);
    try (SolrCore core = it.handler.coreContainer.getCore(cname)) {
      SnapShooter snapShooter = new SnapShooter(repository, core, locationUri, name, commitName);
      // validateCreateSnapshot will create parent dirs instead of throw; that choice is dubious.
      //  But we want to throw. One reason is that
      //  this dir really should, in fact must, already exist here if triggered via a collection backup on a shared
      //  file system. Otherwise, perhaps the FS location isn't shared -- we want an error.
      if (!snapShooter.getBackupRepository().exists(snapShooter.getLocation())) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "Directory to contain snapshots doesn't exist: " + snapShooter.getLocation() + ". " +
            "Note that Backup/Restore of a SolrCloud collection " +
            "requires a shared file system mounted at the same path on all nodes!");
      }
      snapShooter.validateCreateSnapshot();
      snapShooter.createSnapshot();
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Failed to backup core=" + cname + " because " + e, e);
    }
  }
}
