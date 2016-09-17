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

import java.util.Optional;

import org.apache.lucene.store.Directory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;


class DeleteSnapshotOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    CoreContainer cc = it.handler.getCoreContainer();
    final SolrParams params = it.req.getParams();

    String commitName = params.required().get(CoreAdminParams.COMMIT_NAME);
    String cname = params.required().get(CoreAdminParams.CORE);
    try (SolrCore core = cc.getCore(cname)) {
      if (core == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + cname);
      }

      SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
      Optional<SolrSnapshotMetaDataManager.SnapshotMetaData> metadata = mgr.release(commitName);
      if (metadata.isPresent()) {
        long gen = metadata.get().getGenerationNumber();
        String indexDirPath = metadata.get().getIndexDirPath();

        // If the directory storing the snapshot is not the same as the *current* core
        // index directory, then delete the files corresponding to this snapshot.
        // Otherwise we leave the index files related to snapshot as is (assuming the
        // underlying Solr IndexDeletionPolicy will clean them up appropriately).
        if (!indexDirPath.equals(core.getIndexDir())) {
          Directory d = core.getDirectoryFactory().get(indexDirPath, DirectoryFactory.DirContext.DEFAULT, DirectoryFactory.LOCK_TYPE_NONE);
          try {
            SolrSnapshotManager.deleteIndexFiles(d, mgr.listSnapshotsInIndexDir(indexDirPath), gen);
          } finally {
            core.getDirectoryFactory().release(d);
          }
        }
      }
    }
  }
}
