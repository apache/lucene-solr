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

import org.apache.lucene.index.IndexCommit;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;

class CreateSnapshotOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    String commitName = params.required().get(CoreAdminParams.COMMIT_NAME);
    String cname = params.required().get(CoreAdminParams.CORE);

    CoreContainer cc = it.handler.getCoreContainer();

    try (SolrCore core = cc.getCore(cname)) {
      if (core == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + cname);
      }

      final String indexDirPath = core.getIndexDir();
      final IndexDeletionPolicyWrapper delPol = core.getDeletionPolicy();
      final IndexCommit ic = delPol.getAndSaveLatestCommit();
      try {
        if (null == ic) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                                  "No index commits to snapshot in core " + cname);
        }
        final SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
        mgr.snapshot(commitName, indexDirPath, ic.getGeneration());

        it.rsp.add(CoreAdminParams.CORE, core.getName());
        it.rsp.add(CoreAdminParams.COMMIT_NAME, commitName);
        it.rsp.add(SolrSnapshotManager.INDEX_DIR_PATH, indexDirPath);
        it.rsp.add(SolrSnapshotManager.GENERATION_NUM, ic.getGeneration());
        it.rsp.add(SolrSnapshotManager.FILE_LIST, ic.getFileNames());
      } finally {
        delPol.releaseCommitPoint(ic);
      }
    }
  }
}
