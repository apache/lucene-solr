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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;


class DeleteSnapshotOp implements CoreAdminHandler.CoreAdminOp {

  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    final SolrParams params = it.req.getParams();
    String commitName = params.required().get(CoreAdminParams.COMMIT_NAME);
    String cname = params.required().get(CoreAdminParams.CORE);

    CoreContainer cc = it.handler.getCoreContainer();
    SolrCore core = cc.getCore(cname);
    if (core == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + cname);
    }

    try {
      core.deleteNamedSnapshot(commitName);
      // Ideally we shouldn't need this. This is added since the RPC logic in
      // OverseerCollectionMessageHandler can not provide the coreName as part of the result.
      it.rsp.add(CoreAdminParams.CORE, core.getName());
      it.rsp.add(CoreAdminParams.COMMIT_NAME, commitName);
    } finally {
      core.close();
    }
  }
}
