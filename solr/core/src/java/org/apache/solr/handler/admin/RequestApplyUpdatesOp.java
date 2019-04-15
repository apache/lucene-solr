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

import java.util.concurrent.Future;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.UpdateLog;

class RequestApplyUpdatesOp implements CoreAdminHandler.CoreAdminOp {
  @Override
  public void execute(CoreAdminHandler.CallInfo it) throws Exception {
    SolrParams params = it.req.getParams();
    String cname = params.required().get(CoreAdminParams.NAME);
    CoreAdminOperation.log().info("Applying buffered updates on core: " + cname);
    CoreContainer coreContainer = it.handler.coreContainer;
    try (SolrCore core = coreContainer.getCore(cname)) {
      if (core == null)
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Core [" + cname + "] not found");
      UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
      if (updateLog.getState() != UpdateLog.State.BUFFERING) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Core " + cname + " not in buffering state");
      }
      Future<UpdateLog.RecoveryInfo> future = updateLog.applyBufferedUpdates();
      if (future == null) {
        CoreAdminOperation.log().info("No buffered updates available. core=" + cname);
        it.rsp.add("core", cname);
        it.rsp.add("status", "EMPTY_BUFFER");
        return;
      }
      UpdateLog.RecoveryInfo report = future.get();
      if (report.failed) {
        SolrException.log(CoreAdminOperation.log(), "Replay failed");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Replay failed");
      }
      coreContainer.getZkController().publish(core.getCoreDescriptor(), Replica.State.ACTIVE);
      it.rsp.add("core", cname);
      it.rsp.add("status", "BUFFER_APPLIED");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      CoreAdminOperation.log().warn("Recovery was interrupted", e);
    } catch (Exception e) {
      if (e instanceof SolrException)
        throw (SolrException) e;
      else
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Could not apply buffered updates", e);
    } finally {
      if (it.req != null) it.req.close();
    }
  }
}
