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

import java.io.IOException;
import java.util.concurrent.Future;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.store.blob.client.BlobCoreMetadataBuilder;
import org.apache.solr.store.blob.process.CoreUpdateTracker;
import org.apache.solr.store.shared.SharedCoreConcurrencyController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController;
import org.apache.solr.store.shared.metadata.SharedShardMetadataController.SharedShardVersionMetadata;
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
        pushToSharedStore(core);
        return;
      }
      UpdateLog.RecoveryInfo report = future.get();
      if (report.failed) {
        SolrException.log(CoreAdminOperation.log(), "Replay failed");
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Replay failed");
      }

      pushToSharedStore(core);  // we want to do this before setting ACTIVE
      // TODO: why is replica only set to ACTIVE if there were buffered updates?
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


  private void pushToSharedStore(SolrCore core) throws IOException {
    // Push the index to blob storage before we set our state to ACTIVE
    CloudDescriptor cloudDesc = core.getCoreDescriptor().getCloudDescriptor();
    if (cloudDesc.getReplicaType().equals(Replica.Type.SHARED)) {
      CoreContainer cc = core.getCoreContainer();
      CoreUpdateTracker sharedCoreTracker = new CoreUpdateTracker(cc);

      String collectionName = cloudDesc.getCollectionName();
      String shardName = cloudDesc.getShardId();
      String coreName = core.getName();
      SharedShardMetadataController metadataController = cc.getSharedStoreManager().getSharedShardMetadataController();
      // creates the metadata node
      metadataController.ensureMetadataNodeExists(collectionName, shardName);
      SharedShardVersionMetadata shardVersionMetadata = metadataController.readMetadataValue(collectionName, shardName);
      // TODO: We should just be initialized to a default value since this is a new shard.  
      //       As of now we are only taking care of basic happy path. We still need to evaluate what will happen
      //       if a split is abandoned because of failure(e.g. long GC pause) and is re-tried?
      //       How to make sure our re-attempt wins even when the ghost of previous attempt resumes and intervenes?
      if (!SharedShardMetadataController.METADATA_NODE_DEFAULT_VALUE.equals(shardVersionMetadata.getMetadataSuffix())) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "New sub shard has zk information that is not default");
      }

      // sync local cache with zk's default information i.e. equivalent of no-op pull 
      SharedCoreConcurrencyController concurrencyController = cc.getSharedStoreManager().getSharedCoreConcurrencyController();
      String sharedBlobName = Assign.buildSharedShardName(collectionName, shardName);
      concurrencyController.updateCoreVersionMetadata(collectionName, shardName, coreName,
          shardVersionMetadata, BlobCoreMetadataBuilder.buildEmptyCoreMetadata(sharedBlobName));

      sharedCoreTracker.persistShardIndexToSharedStore(
          cc.getZkController().zkStateReader.getClusterState(),
          collectionName,
          shardName,
          coreName);
    }
  }
}
