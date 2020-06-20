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
package org.apache.solr.update.processor;

import java.io.IOException;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.*;


/**
 * Executes the update commands using the underlying UpdateHandler.
 * Almost all processor chains should end with an instance of 
 * <code>RunUpdateProcessorFactory</code> unless the user is explicitly 
 * executing the update commands in an alternative custom 
 * <code>UpdateRequestProcessorFactory</code>
 * 
 * @since solr 1.3
 * @see DistributingUpdateProcessorFactory
 */
public class RunUpdateProcessorFactory extends UpdateRequestProcessorFactory {

  public static final String PRE_RUN_CHAIN_NAME = "_preRun_";

  @Override
  public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp, UpdateRequestProcessor next) {
    RunUpdateProcessor runUpdateProcessor = new RunUpdateProcessor(req, next);
    UpdateRequestProcessorChain preRun = req.getCore().getUpdateProcessingChain(PRE_RUN_CHAIN_NAME);
    if (preRun != null) {
      return preRun.createProcessor(req, rsp, false, runUpdateProcessor);
    } else {
      return runUpdateProcessor;
    }
  }


  static class RunUpdateProcessor extends UpdateRequestProcessor {
    private final SolrQueryRequest req;
    private final UpdateHandler updateHandler;

    private boolean changesSinceCommit = false;

    public RunUpdateProcessor(SolrQueryRequest req, UpdateRequestProcessor next) {
      super(next);
      this.req = req;
      this.updateHandler = req.getCore().getUpdateHandler();
    }

    @Override
    public void processAdd(AddUpdateCommand cmd) throws IOException {

      if (AtomicUpdateDocumentMerger.isAtomicUpdate(cmd)) {
        throw new SolrException
                (SolrException.ErrorCode.BAD_REQUEST,
                        "RunUpdateProcessor has received an AddUpdateCommand containing a document that appears to still contain Atomic document update operations, most likely because DistributedUpdateProcessorFactory was explicitly disabled from this updateRequestProcessorChain");
      }

      updateHandler.addDoc(cmd);
      super.processAdd(cmd);
      changesSinceCommit = true;
    }

    @Override
    public void processDelete(DeleteUpdateCommand cmd) throws IOException {
      if (cmd.isDeleteById()) {
        updateHandler.delete(cmd);
      } else {
        updateHandler.deleteByQuery(cmd);
      }
      super.processDelete(cmd);
      changesSinceCommit = true;
    }

    @Override
    public void processMergeIndexes(MergeIndexesCommand cmd) throws IOException {
      updateHandler.mergeIndexes(cmd);
      super.processMergeIndexes(cmd);
    }

    @Override
    public void processCommit(CommitUpdateCommand cmd) throws IOException {
      updateHandler.commit(cmd);
      super.processCommit(cmd);
      if (!cmd.softCommit) {
        // a hard commit means we don't need to flush the transaction log
        changesSinceCommit = false;
      }
    }

    /**
     * @since Solr 1.4
     */
    @Override
    public void processRollback(RollbackUpdateCommand cmd) throws IOException {
      updateHandler.rollback(cmd);
      super.processRollback(cmd);
      changesSinceCommit = false;
    }


    @Override
    public void finish() throws IOException {
      if (changesSinceCommit && updateHandler.getUpdateLog() != null) {
        updateHandler.getUpdateLog().finish(null);
      }
      super.finish();
    }
  }
}


