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
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.util.TestInjection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedStandaloneUpdateProcessor extends DistributedUpdateProcessor {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DistributedStandaloneUpdateProcessor(SolrQueryRequest req,
                                              SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
  }

  public DistributedStandaloneUpdateProcessor(SolrQueryRequest req,
                                              SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger,
                                              UpdateRequestProcessor next) {
    super(req, rsp, docMerger, next);
  }

  @Override
  String computeCollectionName(CloudDescriptor cloudDesc) {
    return null;
  }

  @Override
  Replica.Type computeReplicaType(CloudDescriptor cloudDesc) {
    return Replica.Type.NRT;
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {

    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;

    CompletionService<Exception> completionService = new ExecutorCompletionService<>(req.getCore().getCoreContainer().getUpdateShardHandler().getUpdateExecutor());
    Set<Future<Exception>> pending = new HashSet<>();
    if (replicaType == Replica.Type.TLOG) {
      if (isLeader) {
        super.processCommit(cmd);
      }
    } else if (replicaType == Replica.Type.PULL) {
      log.warn("Commit not supported on replicas of type " + Replica.Type.PULL);
    } else {
      // NRT replicas will always commit
      super.processCommit(cmd);
    }
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;
    isLeader = getNonZkLeaderAssumption(req);

    super.processAdd(cmd);
  }

  @Override
  protected void doDeleteById(DeleteUpdateCommand cmd) throws IOException {
    isLeader = getNonZkLeaderAssumption(req);
    super.doDeleteById(cmd);
  }

  @Override
  protected void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // even in non zk mode, tests simulate updates from a leader
    isLeader = getNonZkLeaderAssumption(req);
    super.doDeleteByQuery(cmd, null, null);
  }

  @Override
  void setupRequest(UpdateCommand cmd) {
    updateCommand = cmd;
    isLeader = getNonZkLeaderAssumption(req);
  }

  @Override
  protected String getLeaderUrl(String id) {
    return req.getParams().get(DISTRIB_FROM);
  }

}
