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
  Replica.Type computeReplicaType() {
    return Replica.Type.NRT;
  }

  @Override
  public void processCommit(CommitUpdateCommand cmd) throws IOException {

    assert TestInjection.injectFailUpdateRequests();

    updateCommand = cmd;

    // replica type can only be nrt in standalone mode
    // NRT replicas will always commit
    super.processCommit(cmd);
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    assert TestInjection.injectFailUpdateRequests();

    setupRequest(cmd);

    super.processAdd(cmd);
  }

  @Override
  protected void doDeleteById(DeleteUpdateCommand cmd) throws IOException {
    setupRequest(cmd);
    super.doDeleteById(cmd);
  }

  @Override
  protected void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    // even in non zk mode, tests simulate updates from a leader
    setupRequest(cmd);
    super.doDeleteByQuery(cmd, null, null);
  }

  @Override
  public void finish() throws IOException {
    super.finish();

    if (next != null) next.finish();
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
