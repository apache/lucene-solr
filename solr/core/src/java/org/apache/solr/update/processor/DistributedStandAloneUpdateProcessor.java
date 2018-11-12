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

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;

import static org.apache.solr.update.processor.DistributingUpdateProcessorFactory.DISTRIB_UPDATE_PARAM;

public class DistributedStandAloneUpdateProcessor extends DistributedUpdateProcessor {

  public DistributedStandAloneUpdateProcessor(
      SolrQueryRequest req,
      SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger, UpdateRequestProcessor next) {
    super(req, rsp, docMerger, next);
  }

  @Override
  boolean isLeader(UpdateCommand cmd) {
    setUpdateCommand(cmd);

    isLeader = getNonZkLeaderAssumption(getReq());

    return isLeader;
  }

  @Override
  protected String parseCollection(CoreContainer cc) {
    return null;
  }

  @Override
  protected Replica.Type parseReplicaType(CoreContainer cc) {
    return Replica.Type.NRT;
  }

  @Override
  public void processAdd(AddUpdateCommand cmd) throws IOException {
    setUpdateCommand(cmd);

    isLeader = getNonZkLeaderAssumption(getReq());

    super.processAdd(cmd);
  }

  @Override
  protected String getLeaderUrl(String id) {
    return getReq().getParams().get(DISTRIB_FROM);
  }

  @Override
  public void processDelete(DeleteUpdateCommand cmd) throws IOException {
    isLeader = isLeader(cmd);

    super.processDelete(cmd);
  }

  @Override
  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    setUpdateCommand(cmd);

    isLeader = getNonZkLeaderAssumption(getReq());

    DistribPhase phase = DistribPhase.parseParam(getReq().getParams().get(DISTRIB_UPDATE_PARAM));
    if(DistribPhase.FROMLEADER == phase) {
      isLeader = false;
    }
    super.doDeleteByQuery(cmd);
  }

  @Override
  public void finish() throws IOException {
    super.finish();

    if (next != null) next.finish();
  }

}
