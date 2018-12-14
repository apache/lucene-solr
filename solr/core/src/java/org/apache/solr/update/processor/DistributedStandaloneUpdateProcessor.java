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
import java.util.Collections;

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;

public class DistributedStandaloneUpdateProcessor extends DistributedUpdateProcessor {

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
  String getCollectionName(CloudDescriptor cloudDesc) {
    return null;
  }

  @Override
  Replica.Type getReplicaType(CloudDescriptor cloudDesc) {
    return Replica.Type.NRT;
  }

  @Override
  public void doDeleteByQuery(DeleteUpdateCommand cmd) throws IOException {
    isLeader = getNonZkLeaderAssumption(req);
    super.doDeleteByQuery(cmd, Collections.emptyList(), null);
  }

  @Override
  boolean isLeader(UpdateCommand cmd) {
    isLeader = getNonZkLeaderAssumption(req);
    return isLeader;
  }

  @Override
  protected String getLeaderUrl(String id) {
    return req.getParams().get(DISTRIB_FROM);
  }

}
