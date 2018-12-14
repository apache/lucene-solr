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

import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.UpdateCommand;

public class DistributedZkUpdateProcessor extends DistributedUpdateProcessor {

  private final CloudDescriptor cloudDesc;
  private final ZkController zkController;

  public DistributedZkUpdateProcessor(SolrQueryRequest req,
                                      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    zkController = req.getCore().getCoreContainer().getZkController();
  }

  public DistributedZkUpdateProcessor(SolrQueryRequest req,
                                      SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger,
                                      UpdateRequestProcessor next) {
    super(req, rsp, docMerger, next);
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
    zkController = req.getCore().getCoreContainer().getZkController();

  }

  @Override
  String getCollectionName(CloudDescriptor cloudDesc) {
    return cloudDesc.getCollectionName();
  }

  @Override
  Replica.Type getReplicaType(CloudDescriptor cloudDesc) {
    return cloudDesc.getReplicaType();
  }

  @Override
  protected String getLeaderUrl(String id) {
    // try get leader from req params, fallback to zk lookup if not found.
    String distribFrom = req.getParams().get(DISTRIB_FROM);
    if(distribFrom != null) {
      return distribFrom;
    }
    return getLeaderUrlZk(id);
  }

  private String getLeaderUrlZk(String id) {
    // An update we're dependent upon didn't arrive! This is unexpected. Perhaps likely our leader is
    // down or partitioned from us for some reason. Lets force refresh cluster state, and request the
    // leader for the update.
    if (zkController == null) { // we should be in cloud mode, but wtf? could be a unit test
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Can't find document with id=" + id + ", but fetching from leader "
          + "failed since we're not in cloud mode.");
    }
    try {
      return zkController.getZkStateReader().getLeaderRetry(collection, cloudDesc.getShardId()).getCoreUrl();
    } catch (InterruptedException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception during fetching from leader.", e);
    }
  }

  @Override
  boolean isLeader(UpdateCommand cmd) {
    zkCheck();
    if (cmd instanceof AddUpdateCommand) {
      AddUpdateCommand acmd = (AddUpdateCommand)cmd;
      nodes = setupRequest(acmd.getHashableId(), acmd.getSolrInputDocument());
    } else if (cmd instanceof DeleteUpdateCommand) {
      DeleteUpdateCommand dcmd = (DeleteUpdateCommand)cmd;
      nodes = setupRequest(dcmd.getId(), null);
    }
    return isLeader;
  }
}
