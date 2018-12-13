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
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class DistributedZkUpdateProcessor extends DistributedUpdateProcessor {

  private final CloudDescriptor cloudDesc;

  public DistributedZkUpdateProcessor(SolrQueryRequest req,
                                      SolrQueryResponse rsp, UpdateRequestProcessor next) {
    super(req, rsp, next);
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
  }

  public DistributedZkUpdateProcessor(SolrQueryRequest req,
                                      SolrQueryResponse rsp, AtomicUpdateDocumentMerger docMerger,
                                      UpdateRequestProcessor next) {
    super(req, rsp, docMerger, next);
    cloudDesc = req.getCore().getCoreDescriptor().getCloudDescriptor();
  }

  @Override
  String getCollectionName(CloudDescriptor cloudDesc) {
    return cloudDesc.getCollectionName();
  }

  @Override
  Replica.Type getReplicaType(CloudDescriptor cloudDesc) {
    return cloudDesc.getReplicaType();
  }
}
