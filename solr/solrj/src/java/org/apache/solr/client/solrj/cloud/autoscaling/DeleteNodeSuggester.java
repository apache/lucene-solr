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
package org.apache.solr.client.solrj.cloud.autoscaling;

import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.params.CollectionParams;

/**
 * This suggester produces a DELETENODE request using provided {@link org.apache.solr.client.solrj.cloud.autoscaling.Suggester.Hint#SRC_NODE}.
 *
 * @deprecated to be removed in Solr 9.0 (see SOLR-14656)
 */
class DeleteNodeSuggester extends Suggester {

  @Override
  public CollectionParams.CollectionAction getAction() {
    return CollectionParams.CollectionAction.DELETENODE;
  }

  @Override
  @SuppressWarnings({"rawtypes"})
  SolrRequest init() {
    @SuppressWarnings({"unchecked"})
    Set<String> srcNodes = (Set<String>) hints.get(Hint.SRC_NODE);
    if (srcNodes.isEmpty()) {
      throw new RuntimeException("delete-node requires 'src_node' hint");
    }
    if (srcNodes.size() > 1) {
      throw new RuntimeException("delete-node requires exactly one 'src_node' hint");
    }
    return CollectionAdminRequest.deleteNode(srcNodes.iterator().next());
  }
}
