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

package org.apache.solr.cloud.autoscaling;


import java.io.IOException;

import org.apache.solr.client.solrj.cloud.autoscaling.NoneSuggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;

public class AutoAddReplicasPlanAction extends ComputePlanAction {

  @Override
  protected Suggester getSuggester(Policy.Session session, TriggerEvent event, ActionContext context, SolrCloudManager cloudManager) throws IOException {
    // for backward compatibility
    ClusterStateProvider stateProvider = cloudManager.getClusterStateProvider();
    String autoAddReplicas = stateProvider.getClusterProperty(ZkStateReader.AUTO_ADD_REPLICAS, (String) null);
    if (autoAddReplicas != null && autoAddReplicas.equals("false")) {
      return NoneSuggester.get(session);
    }

    Suggester suggester = super.getSuggester(session, event, context, cloudManager);
    ClusterState clusterState;
    try {
      clusterState = stateProvider.getClusterState();
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception getting cluster state", e);
    }

    boolean anyCollections = false;
    for (DocCollection collection: clusterState.getCollectionsMap().values()) {
      if (collection.getAutoAddReplicas()) {
        anyCollections = true;
        suggester.hint(Suggester.Hint.COLL, collection.getName());
      }
    }

    if (!anyCollections) return NoneSuggester.get(session);
    return suggester;
  }
}
