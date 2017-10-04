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


import org.apache.solr.client.solrj.cloud.autoscaling.NoneSuggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkStateReader;

public class AutoAddReplicasPlanAction extends ComputePlanAction {

  @Override
  protected Policy.Suggester getSuggester(Policy.Session session, TriggerEvent event, ZkStateReader zkStateReader) {
    // for backward compatibility
    String autoAddReplicas = zkStateReader.getClusterProperty(ZkStateReader.AUTO_ADD_REPLICAS, (String) null);
    if (autoAddReplicas != null && autoAddReplicas.equals("false")) {
      return new NoneSuggester();
    }

    Policy.Suggester suggester = super.getSuggester(session, event, zkStateReader);
    ClusterState clusterState = zkStateReader.getClusterState();

    boolean anyCollections = false;
    for (DocCollection collection: clusterState.getCollectionsMap().values()) {
      if (collection.getAutoAddReplicas()) {
        anyCollections = true;
        suggester.hint(Policy.Suggester.Hint.COLL, collection.getName());
      }
    }

    if (!anyCollections) return new NoneSuggester();
    return suggester;
  }
}
