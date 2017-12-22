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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.cloud.autoscaling.AutoScalingConfig;
import org.apache.solr.client.solrj.cloud.autoscaling.NoneSuggester;
import org.apache.solr.client.solrj.cloud.autoscaling.Policy;
import org.apache.solr.client.solrj.cloud.autoscaling.PolicyHelper;
import org.apache.solr.client.solrj.cloud.autoscaling.ReplicaInfo;
import org.apache.solr.client.solrj.cloud.autoscaling.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.autoscaling.Suggester;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.params.AutoScalingParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.params.AutoScalingParams.PREFERRED_OP;

/**
 * This class is responsible for using the configured policy and preferences
 * with the hints provided by the trigger event to compute the required cluster operations.
 *
 * The cluster operations computed here are put into the {@link ActionContext}'s properties
 * with the key name "operations". The value is a List of SolrRequest objects.
 */
public class ComputePlanAction extends TriggerActionBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public void process(TriggerEvent event, ActionContext context) throws Exception {
    log.debug("-- processing event: {} with context properties: {}", event, context.getProperties());
    SolrCloudManager cloudManager = context.getCloudManager();
    try {
      AutoScalingConfig autoScalingConf = cloudManager.getDistribStateManager().getAutoScalingConfig();
      if (autoScalingConf.isEmpty()) {
        throw new Exception("Action: " + getName() + " executed but no policy is configured");
      }
      PolicyHelper.SessionWrapper sessionWrapper = PolicyHelper.getSession(cloudManager);
      Policy.Session session = sessionWrapper.get();
      if (log.isTraceEnabled()) {
        ClusterState state = cloudManager.getClusterStateProvider().getClusterState();
        log.trace("-- session: {}", session);
        log.trace("-- state: {}", state);
      }
      try {
        Suggester suggester = getSuggester(session, event, cloudManager);
        while (true) {
          SolrRequest operation = suggester.getSuggestion();
          if (operation == null) break;
          log.info("Computed Plan: {}", operation.getParams());
          Map<String, Object> props = context.getProperties();
          props.compute("operations", (k, v) -> {
            List<SolrRequest> operations = (List<SolrRequest>) v;
            if (operations == null) operations = new ArrayList<>();
            operations.add(operation);
            return operations;
          });
          session = suggester.getSession();
          suggester = getSuggester(session, event, cloudManager);
        }
      } finally {
        releasePolicySession(sessionWrapper, session);
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Unexpected exception while processing event: " + event, e);    }
  }

  private void releasePolicySession(PolicyHelper.SessionWrapper sessionWrapper, Policy.Session session) {
    sessionWrapper.returnSession(session);
    sessionWrapper.release();

  }

  protected Suggester getSuggester(Policy.Session session, TriggerEvent event, SolrCloudManager cloudManager) {
    Suggester suggester;
    switch (event.getEventType()) {
      case NODEADDED:
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Suggester.Hint.TARGET_NODE, event.getProperty(TriggerEvent.NODE_NAMES));
        break;
      case NODELOST:
        suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA)
            .hint(Suggester.Hint.SRC_NODE, event.getProperty(TriggerEvent.NODE_NAMES));
        break;
      case SEARCHRATE:
        Map<String, Map<String, Double>> hotShards = (Map<String, Map<String, Double>>)event.getProperty(AutoScalingParams.SHARD);
        Map<String, Double> hotCollections = (Map<String, Double>)event.getProperty(AutoScalingParams.COLLECTION);
        List<ReplicaInfo> hotReplicas = (List<ReplicaInfo>)event.getProperty(AutoScalingParams.REPLICA);
        Map<String, Double> hotNodes = (Map<String, Double>)event.getProperty(AutoScalingParams.NODE);

        if (hotShards.isEmpty() && hotCollections.isEmpty() && hotReplicas.isEmpty()) {
          // node -> MOVEREPLICA
          if (hotNodes.isEmpty()) {
            log.warn("Neither hot replicas / collection nor nodes are reported in event: " + event);
            return NoneSuggester.INSTANCE;
          }
          suggester = session.getSuggester(CollectionParams.CollectionAction.MOVEREPLICA);
          for (String node : hotNodes.keySet()) {
            suggester = suggester.hint(Suggester.Hint.SRC_NODE, node);
          }
        } else {
          // collection || shard || replica -> ADDREPLICA
          suggester = session.getSuggester(CollectionParams.CollectionAction.ADDREPLICA);
          Set<Pair> collectionShards = new HashSet<>();
          hotShards.forEach((coll, shards) -> shards.forEach((s, r) -> collectionShards.add(new Pair(coll, s))));
          for (Pair<String, String> colShard : collectionShards) {
            suggester = suggester.hint(Suggester.Hint.COLL_SHARD, colShard);
          }
        }
        break;
      case METRIC:
        Map<String, Number> sourceNodes = (Map<String, Number>) event.getProperty(AutoScalingParams.NODE);
        String collection = (String) event.getProperty(AutoScalingParams.COLLECTION);
        String shard = (String) event.getProperty(AutoScalingParams.SHARD);
        String preferredOp = (String) event.getProperty(PREFERRED_OP);
        if (sourceNodes.isEmpty()) {
          log.warn("No nodes reported in event: " + event);
          return NoneSuggester.INSTANCE;
        }
        CollectionParams.CollectionAction action = CollectionParams.CollectionAction.get(preferredOp == null ? CollectionParams.CollectionAction.MOVEREPLICA.toLower() : preferredOp);
        suggester = session.getSuggester(action);
        for (String node : sourceNodes.keySet()) {
          suggester = suggester.hint(Suggester.Hint.SRC_NODE, node);
        }
        if (collection != null) {
          if (shard == null) {
            suggester = suggester.hint(Suggester.Hint.COLL, collection);
          } else {
            suggester = suggester.hint(Suggester.Hint.COLL_SHARD, new Pair(collection, shard));
          }
        }
        break;
      default:
        throw new UnsupportedOperationException("No support for events other than nodeAdded, nodeLost, searchRate and metric. Received: " + event.getEventType());
    }
    return suggester;
  }
}
